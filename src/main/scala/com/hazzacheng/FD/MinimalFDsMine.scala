package com.hazzacheng.FD

import com.hazzacheng.FD.utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  *
  * Description:
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-10-06
  * Time: 9:01 AM
  */

class MinimalFDsMine(private var numPartitions: Int,
                     private val sc: SparkContext,
                     private val df: DataFrame,
                     private val colSize: Int,
                     private val filePath: String
                    ) extends Serializable {

  val results = mutable.ListBuffer.empty[(Set[Int], Int)]
  val allSame = mutable.HashSet.empty[Int]
  val lessAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
  val lessBiggerAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
  val moreAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
  val moreSmallerAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
  val levelMap = mutable.Map.empty[Int, mutable.HashMap[Int, mutable.HashMap[Set[Int], Int]]]
  val wholeCuttedMap = mutable.Map.empty[Int, mutable.HashMap[Int, mutable.HashSet[(Set[Int], Int)]]]
  val candidates = mutable.HashMap.empty[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]]
  val topCandidates = mutable.Set.empty[(Set[Int], Int)]
  val topFDs = mutable.Set.empty[(Set[Int], Int)]
  val equalAttrMap = mutable.Map.empty[Int, List[Int]]
  val ordersMap = mutable.Map.empty[Int, Int]
  var orders = Array[(Int, Int)]()
  val rhsCount = mutable.Map.empty[Int, Int]
  val rdds = mutable.Map.empty[Int, RDD[(Int, List[Array[Int]])]]
  val smallRdds = mutable.Map.empty[Int, RDD[(Int, List[Array[Int]])]]
  val rddsCountMap = mutable.Map.empty[Int, Int]
  val smallRddsCountMap = mutable.Map.empty[Int, Int]
  val THRESHOLD = 10
  val COLMAX = 1
  var wholeRDD: RDD[Array[Int]] = _

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run(): Map[Set[Int], List[Int]] = {

    var time = System.currentTimeMillis()

   /* var singleFDs: Array[(Int, Int)] = null
    var singleColCount: Map[Int, Int] = null
    var twoAttrsCount: List[((Int, Int), Int)] = null
    var del: List[Int] = null

    if (colSize > COLMAX) {
      val (t1, t2) = DataFrameUtils.getDownFds(df, colSize, allSame)
      singleFDs = t1
      singleColCount = t2
    } else {
      val (t1, t2, t3) = DataFrameUtils.getBottomFDs(df, colSize, allSame)
      singleFDs = t1
      singleColCount = t2
      twoAttrsCount = t3
    }
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    if (colSize > COLMAX) {
      del = createNewOrders(equalAttr, singleColCount)
    } else {
      del = createNewOrders(equalAttr, singleColCount, twoAttrsCount)
    }*/

    val (singleFDs, singleColCount, twoAttrsCount) = DataFrameUtils.getBottomFDs(df, colSize, allSame)
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    val del = createNewOrders(equalAttr, singleColCount, twoAttrsCount)

    /*val (singleFDs, singleColCount) = DataFrameUtils.getDownFds(df, colSize, allSame)
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    val del = createNewOrders(equalAttr, singleColCount)*/

    println("===== Get Down FDs: " + (System.currentTimeMillis() - time) + "ms")

    val newColSize = orders.length

    println("====== Now Attrs Count: " + newColSize + " Cut: " + (colSize - newColSize))

    // create the new single lhs fds
    val bottomFDs = getNewBottomFDs(withoutEqualAttr)
    results ++= bottomFDs
    // get new df
    val newDF = DataFrameUtils.getNewDF(df, numPartitions, del.toSet).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val newSize = newDF.count()
    df.unpersist()
    wholeRDD = DataFrameUtils.dfToRdd(newDF)
    println("====== New DF Count: " + newSize)

    // check the fds with the longest lhs
    topCandidates ++= getLongestLhs(newColSize)
    CandidatesUtils.cutInTopLevels(topCandidates, bottomFDs)
    time = System.currentTimeMillis()
    val (rightTopFDs, wrongTopFDs) = DataFrameUtils.getTopFDs(moreAttrsCountMap, newDF, topCandidates, rhsCount)
//    val (rightTopFDs, wrongTopFDs) = RddUtils.getTopFDs(newDF, wholeRDD, rdds, topCandidates, orders.head._1, rhsCount)
    topFDs ++= rightTopFDs
    println("===== Get Top FDs: " + (System.currentTimeMillis() - time) + "ms")
    // get all candidates FD without bottom level and top level
    candidates ++= CandidatesUtils.removeTopAndBottom(CandidatesUtils.getCandidatesParallel(sc, newColSize), newColSize)
    // cut from bottom level and top level
    CandidatesUtils.cutFromDownToTop(candidates, bottomFDs)
    CandidatesUtils.cutFromTopToDown(candidates, wrongTopFDs)

    findByDFandRDD(newDF, newColSize)

    // check the top levels
    if (topFDs.nonEmpty) {
      CandidatesUtils.findMinFD(topFDs)
      results ++= topFDs
    }

    // recover all fds
    val fds = recoverAllFDs()

    fds
  }

/*  private def findByDF(newDF: DataFrame,
                       newColSize: Int
                       //orders:  Array[(Int, Int)]
                      ): Unit = {
    val middle = (newColSize + 1) / 2
    for (level <- 2 to middle) {
      // the higher level
      val symmetrical = newColSize - level
      if (level < symmetrical) {
        val time0 = System.currentTimeMillis()
        val toCheckedHigh = CandidatesUtils.getLevelCandidates(candidates, symmetrical).toList
        println("====df toCheckedHigh: " + "level- " + symmetrical + " " + " lhs: " + toCheckedHigh.toList.length + " whole: " + toCheckedHigh.toList.flatMap(x => x._2.toList).size)
        if (toCheckedHigh.nonEmpty) {
          val failFDs = DataFrameUtils.getFailFDs(newDF, toCheckedHigh, moreAttrsCountMap, moreSmallerAttrsCountMap, topFDs, rhsCount)
          println("====failFDs: " + "level- " + symmetrical + " " + failFDs.toList.toString() )
          CandidatesUtils.cutFromTopToDown(candidates, failFDs)
        }
        println("====df time: " + "level-" + level + " " + (System.currentTimeMillis() - time0))
      }

      //the lower level
      val toCheckedLow = CandidatesUtils.getLevelCandidates(candidates, level).toList
      println("====df toCheckedLow: " + "level- " + level + " " + " lhs: " + toCheckedLow.toList.length + " whole: " + toCheckedLow.toList.flatMap(x => x._2.toList).size)
      if (toCheckedLow.nonEmpty) {
        val time0 = System.currentTimeMillis()
        val minimalFds = DataFrameUtils.getMinimalFDs(newDF, toCheckedLow, lessAttrsCountMap, lessBiggerAttrsCountMap, rhsCount)
        println("====minimalFDs: " + "level-" + level + " " + minimalFds.toList.toString())
        results ++= minimalFds
        CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
        CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
        println("====df time: " + "level-" + level + " " + (System.currentTimeMillis() - time0))
      }
    }

  }*/

  def findBySplit(newDF: DataFrame,
                  newColSize: Int
                 ): Unit = {

    // check the part1
    val part1 = newColSize / 3 - 1
    val cols1 = Range(1, part1 + 1).toSet

    val df1 = DataFrameUtils.getSelectedDF(newDF, numPartitions, cols1).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("===== DF1 Count: " + df1.count())
    findInPart(df1, part1, cols1, 0)

    df1.unpersist()
    smallRdds.foreach(_._2.unpersist())
    smallRdds.clear()
    rddsCountMap.clear()
    smallRddsCountMap.clear()
    levelMap.clear()
    wholeCuttedMap.clear()

    // check the part2
    val part2 = newColSize - part1
    val cols2 = Range(1, part2 + 1).toSet

    val df2 = DataFrameUtils.getSelectedDF(newDF, numPartitions, cols2).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("===== DF2 Count: " + df2.count())
    findInPart(df2, part2, cols2, part2)

    df2.unpersist()
    smallRdds.foreach(_._2.unpersist())
    smallRdds.clear()
    rddsCountMap.clear()
    smallRddsCountMap.clear()
    levelMap.clear()
    wholeCuttedMap.clear()

    // check the whole parts
    findByDFandRDD(newDF, newColSize)
    newDF.unpersist()
    rdds.foreach(_._2.unpersist())
    rdds.clear()
  }

  def findByDFandRDD(df: DataFrame, cols: Int): Unit = {

    val middle = (cols + 1) / 2
    for (low <- 2 to middle) {

      for (col <- 1 to cols) {

        // the higher level
        val high = cols - low
        if (low < high) {
          val t = System.currentTimeMillis()

          val toCheckedHigh = CandidatesUtils.getTargetCandidates(candidates, col, high).toList
          val size = CandidatesUtils.getToCheckedSize(toCheckedHigh)
          if (size > 0 && size <= THRESHOLD) {
            val failFDs = DataFrameUtils.getFailFDs(df, toCheckedHigh, moreAttrsCountMap, moreSmallerAttrsCountMap, topFDs, rhsCount)
            CandidatesUtils.cutFromTopToDown(candidates, failFDs)
          }
          if (size > THRESHOLD) {
            val rdd = rdds.getOrElseUpdate(col, RddUtils.repart(wholeRDD, col).persist(StorageLevel.MEMORY_AND_DISK))
            val map = levelMap.getOrElseUpdate(col, mutable.HashMap.empty[Int, mutable.HashMap[Set[Int], Int]])
            val failFDs = RddUtils.getFailFDs(sc, rdd, toCheckedHigh, cols, topFDs, map)
            levelMap.update(col, map)
            CandidatesUtils.cutFromTopToDown(candidates, failFDs)
          }
          if (size > 0)
            println("====== High Level: " + high + " Col: " + col + " Size: " + size + " Time: " + (System.currentTimeMillis() - t))
        }

        // the lower level
        val t = System.currentTimeMillis()
        val toCheckedLow = CandidatesUtils.getTargetCandidates(candidates, col, low).toList
        val size = CandidatesUtils.getToCheckedSize(toCheckedLow)
        if (size > 0 && size <= THRESHOLD) {
          val minimalFds = DataFrameUtils.getMinimalFDs(df, toCheckedLow, lessAttrsCountMap, lessBiggerAttrsCountMap, rhsCount)
          results ++= minimalFds
          CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
          CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
        }
        if (size > THRESHOLD) {
          val rdd = rdds.getOrElseUpdate(col, RddUtils.repart(wholeRDD, col).persist(StorageLevel.MEMORY_AND_DISK))
          val partitionSize = rddsCountMap.getOrElseUpdate(col, rdd.count().toInt)
          val map = wholeCuttedMap.getOrElseUpdate(col, mutable.HashMap.empty[Int, mutable.HashSet[(Set[Int], Int)]])
          val (minimalFDs, failFDs, partWithFailFDs) =
            RddUtils.getMinimalFDs(sc, rdd, toCheckedLow, partitionSize, cols, map)
          results ++= minimalFDs
          CandidatesUtils.cutFromDownToTop(candidates, minimalFDs)
          CandidatesUtils.cutInTopLevels(topFDs, minimalFDs)
/*          if (failFDs.nonEmpty) {
            val cuttedFDsMap = CandidatesUtils.getCuttedFDsMap(candidates, failFDs)
            val map = wholeCuttedMap(col)
            RddUtils.updateLevelMap(cuttedFDsMap, partWithFailFDs, map, low)
            wholeCuttedMap.update(col, map)
          }*/
        }
        if (size > 0)
          println("====== Low Level: " + low + " Col: " + col + " Size: " + size + " Time: " + (System.currentTimeMillis() - t))

      }
    }

    wholeRDD.unpersist()
  }

  def findInPart(df: DataFrame, cols: Int, colsSet: Set[Int], offset: Int): Unit = {

    val RDD = DataFrameUtils.dfToRdd(df).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val middle = (cols + 1) / 2
    for (low <- 2 to middle) {

      for (col <- 1 to cols) {

        // the higher level
        val high = cols - low
        if (low < high) {
          val t = System.currentTimeMillis()

          val toCheckedHigh = CandidatesUtils.getTargetCandidates(candidates, col, high, colsSet).toList
          val size = CandidatesUtils.getToCheckedSize(toCheckedHigh)
          if (size > 0 && size <= THRESHOLD) {
            val failFDs = DataFrameUtils.getFailFDs(df, toCheckedHigh, moreAttrsCountMap, moreSmallerAttrsCountMap, topFDs, rhsCount)
            CandidatesUtils.cutFromTopToDown(candidates, failFDs)
          }
          if (size > THRESHOLD) {
            val rdd = smallRdds.getOrElseUpdate(col, RddUtils.repart(RDD, col).persist(StorageLevel.MEMORY_AND_DISK))
            val map = levelMap.getOrElseUpdate(col, mutable.HashMap.empty[Int, mutable.HashMap[Set[Int], Int]])
            val failFDs = RddUtils.getFailFDs(sc, rdd, toCheckedHigh, cols, topFDs, map)
            levelMap.update(col, map)
            CandidatesUtils.cutFromTopToDown(candidates, failFDs)
          }
          if (size > 0)
            println("====== High Level: " + high + " Col: " + col + " Offset: " + offset + " Size: " + size + " Time: " + (System.currentTimeMillis() - t))
        }

        // the lower level
        val t = System.currentTimeMillis()
        val toCheckedLow = CandidatesUtils.getTargetCandidates(candidates, col, high, colsSet).toList
        val size = CandidatesUtils.getToCheckedSize(toCheckedLow)
        if (size > 0 && size <= THRESHOLD) {
          val minimalFds = DataFrameUtils.getMinimalFDs(df, toCheckedLow, lessAttrsCountMap, lessBiggerAttrsCountMap, rhsCount)
          results ++= minimalFds
          CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
          CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
        }
        if (size > THRESHOLD) {
          val rdd = smallRdds.getOrElseUpdate(col, RddUtils.repart(RDD, col).persist(StorageLevel.MEMORY_AND_DISK))
          val partitionSize = smallRddsCountMap.getOrElseUpdate(col, rdd.count().toInt)
          val map = wholeCuttedMap.getOrElseUpdate(col, mutable.HashMap.empty[Int, mutable.HashSet[(Set[Int], Int)]])
          val (minimalFDs, failFDs, partWithFailFDs) =
            RddUtils.getMinimalFDs(sc, rdd, toCheckedLow, partitionSize, cols, map)
          results ++= minimalFDs
          CandidatesUtils.cutFromDownToTop(candidates, minimalFDs)
          CandidatesUtils.cutInTopLevels(topFDs, minimalFDs)
          if (failFDs.nonEmpty) {
            val cuttedFDsMap = CandidatesUtils.getCuttedFDsMap(candidates, failFDs)
            val map = wholeCuttedMap(col)
            RddUtils.updateLevelMap(cuttedFDsMap, partWithFailFDs, map, low)
            wholeCuttedMap.update(col, map)
          }
        }
        if (size > 0)
          println("====== Low Level: " + low + " Col: " + col + " Offset: " + offset + " Size: " + size + " Time: " + (System.currentTimeMillis() - t))

      }
    }

    RDD.unpersist()
  }

  def getEqualAttr(fds: Array[(Int, Int)]): (List[Set[Int]], Array[(Int, Int)]) = {
    val len = fds.length
    val tmp = mutable.HashSet.empty[(Int, Int)]

    for (i <- 0 until (len - 1))
      for (j <- i + 1 until len)
        if (fds(i) == fds(j).swap)
          tmp.add(fds(i))
    val newFds = fds.filter(x => !tmp.contains(x) && !tmp.contains(x.swap))

    val res = mutable.ListBuffer.empty[Set[Int]]
    val sets = tmp.map(fd => Set[Int](fd._1, fd._2))
    var setsArr = sets.toArray
    while (setsArr.nonEmpty) {
      var set = setsArr.last
      sets.remove(set)
      setsArr.init.foreach { x =>
        if ((set & x).nonEmpty) {
          set = set | x
          sets.remove(x)
        }
      }
      res.append(set)
      setsArr = sets.toArray
    }


    (res.toList, newFds)
  }

  def createNewOrders(equalAttr: List[Set[Int]],
                      singleLhsCountMap: Map[Int, Int],
                      twoAttrsCount: List[((Int, Int), Int)]
                     ): List[Int] = {


    val tmp = mutable.Set.empty[Int]
    val del = mutable.ListBuffer.empty[Int]
    Range(1, colSize + 1).foreach(tmp.add)
    tmp --= allSame
    del ++= allSame

    equalAttr.foreach { x =>
      val maxAttr = x.maxBy(y => singleLhsCountMap(y))
      val smallAttr = x.filter(_ != maxAttr).toList
      del ++= smallAttr
      tmp --= smallAttr
      equalAttrMap.put(maxAttr, smallAttr)
    }

    var count = 1
    for (i <- tmp.toList.sorted) {
      ordersMap.put(count, i)
      count += 1
    }

    orders = ordersMap.toArray.map {
      x => (x._1, singleLhsCountMap(x._2))
    }.sortWith((x, y) => x._2 > y._2)
    val delSet = del.toSet
    val swappedOrdersMap = ordersMap.map(x => (x._2, x._1))
    rhsCount ++= singleLhsCountMap.filter(x => tmp.contains(x._1))
      .map(x => (swappedOrdersMap(x._1), x._2))
    twoAttrsCount.map(x => (Set[Int](x._1._1, x._1._2), x._2))
      .filter(x => (x._1 & delSet).isEmpty)
      .foreach(x => lessAttrsCountMap.put(x._1.map(swappedOrdersMap(_)), x._2))

    del.toList.sorted
  }

  def createNewOrders(equalAttr: List[Set[Int]],
                      singleLhsCountMap: Map[Int, Int]
                     ): List[Int] = {


    val tmp = mutable.Set.empty[Int]
    val del = mutable.ListBuffer.empty[Int]
    Range(1, colSize + 1).foreach(tmp.add)
    tmp --= allSame
    del ++= allSame

    equalAttr.foreach { x =>
      val maxAttr = x.maxBy(y => singleLhsCountMap(y))
      val smallAttr = x.filter(_ != maxAttr).toList
      del ++= smallAttr
      tmp --= smallAttr
      equalAttrMap.put(maxAttr, smallAttr)
    }

    var count = 1
    for (i <- tmp.toList.sorted) {
      ordersMap.put(count, i)
      count += 1
    }

    orders = ordersMap.toArray.map {
      x => (x._1, singleLhsCountMap(x._2))
    }.sortWith((x, y) => x._2 > y._2)
    val delSet = del.toSet
    val swappedOrdersMap = ordersMap.map(x => (x._2, x._1))
    rhsCount ++= singleLhsCountMap.filter(x => tmp.contains(x._1))
      .map(x => (swappedOrdersMap(x._1), x._2))
    /*twoAttrsCount.map(x => (Set[Int](x._1._1, x._1._2), x._2))
      .filter(x => (x._1 & delSet).isEmpty)
      .foreach(x => lessAttrsCountMap.put(x._1.map(swappedOrdersMap(_)), x._2))*/

    del.toList.sorted
  }

  def getNewBottomFDs(singleFDs: Array[(Int, Int)]): Array[(Set[Int], Int)] = {
    val equalAttrs = equalAttrMap.values.flatMap(_.toSet).toSet
    val swappedMap = ordersMap.map(x => (x._2, x._1))
    val fds = singleFDs.filter(x => !equalAttrs.contains(x._1) && !equalAttrs.contains(x._2))
      .map(x => (Set[Int](swappedMap(x._1)), swappedMap(x._2)))

    fds
  }

  def getLongestLhs(colSize: Int): mutable.Set[(Set[Int], Int)] = {
    val res = mutable.Set.empty[(Set[Int], Int)]
    val range = Range(1, colSize + 1)

    for (i <- 1 to colSize)
      res.add(range.filter(_ != i).toSet, i)

    res
  }



  def recoverAllFDs(): Map[Set[Int], List[Int]] = {
    val fds = mutable.ListBuffer.empty[(Set[Int], Int)]

    val tmp = results.map { x =>
      val lhs = x._1.map(ordersMap(_))
      val rhs = ordersMap(x._2)
      (lhs, rhs)
    }

    val equalAttrs = equalAttrMap.keySet

    for (fd <- tmp) {
      val list = mutable.ListBuffer.empty[mutable.ListBuffer[Int]]
      list.append(mutable.ListBuffer.empty[Int])
      for (attr <- fd._1) {
        if (equalAttrs contains attr) {
          val temp = list.toList.map(_.clone())
          list.foreach(_.append(attr))
          for (ll <- temp) {
            val add = equalAttrMap(attr).map { x =>
              val clone = ll.clone()
              clone.append(x)
              clone
            }
            list ++= add
          }
        } else list.foreach(_.append(attr))
      }
      fds ++= list.map(x => (x.toSet, fd._2))
    }

    for (fd <- fds.toList)
      if (equalAttrs contains fd._2)
        equalAttrMap(fd._2).foreach(x => fds.append((fd._1, x)))

    val equalClass = equalAttrMap.toList.map(x => x._1 :: x._2)
    equalClass.foreach { ec =>
      ec.foreach { x =>
        ec.foreach { y =>
          if (x != y) fds.append((Set[Int](x), y))
        }
      }
    }

    allSame.toArray.foreach { x =>
      Range(1, colSize + 1).foreach{ y =>
        if (x == y) fds.append((Set.empty[Int], x))
        else fds.append((Set[Int](y), x))
      }
    }

    fds.toList.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
  }

}
