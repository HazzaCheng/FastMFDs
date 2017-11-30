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

object MinimalFDsMine {
  private val parallelScaleFactor = 4
  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def findOnSpark(sc: SparkContext,
                  df: DataFrame,
                  colSize: Int,
                  filePath: String): Map[Set[Int], List[Int]] = {
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val results = mutable.ListBuffer.empty[(Set[Int], Int)]
    val lessAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
    val moreAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
    time1 = System.currentTimeMillis()
    // get fds with single lhs
    val (singleFDs, singleLhsCount, twoAttrsCount) = DataFrameUtils.getBottomFDs(df, colSize)
    //println("====singleFDs: " + singleFDs.toList.toString())
    //println("====singleLhsCount: " + singleLhsCount.toString())
    //println("====twoAttrsCount: " + twoAttrsCount.toString())
    // get equal attributes
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    //println("====equalAttr: " + equalAttr.toString())
    // get new orders
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(lessAttrsCountMap, equalAttr, singleLhsCount, colSize, twoAttrsCount)
    //println("====equalAttrMap: " + equalAttrMap.toList.toString())
    //println("====ordersMap: " + ordersMap.toList.toString())
    //println("====orders: " + orders.toList.toString())
    //println("====del: " + del.toString())
    val newColSize = orders.length
    // create the new single lhs fds
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)
    //println("====bottomFDs: " + bottomFDs.toList.toString())
    results ++= bottomFDs
    // get new df
    val newDF = DataFrameUtils.getNewDF(filePath, df, del.toSet).persist(StorageLevel.MEMORY_AND_DISK_SER)
    df.unpersist()
    // check the fds with the longest lhs
    val topCandidates = getLongestLhs(newColSize)
    CandidatesUtils.cutInTopLevels(topCandidates, bottomFDs)
    val (topFDs, wrongTopFDs) = DataFrameUtils.getTopFDs(moreAttrsCountMap, newDF, topCandidates)
    //println("====true topFDs: " + topFDs.toList.toString())
    // get all candidates FD without bottom level and top level
    val candidates = CandidatesUtils.removeTopAndBottom(CandidatesUtils.getCandidates(newColSize), newColSize)
    // cut from bottom level and top level
    CandidatesUtils.cutFromDownToTop(candidates, bottomFDs)
    CandidatesUtils.cutFromTopToDown(candidates, wrongTopFDs)

    // find the minimal fds in the middle levels
    if (newColSize <= 10)
      findByDf(newDF, newColSize, candidates, lessAttrsCountMap, moreAttrsCountMap, topFDs, results, orders)
    else
      findByDfAndRdd(sc, newDF, filePath, del, newColSize, orders,
        candidates, lessAttrsCountMap, moreAttrsCountMap, topFDs, results)

    // check empty lhs
    val emptyFD = mutable.ListBuffer.empty[Int]
    emptyFD ++= orders.filter(_._2.toInt == 1).map(_._1)
    if (emptyFD.nonEmpty)
      emptyFD.toList.foreach(rhs => results.append((Set.empty[Int], rhs)))

    // check the top levels
    if (topFDs.nonEmpty) {
      CandidatesUtils.findMinFD(topFDs)
      results ++= topFDs
    }

    // recover all fds
    val fds = recoverAllFDs(results.toList, equalAttrMap, ordersMap)
    fds
  }

  private def findByDf(newDF: DataFrame,
                       newColSize: Int,
                       candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                       lessAttrsCountMap: mutable.HashMap[Set[Int], Int],
                       moreAttrsCountMap: mutable.HashMap[Set[Int], Int],
                       topFDs: mutable.Set[(Set[Int], Int)],
                       results: mutable.ListBuffer[(Set[Int], Int)],
                       orders:  Array[(Int, Int)]
                      ): Unit = {
    val middle = (newColSize + 1) / 2
    for (level <- 2 to middle) {
      // the higher level
      val symmetrical = newColSize - level
      if (level != symmetrical) {
        val time0 = System.currentTimeMillis()
        val toCheckedHigh = CandidatesUtils.getLevelCandidates(candidates, symmetrical)
        println("====df toCheckedHigh: " + "level- " + symmetrical + " " + " lhs: " + toCheckedHigh.toList.length + " whole: " + toCheckedHigh.toList.flatMap(x => x._2.toList).size)
        if (toCheckedHigh.nonEmpty) {
          val failFDs = DataFrameUtils.getFailFDs(newDF, toCheckedHigh, moreAttrsCountMap, topFDs)
          println("====failFDs: " + "level- " + symmetrical + " " + failFDs.toList.toString() )
          CandidatesUtils.cutFromTopToDown(candidates, failFDs)
        }
        println("====df time: " + "level-" + level + " " + (System.currentTimeMillis() - time0))
      }

      //the lower level
      val toCheckedLow = CandidatesUtils.getLevelCandidates(candidates, level)
      println("====df toCheckedLow: " + "level- " + level + " " + " lhs: " + toCheckedLow.toList.length + " whole: " + toCheckedLow.toList.flatMap(x => x._2.toList).size)
      if (toCheckedLow.nonEmpty) {
        val time0 = System.currentTimeMillis()
        val minimalFds = DataFrameUtils.getMinimalFDs(newDF, toCheckedLow, lessAttrsCountMap)
        println("====minimalFDs: " + "level-" + level + " " + minimalFds.toList.toString())
        results ++= minimalFds
        CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
        CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
        println("====df time: " + "level-" + level + " " + (System.currentTimeMillis() - time0))
      }
    }

  }

  private def findByDfAndRdd(sc: SparkContext,
                             newDF: DataFrame,
                             filePath: String,
                             del: List[Int],
                             newColSize: Int,
                             orders: Array[(Int, Int)],
                             candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                             lessAttrsCountMap: mutable.HashMap[Set[Int], Int],
                             moreAttrsCountMap: mutable.HashMap[Set[Int], Int],
                             topFDs: mutable.Set[(Set[Int], Int)],
                             results: mutable.ListBuffer[(Set[Int], Int)]
                            ): Unit = {
    val commmonAttr = orders.head._1
    val rdd = RddUtils.readAsRdd(sc, filePath, del).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val partitionRDD = repart(sc, rdd, commmonAttr).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val partitionSize = orders.head._2
    // create the map which save cutted leaves from bottom to top
    val wholeCuttedMap = mutable.HashMap
      .empty[String, mutable.HashSet[(Set[Int], Int)]]

    val middle = (newColSize + 1) / 2
    for (level <- 2 to middle) {
      // the higher level
      val symmetrical = newColSize - level
      if (level != symmetrical) {
        val time0 = System.currentTimeMillis()
        val toCheckedHigh = CandidatesUtils.getLevelCandidates(candidates, symmetrical)
        println("====toCheckedHigh: " + "level- " + symmetrical + " " + " lhs: " + toCheckedHigh.toList.length + " whole: " + toCheckedHigh.toList.flatMap(x => x._2.toList).size)
        if (toCheckedHigh.nonEmpty) {
          val failFDs = DataFrameUtils.getFailFDs(newDF, toCheckedHigh, moreAttrsCountMap, topFDs)
          CandidatesUtils.cutFromTopToDown(candidates, failFDs)
        }
        println("====toCheckedHigh time: " + "level- " + symmetrical + " " + (System.currentTimeMillis() - time0))
      }

      // the lower level
      val time_ = System.currentTimeMillis()
      val toCheckedCommon = CandidatesUtils.getTargetCandidates(candidates, commmonAttr, level).toList
      println("====toCheckedLow: " + "level- " + level + " " + " common: " + commmonAttr + " lhs: " + toCheckedCommon.length + " whole: " + toCheckedCommon.flatMap(x => x._2.toList).size)
      val toCheckedLow = CandidatesUtils.getLevelCandidates(candidates, level)
      println("====toCheckedLow: " + "level- " + level + " " + " lhs: " + toCheckedLow.toList.length + " whole: " + toCheckedLow.toList.flatMap(x => x._2.toList).size)
      if (toCheckedCommon.nonEmpty) {
        val (minimalFDs, failFDs, partWithFailFDs) =
          RddUtils.getMinimalFDs(sc, partitionRDD, toCheckedCommon, results, partitionSize, newColSize, wholeCuttedMap)
        CandidatesUtils.cutFromDownToTop(candidates, minimalFDs)
        CandidatesUtils.cutInTopLevels(topFDs, minimalFDs)
        if (failFDs.nonEmpty) {
          val cuttedFDsMap = CandidatesUtils.getCuttedFDsMap(candidates, failFDs)
          RddUtils.updateLevelMap(cuttedFDsMap, partWithFailFDs, wholeCuttedMap, level)
        }
      }
      if (toCheckedLow.nonEmpty) {
        val minimalFds = DataFrameUtils.getMinimalFDs(newDF, toCheckedLow, lessAttrsCountMap)
        results ++= minimalFds
        CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
        CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
      }
      println("====toCheckedLow time: " + "level- " + level + " " + (System.currentTimeMillis() - time_))
    }

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
          set = (set | x)
          sets.remove(x)
        }
      }
      res.append(set)
      setsArr = sets.toArray
    }


    (res.toList, newFds)
  }


  def createNewOrders(attrsCountMap: mutable.HashMap[Set[Int], Int],
                      equalAttr: List[Set[Int]],
                      singleLhsCount: List[(Int, Int)],
                      colSize: Int,
                      twoAttrsCount: List[((Int, Int), Int)]
                     ): (Map[Int, List[Int]], Map[Int, Int], Array[(Int, Int)], List[Int]) = {
    val maps = singleLhsCount.toMap
    val equalAttrMap = mutable.Map.empty[Int, List[Int]]
    val ordersMap = mutable.Map.empty[Int, Int]
    val tmp = mutable.Set.empty[Int]
    val del = mutable.ListBuffer.empty[Int]
    Range(1, colSize + 1).foreach(tmp.add(_))

    equalAttr.foreach { x =>
      val maxAttr = x.maxBy(y => maps(y))
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

    val orders = ordersMap.toArray.map(x => (x._1, maps(x._2)))
      .sortWith((x, y) => x._2 > y._2)
    val delSet = del.toSet
    val swappedOrdersMap = ordersMap.map(x => (x._2, x._1))
    twoAttrsCount.map(x => (Set[Int](x._1._1, x._1._2), x._2))
      .filter(x => (x._1 & delSet).isEmpty)
      .foreach(x => attrsCountMap.put(x._1.map(swappedOrdersMap(_)), x._2))

    (equalAttrMap.toMap, ordersMap.toMap, orders, del.toList.sorted)
  }

  def getNewBottomFDs(singleFDs: Array[(Int, Int)],
                      ordersMap: Map[Int, Int],
                      equalAttrMap: Map[Int, List[Int]]): Array[(Set[Int], Int)] = {
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

  private def repart(sc: SparkContext,
                     rdd: RDD[Array[String]],
                     attribute: Int): RDD[(String, List[Array[String]])] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _)

    partitions
  }

  def recoverAllFDs(results: List[(Set[Int], Int)],
                    equalAttrMap: Map[Int, List[Int]],
                    ordersMap: Map[Int, Int]): Map[Set[Int], List[Int]] = {
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

    fds.toList.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
  }

}
