package com.hazzacheng.FD

import com.hazzacheng.FD.temp.FDsUtils
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

    // get fds with single lhs
    val (singleFDs, singleLhsCount, twoAttrsCount) = DataFrameCheckUtils.getBottomFDs(df, colSize)
    // get equal attributes
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    // get new orders
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(lessAttrsCountMap, equalAttr, singleLhsCount, colSize, twoAttrsCount)
    val newColSize = orders.length
    // create the new single lhs fds
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)
    results ++= bottomFDs
    // get new df
    val newDF = DataFrameCheckUtils.getNewDF(filePath, df, del.toSet).persist(StorageLevel.MEMORY_AND_DISK_SER)
    df.unpersist()
    // check the fds with the longest lhs
    val topCandidates = getLongestLhs(newColSize)
    CandidatesUtils.cutInTopLevels(topCandidates, bottomFDs)
    val (topFDs, wrongTopFDs) = DataFrameCheckUtils.getTopFDs(moreAttrsCountMap, newDF, topCandidates)
    // get all candidates FD without bottom level and top level
    val candidates = CandidatesUtils.removeTopAndBottom(CandidatesUtils.getCandidates(newColSize), newColSize)
    // cut from bottom level and top level
    CandidatesUtils.cutFromDownToTop(candidates, bottomFDs)
    CandidatesUtils.cutFromTopToDown(candidates, wrongTopFDs)

    // find the minimal fds in the middle levels
    if (newColSize <= 10)
      findByDf(newDF, newColSize, candidates, lessAttrsCountMap, moreAttrsCountMap, topFDs, results)
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
      // TODO: get the minimal FDs from topFDs
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
                       results: mutable.ListBuffer[(Set[Int], Int)]
                      ): Unit = {
    val middle = (newColSize + 1) / 2
    for (level <- 2 to middle) {
      val toCheckedLow = CandidatesUtils.getLevelCandidates(candidates, level)
      if (toCheckedLow.nonEmpty) {
        val minimalFds = DataFrameCheckUtils.getMinimalFDs(newDF, toCheckedLow, lessAttrsCountMap)
        results ++= minimalFds
        CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
        CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
      }

      val symmetrical = newColSize - level
      if (level != symmetrical) {
        val toCheckedHigh = CandidatesUtils.getLevelCandidates(candidates, symmetrical)
        if (toCheckedHigh.nonEmpty) {
          val failFDs = DataFrameCheckUtils.getFailFDs(newDF, toCheckedHigh, moreAttrsCountMap, topFDs)
          CandidatesUtils.cutFromTopToDown(candidates, failFDs)
        }
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
    val spiltLevel = 2
    for (level <- 2 to spiltLevel) {
      val toCheckedLow = CandidatesUtils.getLevelCandidates(candidates, level)
      if (toCheckedLow.nonEmpty) {
        val minimalFds = DataFrameCheckUtils.getMinimalFDs(newDF, toCheckedLow, lessAttrsCountMap)
        results ++= minimalFds
        CandidatesUtils.cutFromDownToTop(candidates, minimalFds)
        CandidatesUtils.cutInTopLevels(topFDs, minimalFds)
      }

      val symmetrical = newColSize - level
      if (level != symmetrical) {
        val toCheckedHigh = CandidatesUtils.getLevelCandidates(candidates, symmetrical)
        if (toCheckedHigh.nonEmpty) {
          val failFDs = DataFrameCheckUtils.getFailFDs(newDF, toCheckedHigh, moreAttrsCountMap, topFDs)
          CandidatesUtils.cutFromTopToDown(candidates, failFDs)
        }
      }
    }

    if (candidates.nonEmpty) {
      // create the RDD
      val rdd = RddCheckUtils.readAsRdd(sc, filePath, del)

      // create the map which save cutted leaves from bottom to top
      val wholeCuttedMap = mutable.HashMap
        .empty[Int, mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]]
      // create the map which save count from top to bottom
      val wholeCountMap = mutable.HashMap
        .empty[Int, mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]]

      // get all the partitions by common attributes
      val partitions = new Array[RDD[(String, List[Array[String]])]](newColSize)
      for (common <- orders) {
        wholeCuttedMap.put(common._1, mutable.HashMap.empty[String, mutable.HashSet[(Set[Int], Int)]])
        wholeCountMap.put(common._1, mutable.HashMap.empty[String, mutable.HashMap[Set[Int], Int]])
        partitions(common._1 - 1) = repart(sc, rdd, common._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }

      val middle = (newColSize + 1) / 2
      for (level <- (spiltLevel + 1) to middle) {
        for (common <- orders) {
          val partitionRDD = partitions(common._1 - 1)
          RddCheckUtils.checkInLowLevel(sc, level, common._1, common._2, partitionRDD,
            candidates, newColSize, wholeCuttedMap, topFDs, results)
        }

        val symmetrical= newColSize - level
        if (level != symmetrical) {
          for (common <- orders) {
            val partitionRDD = partitions(common._1 - 1)
            RddCheckUtils.checkInHighLevel(sc, symmetrical, common._1, common._2, partitionRDD,
              candidates, newColSize, wholeCountMap, topFDs, results)
          }
        }
      }

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
      .foreach(x => attrsCountMap.put(x._1.map(ordersMap(_)), x._2))

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
