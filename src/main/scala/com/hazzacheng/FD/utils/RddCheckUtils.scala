package com.hazzacheng.FD.utils

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-11-26 
  * Time: 12:58 PM
  */
object RddCheckUtils {

  def readAsRdd(sc: SparkContext,
                filePath: String,
                del: scala.List[Int]): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath, sc.defaultParallelism * 4)
      .map(line => line.split(",")
        .zipWithIndex.filter(x => !del.contains(x._2 + 1)).map(_._1))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    rdd
  }

  def checkInLowLevel(sc: SparkContext,
                      level: Int,
                      common: Int,
                      partitionSize: Int,
                      partitionRDD: RDD[(String, scala.List[Array[String]])],
                      candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                      newColSize: Int,
                      wholeCuttedMap: mutable.HashMap[Int, mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]],
                      topFDs: mutable.Set[(Set[Int], Int)],
                      results: mutable.ListBuffer[(Set[Int], Int)]
                     ): Unit = {
    val toChecked = CandidatesUtils.getTargetCandidates(candidates, common, level).toList

    if (toChecked.nonEmpty) {
      val levelMap = wholeCuttedMap(common)

      val (minimalFDs, failFDs, partWithFailFDs) =
        RddCheckUtils.getMinimalFDs(sc, partitionRDD, toChecked, results, partitionSize, newColSize, levelMap)
      CandidatesUtils.cutFromDownToTop(candidates, minimalFDs)
      CandidatesUtils.cutInTopLevels(topFDs, minimalFDs)
      if (failFDs.nonEmpty) {
        val cuttedFDsMap = CandidatesUtils.getCuttedFDsMap(candidates, failFDs)
        RddCheckUtils.updateLevelMap(cuttedFDsMap, partWithFailFDs, levelMap, level)
      }

      wholeCuttedMap.update(common, levelMap)
    }
  }

  def checkInHighLevel(sc: SparkContext,
                       level: Int,
                       common: Int,
                       partitionSize: Int,
                       partitionRDD: RDD[(String, scala.List[Array[String]])],
                       candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                       newColSize: Int,
                       wholeCountMap: mutable.HashMap[Int, mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]],
                       topFDs: mutable.Set[(Set[Int], Int)],
                       results: mutable.ListBuffer[(Set[Int], Int)]
                      ): Unit = {
    val toChecked = CandidatesUtils.getTargetCandidates(candidates, common, level).toList
    if (toChecked.nonEmpty) {
      val levelMap = wholeCountMap(common)
      val failFDs = RddCheckUtils.getFailFDs(sc, partitionRDD, toChecked, newColSize, levelMap)
      val rightFDs = toChecked.flatMap(x => x._2.map((x._1, _))).toSet -- failFDs
      topFDs ++= rightFDs
      CandidatesUtils.cutFromTopToDown(candidates, failFDs)

      wholeCountMap.update(common, levelMap)
    }
  }

  def getMinimalFDs(sc: SparkContext,
                    partitionsRDD: RDD[(String, List[Array[String]])],
                    fds: List[(Set[Int], mutable.Set[Int])],
                    res: mutable.ListBuffer[(Set[Int], Int)],
                    partitionSize: Int,
                    colSize: Int,
                    levelMap: mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]
                   ): (Array[(Set[Int], Int)], Set[(Set[Int], Int)], Array[(String, List[(Set[Int], Int)])]) = {
    val fdsBV = sc.broadcast(fds)
    val levelMapBV = sc.broadcast(levelMap)
    val tuplesRDD = partitionsRDD.map(p => checkEachPartition(fdsBV, levelMapBV, p, colSize))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val duplicatesRDD = tuplesRDD.flatMap(x => x._2)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val candidates = duplicatesRDD.map(x => (x, 1)).reduceByKey(_ + _).collect()
    // TODO: local vs parallel
    val minimalFDs = candidates.filter(_._2 == partitionSize).map(_._1)
    res ++= minimalFDs

    val failFDs = duplicatesRDD.collect().distinct.toSet -- minimalFDs
    val partWithFailFDs = tuplesRDD.collect()

    tuplesRDD.unpersist()
    duplicatesRDD.unpersist()
    fdsBV.unpersist()

    (minimalFDs, failFDs, partWithFailFDs)
  }

  private def checkEachPartition(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                 levelMapBV: Broadcast[mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]],
                                 partition: (String, List[Array[String]]),
                                 colSize: Int): (String, List[(Set[Int], Int)]) = {
    val levelMap = levelMapBV.value.getOrElse(partition._1, mutable.HashSet.empty[(Set[Int], Int)])
    val minimalFDs = fdsBV.value.flatMap(fd => checkFD(partition._2, levelMap, fd._1, fd._2, colSize))

    (partition._1, minimalFDs)
  }

  def checkFD(partition: List[Array[String]],
              levelMap: mutable.HashSet[(Set[Int], Int)],
              lhs: Set[Int],
              rhs: mutable.Set[Int],
              colSize: Int): List[(Set[Int], Int)] = {
    val true_rhs = rhs.clone()
    val tmp = mutable.Set.empty[Int]
    val dict = mutable.HashMap.empty[String, Array[String]]

    rhs.foreach{r =>
      if (levelMap contains (lhs, r)) {
        tmp.add(r)
        true_rhs.remove(r)
      }
    }

    partition.foreach(line => {
      val left = takeAttrLHS(line, lhs)
      val right = takeAttrRHS(line, true_rhs, colSize)
      val value = dict.getOrElse(left, null)
      if (value != null) {
        for (i <- true_rhs.toList)
          if (!value(i).equals(right(i))) true_rhs.remove(i)
      } else dict.put(left, right)
      if (true_rhs.isEmpty) return List()
    })

    true_rhs ++= tmp

    true_rhs.map(r => (lhs, r)).toList
  }

  def getFailFDs(sc: SparkContext,
                 partitionsRDD: RDD[(String, List[Array[String]])],
                 fds: List[(Set[Int], mutable.Set[Int])],
                 colSize: Int,
                 levelMap: mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]
                ): Array[(Set[Int], Int)] = {
    val fdsBV = sc.broadcast(fds)
    val levelMapBV = sc.broadcast(levelMap)
    val tuples = partitionsRDD.map(p => checkEachPartitionForWrong(fdsBV, levelMapBV, p, colSize)).collect()
    val failFDs = tuples.flatMap(_._2.flatMap(_._1)).distinct
    val counts = tuples.map(x => (x._1, x._2.map(_._2)))

    levelMapBV.unpersist()
    fdsBV.unpersist()

    levelMap.clear()
    counts.foreach{x =>
      val map = mutable.HashMap.empty[Set[Int], Int]
      x._2.foreach(y => map.put(y._1, y._2))
      levelMap.put(x._1, map)
    }

    failFDs
  }

  private def checkEachPartitionForWrong(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                         levelMapBV: Broadcast[mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]],
                                         partition: (String, List[Array[String]]),
                                         colSize: Int
                                        ): (String,  List[(List[(Set[Int], Int)], (Set[Int], Int))]) = {
    val levelMap = levelMapBV.value.getOrElse(partition._1, mutable.HashMap.empty[Set[Int], Int])
    val minimalFDs = fdsBV.value.map(fd => checkFD(partition._2, levelMap, fd._1, fd._2, colSize))

    (partition._1, minimalFDs)
  }

  def checkFD(partition: List[Array[String]],
              levelMap: mutable.HashMap[Set[Int], Int],
              lhs: Set[Int],
              rhs: mutable.Set[Int],
              colSize: Int): (List[(Set[Int], Int)], (Set[Int], Int)) = {
    val (existRhs, nonExistRhs) = rhs.partition(r => levelMap.contains(lhs + r))
    val lhsCount = partition.map(p => (takeAttrLHS(p, lhs), 1)).groupBy(_._1).size
    val nonExistRhsCount = nonExistRhs.map(r => (r, partition.map(p => (takeAttrLHS(p, lhs + r), 1)).groupBy(_._1).size))
    val existRhsCount = existRhs.map(x => (x, levelMap(lhs + x)))
    val wrong = (nonExistRhsCount ++ existRhsCount).filter(r => r._2 != lhsCount).map(x => (lhs, x._1))
    nonExistRhsCount.foreach(r => levelMap.put(lhs + r._1, r._2))

    (wrong.toList, (lhs, lhsCount))
  }

  private def takeAttrLHS(arr: Array[String],
                          attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.toList.foreach(attr => s.append(arr(attr - 1) + " "))

    s.toString()
  }

  private def takeAttrRHS(arr: Array[String],
                          attributes: mutable.Set[Int],
                          colSize: Int): Array[String] = {
    val res = new Array[String](colSize + 1)
    attributes.toList.foreach(attr => res(attr) = arr(attr - 1))
    res
  }

  def updateLevelMap(cuttedFDsMap: mutable.HashMap[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]],
                     partWithFailFDs: Array[(String, List[(Set[Int], Int)])],
                     levelMap: mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]],
                     level: Int): Unit = {
    partWithFailFDs.foreach{x =>
      val cutted = levelMap.getOrElse(x._1, mutable.HashSet.empty[(Set[Int], Int)])
        .filter(_._1.size > level)

      x._2.foreach{y =>
        val value = cuttedFDsMap.getOrElse(y, mutable.HashSet.empty[(Set[Int], Int)])
        if (value.nonEmpty) cutted ++= value
      }

      if (cutted.nonEmpty) levelMap.update(x._1, cutted)
    }
  }


}
