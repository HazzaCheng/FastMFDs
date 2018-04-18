package com.hazzacheng.FD.utils

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
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
object RddUtils {

  def readAsRdd(sc: SparkContext,
                filePath: String,
                del: scala.List[Int]): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath, sc.defaultParallelism)
      .map(line => line.split(",")
        .zipWithIndex.filter(x => !del.contains(x._2 + 1)).map(_._1))

    rdd
  }

  def outPutFormat(minFD: Map[Set[Int], List[Int]]): List[String] = {
    minFD.map(d => d._1.toList.sorted.map(x => "column" + x).mkString("[", ",", "]")
      + ":" + d._2.sorted.map(x => "column" + x).mkString(",")).toList
  }

  def repart(rdd: RDD[Array[Int]],
                     attribute: Int): RDD[(Int, List[Array[Int]])] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _)

    partitions
  }

  def getMinimalFDs(sc: SparkContext,
                    partitionsRDD: RDD[(Int, List[Array[Int]])],
                    fds: List[(Set[Int], mutable.Set[Int])],
                    partitionSize: Int,
                    colSize: Int,
                    levelMap: mutable.HashMap[Int, mutable.HashSet[(Set[Int], Int)]]
                   ): (Array[(Set[Int], Int)], Set[(Set[Int], Int)], Array[(Int, List[(Set[Int], Int)])]) = {

    val fdsBV = sc.broadcast(fds)
    val levelMapBV = sc.broadcast(levelMap)

    val tuplesRDD = partitionsRDD.map(p => checkEachPartition(fdsBV, levelMapBV, p, colSize))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val duplicatesRDD = tuplesRDD.flatMap(x => x._2)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val candidates = duplicatesRDD.map(x => (x, 1)).reduceByKey(_ + _).collect()
    // TODO: local vs parallel
    val minimalFDs = candidates.filter(_._2 == partitionSize).map(_._1)

    val failFDs = duplicatesRDD.collect().distinct.toSet -- minimalFDs
    val partWithFailFDs = tuplesRDD.collect()

    tuplesRDD.unpersist()
    duplicatesRDD.unpersist()
    fdsBV.unpersist()

    (minimalFDs, failFDs, partWithFailFDs)
  }

  private def checkEachPartition(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                 levelMapBV: Broadcast[mutable.HashMap[Int, mutable.HashSet[(Set[Int], Int)]]],
                                 partition: (Int, List[Array[Int]]),
                                 colSize: Int): (Int, List[(Set[Int], Int)]) = {

    val levelMap = levelMapBV.value.getOrElse(partition._1, mutable.HashSet.empty[(Set[Int], Int)])
    val minimalFDs = fdsBV.value.flatMap(fd => checkFD(partition._2, levelMap, fd._1, fd._2, colSize))

    (partition._1, minimalFDs)
  }

  def checkFD(partition: List[Array[Int]],
              levelMap: mutable.HashSet[(Set[Int], Int)],
              lhs: Set[Int],
              rhs: mutable.Set[Int],
              colSize: Int): List[(Set[Int], Int)] = {

    val true_rhs = rhs.clone()
    val tmp = mutable.Set.empty[Int]
    val dict = mutable.HashMap.empty[String, Array[Int]]

    rhs.foreach{r =>
      if (levelMap contains (lhs, r)) {
        tmp.add(r)
        true_rhs.remove(r)
      }
    }

    partition.foreach(line => {
      val left = takeAttrs(line, lhs)
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
                 partitionsRDD: RDD[(Int, List[Array[Int]])],
                 fds: List[(Set[Int], mutable.Set[Int])],
                 colSize: Int,
                 topFDs: mutable.Set[(Set[Int], Int)],
                 levelMap: mutable.HashMap[Int, mutable.HashMap[Set[Int], Int]]
                ): Array[(Set[Int], Int)] = {
    val fdsBV = sc.broadcast(fds)
    val levelMapBV = sc.broadcast(levelMap)
    val tuples = partitionsRDD.map(p => checkEachPartitionForWrong(fdsBV, levelMapBV, p, colSize)).collect()
    val failFDs = tuples.flatMap(_._2.flatMap(_._1)).distinct
    val counts = tuples.map(x => (x._1, x._2.map(_._2)))

    levelMapBV.unpersist()
    fdsBV.unpersist()

    val rightFDs = fds.flatMap(x => x._2.map(y => (x._1, y))).toSet -- failFDs
    topFDs ++= rightFDs

    levelMap.clear()
    counts.foreach{x =>
      val map = mutable.HashMap.empty[Set[Int], Int]
      x._2.foreach(y => map.put(y._1, y._2))
      levelMap.put(x._1, map)
    }

    failFDs
  }

  private def checkEachPartitionForWrong(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                         levelMapBV: Broadcast[mutable.HashMap[Int, mutable.HashMap[Set[Int], Int]]],
                                         partition: (Int, List[Array[Int]]),
                                         colSize: Int
                                        ): (Int,  List[(List[(Set[Int], Int)], (Set[Int], Int))]) = {
    val levelMap = levelMapBV.value.getOrElse(partition._1, mutable.HashMap.empty[Set[Int], Int])
    val wrongFDs = fdsBV.value.map(fd => checkFD(partition._2, levelMap, fd._1, fd._2, colSize))

    (partition._1, wrongFDs)
  }

  def checkFD(partition: List[Array[Int]],
              levelMap: mutable.HashMap[Set[Int], Int],
              lhs: Set[Int],
              rhs: mutable.Set[Int],
              colSize: Int): (List[(Set[Int], Int)], (Set[Int], Int)) = {
    val (existRhs, nonExistRhs) = rhs.partition(r => levelMap.contains(lhs + r))
    val lhsCount = partition.map(p => (takeAttrs(p, lhs), 1)).groupBy(_._1).size
    val nonExistRhsCount = nonExistRhs.map(r => (r, partition.map(p => (takeAttrs(p, lhs + r), 1)).groupBy(_._1).size))
    val existRhsCount = existRhs.map(x => (x, levelMap(lhs + x)))
    val wrong = (nonExistRhsCount ++ existRhsCount).filter(r => r._2 != lhsCount).map(x => (lhs, x._1))
    //nonExistRhsCount.foreach(r => levelMap.put(lhs + r._1, r._2))

    (wrong.toList, (lhs, lhsCount))
  }

  private def takeAttrs(arr: Array[Int],
                        attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder

    attributes.toList.foreach{attr =>
      val a = arr(attr - 1)
      s.append(a + " ")
    }

    s.toString()//.hashCode
  }

  private def takeAttrRHS(arr: Array[Int],
                          attributes: mutable.Set[Int],
                          colSize: Int): Array[Int] = {
    val res = new Array[Int](colSize + 1)
    attributes.toList.foreach(attr => res(attr) = arr(attr - 1))

    res
  }

  def updateLevelMap(cuttedFDsMap: mutable.HashMap[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]],
                     partWithFailFDs: Array[(Int, List[(Set[Int], Int)])],
                     levelMap: mutable.HashMap[Int, mutable.HashSet[(Set[Int], Int)]],
                     level: Int): Unit = {
    partWithFailFDs.foreach{x =>
     /* val cutted = levelMap.getOrElse(x._1, mutable.HashSet.empty[(Set[Int], Int)])
        .filter(_._1.size > level)*/
      val cutted = mutable.HashSet.empty[(Set[Int], Int)]

      x._2.foreach{y =>
        val value = cuttedFDsMap.getOrElse(y, mutable.HashSet.empty[(Set[Int], Int)])
        if (value.nonEmpty) cutted ++= value
      }

      levelMap.update(x._1, cutted)
    }
  }

  def getTopFDs(df: DataFrame,
                wholeRDD: RDD[Array[Int]],
                rdds: mutable.Map[Int, RDD[(Int, List[Array[Int]])]],
                topCandidates: mutable.Set[(Set[Int], Int)],
                col: Int,
                rhsCount: mutable.Map[Int, Int]
               ): (mutable.Set[(Set[Int], Int)], Array[(Set[Int], Int)]) = {

    val topFDs = mutable.Set.empty[(Set[Int], Int)]
    val wrongFDs = mutable.ListBuffer.empty[(Set[Int], Int)]

    val (r, f) = topCandidates.partition(_._2 != col)

    if (f.nonEmpty) {
      val fd = f.head
      val cols = df.columns
      val lhs = fd._1.map(y => cols(y - 1)).toArray
      val whole = df.groupBy(cols(fd._2 - 1), lhs: _*).count().count().toInt
      if (whole >= rhsCount(fd._2)) {
        val left = df.groupBy(lhs.head, lhs.tail: _*).count().count().toInt

        if (whole == left) topFDs.add(fd)
      } else wrongFDs.append(fd)
    }

    if (r.nonEmpty) {
      val rdd = rdds.getOrElseUpdate(col, RddUtils.repart(wholeRDD, col).persist(StorageLevel.MEMORY_AND_DISK))
      val (right, wrong) = getFailFDs(rdd, r.toList)
      topFDs ++= right
      wrongFDs ++= wrong
    }


    (topFDs, wrongFDs.toArray)
  }

  def getFailFDs(partitionsRDD: RDD[(Int, List[Array[Int]])],
                 fds: List[(Set[Int], Int)]
                ): (Set[(Set[Int], Int)], Array[(Set[Int], Int)]) = {
    val tuples = partitionsRDD.flatMap(p => checkEachPartitionForWrong(fds, p)).collect()
    val failFDs = tuples.distinct

    val rightFDs = fds.toSet -- failFDs

    (rightFDs, failFDs)
  }

  private def checkEachPartitionForWrong(fds: List[(Set[Int], Int)],
                                         partition: (Int, List[Array[Int]])
                                        ): List[(Set[Int], Int)] = {
    val wrongFDs = fds.flatMap(fd => checkFD(partition._2, fd._1, fd._2))

    wrongFDs
  }

  def checkFD(partition: List[Array[Int]],
              lhs: Set[Int],
              rhs: Int
              ): List[(Set[Int], Int)] = {
    val lhsCount = partition.map(p => (takeAttrs(p, lhs), 1)).groupBy(_._1).size
    val rhsCount = partition.map(p => (takeAttrs(p, lhs + rhs), 1)).groupBy(_._1).size

    if (lhsCount == rhsCount) List((lhs, rhs))
    else List.empty[(Set[Int], Int)]
  }

}
