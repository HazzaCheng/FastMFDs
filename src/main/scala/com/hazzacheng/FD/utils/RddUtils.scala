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
                     attribute: Int): RDD[List[Array[Int]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(_._2)

    partitions
  }

  def getMinimalFDs(sc: SparkContext,
                    partitionsRDD: RDD[List[Array[Int]]],
                    fds: List[(Set[Int], mutable.Set[Int])],
                    partitionSize: Int,
                    colSize: Int
                   ): Array[(Set[Int], Int)] = {

    val fdsBV = sc.broadcast(fds)

    val duplicatesRDD = partitionsRDD.flatMap(p => checkEachPartition(fdsBV, p, colSize))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val candidates = duplicatesRDD.map(x => (x, 1)).reduceByKey(_ + _).collect()
    // TODO: local vs parallel
    val minimalFDs = candidates.filter(_._2 == partitionSize).map(_._1)

    duplicatesRDD.unpersist()
    fdsBV.unpersist()

    minimalFDs
  }

  private def checkEachPartition(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                 partition: List[Array[Int]],
                                 colSize: Int): List[(Set[Int], Int)] = {

    val minimalFDs = fdsBV.value.flatMap(fd => checkRightFD(partition, fd._1, fd._2, colSize))

    minimalFDs
  }

  def checkRightFD(partition: List[Array[Int]],
              lhs: Set[Int],
              rhs: mutable.Set[Int],
              colSize: Int): List[(Set[Int], Int)] = {

    val trueRhs = rhs.clone()
    val dict = mutable.HashMap.empty[String, Array[Int]]

    partition.foreach(line => {
      val left = takeAttrs(line, lhs)
      val right = takeAttrRHS(line, trueRhs, colSize)
      val value = dict.getOrElse(left, null)
      if (value != null) {
        for (i <- trueRhs.toList)
          if (!value(i).equals(right(i))) trueRhs.remove(i)
      } else dict.put(left, right)
      if (trueRhs.isEmpty) return List()
    })


    trueRhs.map(r => (lhs, r)).toList
  }

  def getFailFDs(sc: SparkContext,
                 partitionsRDD: RDD[List[Array[Int]]],
                 fds: List[(Set[Int], mutable.Set[Int])],
                 colSize: Int
                ): (Array[(Set[Int], Int)], Array[(Set[Int], Int)]) = {
    val fdsBV = sc.broadcast(fds)
    val failFDs = partitionsRDD.flatMap(p => checkEachPartitionForWrong(fdsBV, p, colSize)).collect().distinct

    fdsBV.unpersist()

    val rightFDs = fds.flatMap(x => x._2.map(y => (x._1, y))).toSet -- failFDs

    (failFDs, rightFDs.toArray)
  }

  private def checkEachPartitionForWrong(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                         partition: List[Array[Int]],
                                         colSize: Int
                                        ): List[(Set[Int], Int)] = {
    val wrongFDs = fdsBV.value.flatMap(fd => checkWrongFD(partition, fd._1, fd._2, colSize))

    wrongFDs
  }

  def checkWrongFD(partition: List[Array[Int]],
                   lhs: Set[Int],
                   rhs: mutable.Set[Int],
                   colSize: Int): List[(Set[Int], Int)] = {

    val trueRhs = rhs.clone()
    val wrongRhs = mutable.Set.empty[Int]
    val dict = mutable.HashMap.empty[String, Array[Int]]

    partition.foreach(line => {
      val left = takeAttrs(line, lhs)
      val right = takeAttrRHS(line, trueRhs, colSize)
      val value = dict.getOrElse(left, null)
      if (value != null) {
        for (i <- trueRhs.toList) {
          if (!value(i).equals(right(i))) {
            wrongRhs.add(i)
            trueRhs.remove(i)
          }
        }
      } else dict.put(left, right)
      if (trueRhs.isEmpty) return wrongRhs.map(r => (lhs, r)).toList
    })

    wrongRhs.map(r => (lhs, r)).toList
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

}
