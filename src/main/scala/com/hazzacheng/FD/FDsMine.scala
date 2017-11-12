package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-10-06
  * Time: 9:01 AM
  */

object FDsMine {
  private val parallelScaleFactor = 4
  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]],
                  colSize: Int, orders: Array[(Int, Long)]): Map[Set[Int], mutable.Set[Int]] = {
    val dependencies = FDUtils.getDependencies(colSize)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    var sum = 0
    for (i <- orders) {
      time2 = System.currentTimeMillis()
      val candidates = FDUtils.getCandidateDependencies(dependencies, i._1)
      val lhsWithSize = candidates.toList.groupBy(_._1.size)
      val keys = lhsWithSize.keys.toList.sortWith((x, y) => x < y)

      val partitionsRDD = repart(sc, rdd, i._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
//      var partitionsLocal: Array[List[Array[String]]] = Array()
//      if (i._2 <= 20) partitionsLocal = partitionsRDD.collect().map(x => x._2)
      for (k <- keys) {
        val fds = lhsWithSize(k)

        println("========Size- lhs:"  + i + " " + k + " " + fds.size)
//        println("========Size- depend:"  + i + " " + k + " " + FDUtils.getDependenciesNums(fds))
        if (fds.nonEmpty) {
          val successed = ListBuffer.empty[(Set[Int], Int)]
          time1 = System.currentTimeMillis()
          checkPartitions(sc, partitionsRDD, fds, successed)
          println("===========Paritions Small" + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))

          time1 = System.currentTimeMillis()
          println("========Size- failed:"  + i + " " + k + " " + failed.size)
          if (failed.nonEmpty) {
            val leaves = cutLeaves(dependencies, candidates, failed.toList, i._1)
            println("===========Size- reduce leaves " + i + " " + k + " " + leaves)
            println("===========Cut Leaves" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " + (System.currentTimeMillis() - time1))
          }
        }
      }

      results ++= candidates
      println("===========Common Attr" + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }
    println("============Tasks============" + sum + "======================")
    time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(sc, results)
    println("===========FindMinFD Use Time=============" + System.currentTimeMillis() + " " + time1 + " " +(System.currentTimeMillis() - time1))
    if (emptyFD.nonEmpty) results += (Set.empty[Int] -> emptyFD)

    minFD
  }


  def repart(sc: SparkContext,
             rdd: RDD[Array[String]],
             attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(_._2)

    partitions
  }

  def checkPartitions(sc: SparkContext,
                      partitionsRDD: RDD[List[Array[String]]],
                      fds: List[(Set[Int], mutable.Set[Int])],
                      successed: ListBuffer[(Set[Int], Int)]): Unit = {
    val successedFD = mutable.ListBuffer.empty[(Set[Int], Int)]
    val fdsBV = sc.broadcast(fds)
    val minimalFDs = partitionsRDD.map(p => checkFDs(fdsBV, p)).collect()

    tupleInfo.foreach(tuple => {
      dict.put(tuple._2, tuple._3)
      failedFD ++= tuple._1
    })
    failed ++= failedFD.toList.distinct
    fdsBV.unpersist()

  }

  def checkFDs(fdsBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
               partition: List[Array[String]]
              ): (List[(Set[Int], Int)], String, mutable.HashMap[Set[Int], Int]) = {
    val failed = mutable.ListBuffer.empty[(Set[Int],Int)]
    val res = fdsBV.value.toList.map(fd =>
      FDUtils.checkEach(partition, fd._1, fd._2))
    res.foreach(r => {
      failed ++= r._1
      dict.put(r._2._1, r._2._2)
    })
    (failed.toList.distinct, partition._1, dict)
//    val res = FDUtils.checkAll(partition._2, fdsBV.value, map)
//    (res._1, partition._1, res._2)
  }

  def check(partition: List[Array[String]],
            lhs: Set[Int],
            rhs: Set[Int]): List[(Set[Int], Int)] = {

  }

  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                failed: List[(Set[Int], Int)], commonAttr: Int) :Int = {
    var sum = 0
    for (d <- failed) {
      val subSets = FDUtils.getSubsets(d._1.toArray)
      for (subSet <- subSets) {
        if (subSet contains commonAttr) sum += FDUtils.cut(candidates, subSet, d._2)
        else sum += FDUtils.cut(dependencies, subSet, d._2)
      }
    }
    sum
  }


}
