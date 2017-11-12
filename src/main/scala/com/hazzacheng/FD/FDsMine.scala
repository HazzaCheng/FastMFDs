package com.hazzacheng.FD

import com.hazzacheng.FD.FDUtils.{takeAttrLHS, takeAttrRHS}
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
                  colSize: Int, orders: Array[(Int, Long)]): Map[Set[Int], List[Int]] = {
    val dependencies = FDUtils.getDependencies(colSize)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.ListBuffer.empty[(Set[Int], Int)]

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
          checkPartitions(sc, partitionsRDD, fds, results)
          println("===========Paritions " + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))

          time1 = System.currentTimeMillis()
          println("========Size- successed:"  + i + " " + k + " " + successed.size)
          if (successed.nonEmpty) {
            val leaves = cutLeaves(dependencies, candidates, successed.toList, i._1)
            println("===========Size- reduce leaves " + i + " " + k + " " + leaves)
            println("===========Cut Leaves" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " + (System.currentTimeMillis() - time1))
          }
        }
      }

      println("===========Common Attr" + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }
    if (emptyFD.nonEmpty)
      emptyFD.foreach(rhs => results.append((Set.empty[Int], rhs)))

    results.toList.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
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
                      res: mutable.ListBuffer[(Set[Int], Int)]): Unit = {
    val successedFD = mutable.ListBuffer.empty[(Set[Int], Int)]
    val fdsBV = sc.broadcast(fds)
    val minimalFDs = partitionsRDD.flatMap(p => checkFDs(fdsBV, p)).collect().distinct

    res ++= minimalFDs
    fdsBV.unpersist()
  }

  def checkFDs(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
               partition: List[Array[String]]
              ): List[(Set[Int], Int)] = {
    val failed = mutable.ListBuffer.empty[(Set[Int],Int)]
    val minimalFDs = fdsBV.value.toList.flatMap(fd => check(partition, fd._1, fd._2))

    minimalFDs
  }

  def check(partition: List[Array[String]],
            lhs: Set[Int],
            rhs: mutable.Set[Int]): List[(Set[Int], Int)] = {
    val true_rhs = rhs.clone()
    val dict = mutable.HashMap.empty[String, Array[String]]
    partition.foreach(d => {
      val left = takeAttrLHS(d, lhs)
      val right = takeAttrRHS(d, rhs)
      if(dict.contains(left)){
        for(i <- true_rhs){
          if(!dict(left)(i).equals(right(i))){
            true_rhs -= i
          }
        }
      }
      else dict += left -> right
    })

    true_rhs.map(rhs => (lhs, rhs)).toList
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
