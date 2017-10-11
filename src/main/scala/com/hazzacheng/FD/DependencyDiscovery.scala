package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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

object DependencyDiscovery {
  private val parallelScaleFactor = 4

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val nums = rdd.first().length
    val dependencies = FDUtils.getDependencies(nums)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (i <- 1 to nums) {
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val partitions = repart(sc, rdd, i).cache()
      if (partitions.count() == 1) emptyFD += i

      for (k <- keys) {
        val candidatesBV = sc.broadcast(candidates)
        val ls = lhsAll.get(k).get
        val lsBV = sc.broadcast(ls)
        val failedTemp = partitions.flatMap(p => checkDependencies(p, candidatesBV, lsBV)).collect()
        val failed = failedTemp.distinct
//        val failed = sc.parallelize(ls).flatMap(lhs => checkDependencies(partitions, candidatesBV, lhs)).collect()
        cutLeaves(dependencies, candidates, failed, i)
      }
      partitions.unpersist()
      results ++= candidates
    }
    println("===========Start Find FD=============")
    val time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(results)
    println("===========Use Time=============" + (System.currentTimeMillis() - time1))
    if (emptyFD.size > 0) results += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(t => t._2).repartition(sc.defaultParallelism * parallelScaleFactor)

    partitions
  }

//  def checkDependencies(partitions: RDD[List[Array[String]]],
//                        candidatesBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
//                        lhs: Set[Int]): Array[(Set[Int], Int)] = {
//    val existed = candidatesBV.value.get(lhs)
//    if (existed != None) {
//      val rs = existed.get.toList
//      val failed = partitions.flatMap(p => FDUtils.check(p, lhs.toList, rs))
//        .distinct().map(rhs => (lhs, rhs))
//      failed.collect()
//    } else Array()
//  }

  def checkDependencies(p: List[Array[String]],
                        candidatesBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
                        lsBV: Broadcast[List[Set[Int]]]): List[(Set[Int], Int)] = {
    val failed = new ListBuffer[(Set[Int], Int)]()
    for (lhs <- lsBV.value) {
      val existed = candidatesBV.value.get(lhs)
      if (existed != None) {
        val rs = existed.get.toList
        val fail = FDUtils.check(p, lhs.toList, rs).map(rhs => (lhs, rhs))
        failed ++= fail
      }
    }

    failed.toList
  }


  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                failed: Array[(Set[Int], Int)], commonAttr: Int) = {
    for (d <- failed) {
      val subSets = FDUtils.getSubsets(d._1.toArray)
      for (subSet <- subSets) {
        if (subSet contains commonAttr) FDUtils.cut(candidates, subSet, d._2)
        else FDUtils.cut(dependencies, subSet, d._2)
      }
    }

  }


  def findMinFD(fd :mutable.HashMap[Set[Int], mutable.Set[Int]]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val minFD = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    val keys = fd.keySet.toList.sortWith((x,y) => x.size < y.size)
    for(k1 <- keys){
      for(k2 <- keys){
        if (fd.contains(k2) && fd.contains(k1)) {
          if (FDUtils.isSubset(k1, k2) && (fd(k1) & fd(k2)).nonEmpty) {
            fd(k2) --= fd(k1) & fd(k2)
            if (fd(k2).isEmpty) fd -= k2
          }
        }
      }
    }

    fd
  }

}
