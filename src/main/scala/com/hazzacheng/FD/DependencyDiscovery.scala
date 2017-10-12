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
  var time1 = System.currentTimeMillis()

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]]): Map[Set[Int], mutable.Set[Int]] = {
    val nums = rdd.first().length
    val dependencies = FDUtils.getDependencies(nums)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (i <- 1 to nums) {
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val partitions = repart(sc, rdd, i).cache()
      println("===========Partitions Size ============= Total" + partitions.count())
      partitions.map(p => p.length).collect().foreach(println)
      time1 = System.currentTimeMillis()
      if (partitions.count() == 1) emptyFD += i
      println("===========Partitions count Use Time=============" + (System.currentTimeMillis() - time1))


      for (k <- keys) {
        val candidatesBV = sc.broadcast(candidates)
        val ls = lhsAll.get(k).get
        val lsBV = sc.broadcast(ls)
        val failedTemp = partitions.flatMap(p => checkDependencies(p, candidatesBV, lsBV)).collect()
        time1 = System.currentTimeMillis()
        val failed = failedTemp.distinct
        println("===========Distinct" + k + " Use Time=============" + (System.currentTimeMillis() - time1))
        //        val failed = sc.parallelize(ls).flatMap(lhs => checkDependencies(partitions, candidatesBV, lhs)).collect()
        time1 = System.currentTimeMillis()
        cutLeaves(dependencies, candidates, failed, i)
        println("===========Cut Leaves" + k + " Use Time=============" + (System.currentTimeMillis() - time1))
      }
      partitions.unpersist()
      results ++= candidates
    }

    time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(sc, results)
    println("===========FindMinFD Use Time=============" + (System.currentTimeMillis() - time1))
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
    println("===========My Size=============" + p.length)
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

  def findMinFD(sc:SparkContext,
                fd:mutable.HashMap[Set[Int], mutable.Set[Int]]):  Map[Set[Int], mutable.Set[Int]] = {
    val fdList = fd.toList
    val data = fdList.groupBy(_._1.size).map(f => (f._1, f._2.toMap))
    val index = data.keys.toList.sortWith((x, y) => x < y)
    val dataBV = sc.broadcast(data)
    val indexBV = sc.broadcast(index)
    val rdd = sc.parallelize(fdList.map(f => (f._1.size, f)), sc.defaultParallelism * parallelScaleFactor)
    val minFD = rdd.map(f => getMinFD(dataBV, f, indexBV)).filter(_._2.size > 0).collect()

    minFD.toMap
  }

  def getMinFD(dataBV: Broadcast[Map[Int, Map[Set[Int], mutable.Set[Int]]]],
               f:(Int, (Set[Int], mutable.Set[Int])), index:Broadcast[List[Int]]): (Set[Int], mutable.Set[Int]) = {
    for(i <- index.value){
      if(i >= f._1) return f._2
      for(fd <- dataBV.value(i))
        if(FDUtils.isSubset(fd._1, f._2._1)) f._2._2 --= fd._2
    }
    f._2
  }

}
