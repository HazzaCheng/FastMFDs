package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import Utils.getSubsets

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
class DependencyDiscovery {


}

object DependencyDiscovery {
  private val parallelScaleFactor = 4

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val nums = rdd.first().length
    val dependencies = Utils.getDependencies(nums)
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (i <- 1 to nums) {
      val candidates = Utils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val partitions = repart(sc, rdd, i)

      for (k <- keys) {
        val candidatesBV = sc.broadcast(candidates)
        val ls = lhsAll.get(k).get
        val failed = sc.parallelize(ls).flatMap(lhs => checkDependencies(partitions, candidatesBV, lhs)).collect()
        cutLeaves(dependencies, candidates, failed, i)
        results ++= candidates
      }

    }

    results
  }

  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                failed: Array[(Set[Int], Int)], commonAttr: Int) = {
    for (d <- failed) {
      val subSets = Utils.getSubsets(d._1.toArray)
      for (subSet <- subSets) {
        if (subSet contains commonAttr) cut(candidates, subSet, d._2)
        else cut(dependencies, subSet, d._2)
      }
    }

  }

  def FindMinFD(fd :mutable.HashMap[Set[Int], Set[Int]]) = {
    val minfd = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    val keys = fd.keySet.toList.sortWith((x,y) => x.size < y.size)
    for(k1 <- keys){
      for(k2 <- keys){
        if(IsSubset(k1,k2) && (fd(k1) & fd(k2)).nonEmpty){
           fd(k2) --= fd(k1) & fd(k2)
          if(fd(k2).isEmpty){
            fd -= (k2, fd(k2))
          }
        }
      }
    }
    fd
  }

  def IsSubset(x:Set[Int], y:Set[Int]):Boolean = {
    if(x.size >= y.size) false
    else{
      if(x ++ y == y)true
      else false
    }
  }

  def cut(map: mutable.HashMap[Set[Int], mutable.Set[Int]],
                      lhs: Set[Int], rhs: Int) = {
    val v = map.get(lhs).get
    if (v contains rhs) {
      if (v.size == 1) map -= lhs
      else {
        v -= rhs
        map.update(lhs, v)
      }
    }
  }


  def checkDependencies(partitions: RDD[List[Array[String]]],
                        candidatesBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
                        lhs: Set[Int]): Array[(Set[Int], Int)] = {
    val existed = candidatesBV.value.get(lhs)
    if (existed != None) {
      val rs = existed.get.toList
      val failed = partitions.flatMap(p => check(p, lhs.toList, rs))
        .distinct().map(rhs => (lhs, rhs))
      failed.collect()
    } else Array()
  }

  private def takeAttributes(arr: Array[String], attributes: List[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr - 1)))

    s.toString()
  }

  def check(data: List[Array[String]], lhs: List[Int], rhs: List[Int]): List[Int] ={
    val lSize = data.map(d => (takeAttributes(d, lhs),d)).groupBy(_._1).values.size
    val res = rhs.map(y => {
      val rSize = data.map(d => (takeAttributes(d, lhs :+ y),d)).groupBy(_._1).values.size
      if(lSize != rSize) y
      else 0
    }).filter(_ > 0)

    res
  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(t => t._2).repartition(sc.defaultParallelism * parallelScaleFactor)

    partitions
  }

}
