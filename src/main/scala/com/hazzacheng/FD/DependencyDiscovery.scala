package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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
      val candidatesBV = sc.broadcast(candidates)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val partitions = repart(sc, rdd, i)

      for (k <- keys) {
        val ls = lhsAll.get(k).get
        sc.parallelize(ls).map(lhs => )
      }

    }

    results
  }

  def checkDependencies(sc: SparkContext, partitions: RDD[List[Array[String]]],
                        candidatesBV: Broadcast[mutable.HashMap[Set[Int], Set[Int]]],
                        lhs: Set[Int]) = {
    val rs = candidatesBV.value.get(lhs).get.toList
    val failed = partitions.flatMap().distinct().map(rhs => (lhs, rhs))

    failed
  }

  private def takeAttributes(arr: Array[String], attributes: List[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr - 1)))

    s.toString()
  }

  def check(data: List[Array[String]], attribute_x: List[Int], attribute_y: List[Int]): List[Int] ={
    val res = attribute_y.map(y => {
      var flag = 0
      val lhs = data.map(d => (takeAttributes(d, attribute_x),d)).groupBy(_._1).values.size
      val rhs = data.map(d => (takeAttributes(d, attribute_x :+ y),d)).groupBy(_._1).values.size
      if(lhs != rhs){
        flag = y
      }
      flag
    }).filter(_ > 0)

    res
  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(t => t._2).repartition(sc.defaultParallelism * parallelScaleFactor)

    partitions
  }

}
