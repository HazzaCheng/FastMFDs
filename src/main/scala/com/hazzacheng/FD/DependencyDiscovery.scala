package com.hazzacheng.FD

import org.apache.spark.SparkContext
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
      val lhs = candidates.keySet.toList.sortWith((x, y) => x.size > y.size)
      val parations = repart(rdd, i)

    }

    results
  }

  private def takeAttributes(arr: Array[String], attributes: List[Int]) = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr - 1)))

    s.toString()
  }

  def check(data: RDD[Array[String]], attribute_x: List[Int], attribute_y: Int): Boolean = {
    val partitions_x = data.map(line => (takeAttributes(line, attribute_x), List(line))).reduceByKey((A, B) => A++B).values.count()
    val partitions_y = data.map(line => (takeAttributes(line, attribute_x :+ attribute_y), List(line))).reduceByKey((A, B) => A++B).values.count()
    val result = partitions_x == partitions_y
    result
  }

  def repart(rdd: RDD[Array[String]], attribute: Int) = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(t => t._2).repartition(parallelScaleFactor * 30)

    partitions
  }

}
