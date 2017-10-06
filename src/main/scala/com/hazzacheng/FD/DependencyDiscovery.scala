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
  }

  def check(rdd: RDD[Array[String]], attributes: List[Int]): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (takeAttributes(line, attributes), List(line)))
      .reduceByKey(_ ++  _).map(t => t._2)

    partitions
  }

  private def takeAttributes(arr: Array[String], attributes: List[Int]) = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr)))

    s
  }

}
