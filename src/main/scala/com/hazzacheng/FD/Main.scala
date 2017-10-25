package com.hazzacheng.FD

import org.apache.spark.sql.SparkSession

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-09-26
  * Time: 9:44 PM
  */
object Main {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().getOrCreate()

    val input = args(0)
    val output = args(1)
    val spiltLen = args(2).toInt
    val rdd = FDUtils.readAsRdd(ss.sparkContext, input)
    val res = DependencyDiscovery.findOnSpark(ss, rdd, rdd.first().length, spiltLen)
    val fdMin = FDUtils.outPutFormat(res)
    ss.sparkContext.parallelize(fdMin).saveAsTextFile(output)
  }


}
