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
    val ss = new SparkSession()
    val sc = ss.sparkContext
    val input = args(0)
    val output = args(1)
//    val spiltLen = args(2).toInt
    val (colSize, orders) = FDUtils.getColSizeAndOreders(ss, input)
    val rdd = FDUtils.readAsRdd(sc, input)
    val res = DependencyDiscovery.findOnSpark(sc, rdd, colSize, orders, 1000)
    val fdMin = FDUtils.outPutFormat(res)
    sc.parallelize(fdMin).saveAsTextFile(output)


  }


}
