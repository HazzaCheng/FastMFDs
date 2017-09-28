package com.hazzacheng.FD

import org.apache.spark.SparkContext

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
    val sc = new SparkContext()

    val input = args(0)
    val output = args(1)
    val rdd = Utils.readAsRDD(sc, input)
  }


}
