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
    val sc = ss.sparkContext
    val input = args(0)
    val output = args(1)
    val (colSize, orders) = FDUtils.getColSizeAndOreders(ss, input)
    val rdd = FDUtils.readAsRdd(sc, input)
  //  val res = DependencyDiscovery.findOnSpark(sc, rdd, colSize, orders)
  //  val fdMin = FDUtils.outPutFormat(res)
  //  sc.parallelize(fdMin).saveAsTextFile(output)

    val sets = FastFDs.genDiffSets(sc, rdd, colSize, orders: Array[(Int, Long)])
    print("=========Size sets " + sets.size)
  }


}
