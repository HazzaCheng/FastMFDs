package com.hazzacheng.FD

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

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
    val df = utils.DataFrameCheckUtils.getDataFrameFromCSV(ss, input)
    val colSize = utils.DataFrameCheckUtils.getColSize(df)
    val fds = FDsMine_test.findOnSpark(sc, df, colSize, input)
    val res = FDsUtils.outPutFormat(fds)

    sc.parallelize(res).saveAsTextFile(output)
  }


}
