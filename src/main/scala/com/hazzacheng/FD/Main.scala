package com.hazzacheng.FD

import com.hazzacheng.FD.utils.RddUtils
import org.apache.spark.SparkConf
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
    val conf = new SparkConf().set("spark.rdd.compress", "true")
      .set("spark.speculation", "true")

    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext
    val input = args(0)
    val output = args(1)
    val df = utils.DataFrameUtils.getDataFrameFromCSV(ss, input)
    val colSize = utils.DataFrameUtils.getColSize(df)
    val fds = MinimalFDsMine.findOnSpark(sc, df, colSize, input)
    val res = RddUtils.outPutFormat(fds)

    sc.parallelize(res).saveAsTextFile(output)
  }


}
