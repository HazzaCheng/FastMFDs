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
    val conf = new SparkConf()
      .set("spark.default.parallelism", "250")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.speculation", "true")

    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext
    val numPartitions = sc.defaultParallelism

    val input = args(0)
    val output = args(1)
    val temp = args(2)

    val (df, colSize, tempFilePath) = utils.DataFrameUtils.getDataFrameFromCSV(ss, numPartitions, input, temp)
    val fds = new MinimalFDsMine(numPartitions, ss, sc, df, colSize, temp).run()
    val res = RddUtils.outPutFormat(fds)

    sc.parallelize(res).saveAsTextFile(output)
  }

}
