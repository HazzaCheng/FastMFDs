package com.hazzacheng.FD

import org.apache.spark.SparkContext

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-09-28
  * Time: 8:34 PM
  */
object Utils {

  def readAsRdd(sc: SparkContext, filePath: String) = {
    sc.textFile(filePath, sc.defaultParallelism * 4)
      .map(line => line.split(",").map(word => word.trim()))
  }
}
