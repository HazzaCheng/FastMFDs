package com.hazzacheng.FD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
  * Created by Administrator on 2017/11/7.
  */
object DepMiner {

  def produceAgrS(sc: SparkContext, rdd:RDD[Array[String]]):mutable.HashMap[(Long,Long), mutable.Set[Int]] = {
    val num = rdd.first().length
    val data = rdd.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
    val dict = mutable.HashMap.empty[(Long,Long), mutable.Set[Int]]
    //val arr = mutable.ListBuffer.empty[RDD[((Long,Long),Int)]]
    val arr = mutable.ListBuffer.empty[((Long,Long),Int)]
    for(i <- 1 to num){
      val proRDD = data.map(x => (x._1(i - 1), ListBuffer(x._2))).reduceByKey((x, y) => x ++ y).filter(x => x._2.size > 1).values
      val p = proRDD.map(d => makePair(d.toList, i)).flatMap(x => x).collect()
      arr ++= p.toList
    }
    val rowRDD = sc.parallelize(arr, sc.defaultParallelism * 4)//.repartition(sc.defaultParallelism * 4)
    val needarr = rowRDD.map(x => (x._1, mutable.Set(x._2))).reduceByKey((x,y) => x ++ y).collect()
    needarr.foreach(p => {
      dict += p
    })
    dict
  }

  def makePair(data: List[Long], attr:Int): List[((Long,Long),Int)] = {
    val res = mutable.ListBuffer.empty[((Long,Long), Int)]
    val len = data.size
    for(i <- 0 until (len - 1)){
      for(j <- (i + 1) until len){
        res += (data(i), data(j)) -> attr
      }
    }
    res.toList
  }


}
