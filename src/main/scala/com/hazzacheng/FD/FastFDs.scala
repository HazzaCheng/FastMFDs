package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-11-7 
  * Time: 9:33 PM
  */
object FastFDs {
  def genDiffSets(sc: SparkContext, rdd: RDD[Array[String]],
                  colSize: Int, orders: Array[(Int, Long)]): mutable.HashSet[Set[Int]] = {
    val sets = mutable.HashSet.empty[Set[Int]]
    for (i <- orders) {
      val partitionsRDD = repart(sc, rdd, i._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val s = partitionsRDD.flatMap(p => getAgreeSets(p, i._1, colSize)).collect()
      sets ++= s
      partitionsRDD.unpersist()
    }

    sets
  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(_._2)

    partitions
  }

  def getAgreeSets(p: List[Array[String]], commonAttr: Int, colSize: Int) = {
    val len = p.length
    val sets = mutable.HashSet.empty[Set[Int]]
    val cols = Range(0, colSize).filter(_ != commonAttr).toArray
    for (i <- 0 until len)
      for (j <- i + 1 until len)
        sets.add(checkTwoLine(p(i), p(j), cols) + commonAttr)
    sets
  }

  def checkTwoLine(rowI: Array[String], rowJ: Array[String], cols: Array[Int]): Set[Int] = {
    cols.filter(i => rowI(i) equals rowJ(i)).toSet
  }


}
