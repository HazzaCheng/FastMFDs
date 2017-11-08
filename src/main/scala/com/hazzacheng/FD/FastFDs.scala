package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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
  var time1 = 0L

  def genDiffSets(sc: SparkContext, rdd: RDD[(Array[String], Long)],
                  colSize: Int): Array[Set[Int]] = {
    genAgreeSets(sc, rdd, colSize)
  }

  private def genAgreeSets(sc: SparkContext, rdd: RDD[(Array[String], Long)],
                  colSize: Int): Array[Set[Int]] = {
    val sets = mutable.HashSet.empty[Set[Int]]
    val striped = new Array[Array[(Set[Int], Int)]](colSize)
    for (i <- 1 to colSize) {
      val temp = getStripPartitions(sc, rdd, i).collect()
      striped(i - 1) = temp.zipWithIndex
      sets ++= temp
    }

    getMC(sc, sets)
  }

  private def getStripPartitions(sc: SparkContext, rdd: RDD[(Array[String], Long)],
             attribute: Int): RDD[Set[Int]] = {
    val partitions = rdd.map(line => (line._1(attribute - 1), List(line._2.toInt)))
      .reduceByKey(_ ++ _).map(_._2.toSet)

    partitions
  }

  private def getMC(sc: SparkContext, sets: mutable.HashSet[Set[Int]]): Array[Set[Int]] = {
    time1 = System.currentTimeMillis()
    val temp = sets.toArray.map(x => (x.size, x)).sortWith((x, y) => x._1 > y._1)
    println("===========USE TIME sort: " + (System.currentTimeMillis() - time1))
    val tempBV = sc.broadcast(temp)
    val mc = sc.parallelize(temp).filter(isBigSet(tempBV, _)).collect()

    mc.map(x => x._2)
  }

  private def isBigSet(tempBV: Broadcast[Array[(Int,Set[Int])]], s: (Int,Set[Int])): Boolean = {
    val arr = tempBV.value
    for (set <- arr) {
      if(s._1 <= set._1) {
        if ((s._2 & set._2) == s && s != set) return false
      }
    }
    true
  }

/*  def getAgreeSets(p: List[Array[String]], commonAttr: Int, colSize: Int) = {
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
  }*/


}
