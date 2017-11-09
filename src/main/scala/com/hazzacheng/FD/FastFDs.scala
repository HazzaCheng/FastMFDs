package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
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
  var time1 = 0L

  def genDiffSets(sc: SparkContext,
                  rdd: RDD[(Array[String], Long)],
                  colSize: Int): Set[Set[Int]] = {
    genAgreeSets(sc, rdd, colSize)
  }

  private def genAgreeSets(sc: SparkContext,
                           rdd: RDD[(Array[String], Long)],
                           colSize: Int): Set[Set[Int]] = {
    val sets = mutable.HashSet.empty[Set[Int]]
    val stripped = new Array[Array[Set[Int]]](colSize)
    for (i <- 1 to colSize) {
      val temp = getStripPartitions(sc, rdd, i).collect()
      stripped(i - 1) = temp
      sets ++= temp
    }
    val mc = getMC(sc, sets)
    time1 = System.currentTimeMillis()
    val rows = getRows(mc)
    println("====USE TIME get rows: " + (System.currentTimeMillis() - time1))
    val ecMap = getAllEc(sc, stripped, rows)
    val couplesRdd = getAllCouples(sc, mc)
    val ag = getAg(sc, couplesRdd, ecMap)
    println("====Size ag sets: " + ag.size)

    ag
  }

  private def getStripPartitions(sc: SparkContext,
                                 rdd: RDD[(Array[String], Long)],
                                 attribute: Int): RDD[Set[Int]] = {
    val partitions = rdd.map(line => (line._1(attribute - 1), List(line._2.toInt)))
      .reduceByKey(_ ++ _).map(_._2.toSet)

    partitions
  }

  private def getMC(sc: SparkContext,
                    sets: mutable.HashSet[Set[Int]]): Array[Set[Int]] = {
    time1 = System.currentTimeMillis()
    val temp = sets.toArray.map(x => (x.size, x)).sortWith((x, y) => x._1 > y._1)
    println("====USE TIME sort: " + (System.currentTimeMillis() - time1))
    val tempBV = sc.broadcast(temp)
    val mc = sc.parallelize(temp).filter(isBigSet(tempBV, _)).collect()
    println("====Size mc" + mc.length)
    mc.map(x => x._2)
  }

  private def isBigSet(tempBV: Broadcast[Array[(Int,Set[Int])]],
                       s: (Int,Set[Int])): Boolean = {
    val arr = tempBV.value
    for (set <- arr) {
      if(s._1 <= set._1) {
        if ((s._2 & set._2) == s && s != set) return false
      }
    }
    true
  }

  private def getAllCouples(sc: SparkContext,
                            mc: Array[Set[Int]]): RDD[(Int, Int)] = {
    val couples = sc.parallelize(mc).flatMap(set => getCouples(set))
      .distinct().persist(StorageLevel.MEMORY_AND_DISK_SER)

    couples
  }

  private def getCouples(set: Set[Int]): List[(Int, Int)] = {
    val arr = set.toArray.sorted
    val len = arr.length
    val list = mutable.ListBuffer.empty[(Int, Int)]
    for (i <- 0 until len)
      for (j <- i + 1 until len)
        list.append((arr(i), arr(j)))

    list.toList
  }

  private def getRows(mc: Array[Set[Int]]): Array[Int] = {
    val rows = mc.flatMap(_.toList)

    rows.distinct
  }

  private def getAllEc(sc: SparkContext,
                       stripped: Array[Array[Set[Int]]],
                       rows: Array[Int]): mutable.HashMap[Int, Set[(Int, Int)]] = {
    val strippedBV = sc.broadcast(stripped)
    val ec = sc.parallelize(rows).map(r => getEc(strippedBV, r)).collect()
    val ecMap = mutable.HashMap.empty[Int, Set[(Int, Int)]]
    time1 = System.currentTimeMillis()
    ec.foreach(x => ecMap.put(x._1, x._2))
    println("====USE TIME get ec: " + (System.currentTimeMillis() - time1))

    ecMap
  }

  private def getEc(strippedBV: Broadcast[Array[Array[Set[Int]]]],
                    r: Int): (Int, Set[(Int, Int)]) = {
    val stripped = strippedBV.value
    val res = mutable.ListBuffer.empty[(Int, Int)]
    val len1 = stripped.length
    for (i <- 0 until len1) {
      val len2 = stripped(i).length
      var nonFind = true
      var j = 0
      while (nonFind && j < len2) {
        if (stripped(i)(j) contains r) {
          res.append((i + 1, j))
          j += 1
          nonFind = false
        }
      }
    }

    (r, res.toSet)
  }

  private def getAg(sc: SparkContext,
                    couplesRdd: RDD[(Int, Int)],
                    ecMap: mutable.HashMap[Int, Set[(Int, Int)]]): Set[Set[Int]] = {
    val ecMapBV = sc.broadcast(ecMap)
    val ag = couplesRdd.map(couple => getCommon(ecMapBV, couple._1, couple._2)).collect()

    ag.toSet
  }

  private def getCommon(ecMapBV: Broadcast[mutable.HashMap[Int, Set[(Int, Int)]]],
                        i: Int, j: Int): Set[Int] = {
    val ecMap = ecMapBV.value
    val s1 = ecMap(i)
    val s2 = ecMap(j)
    val common = (s1 & s2).map(_._1)

    common
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
