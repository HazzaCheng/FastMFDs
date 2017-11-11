package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.control.Breaks

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
    time1 = System.currentTimeMillis()
    val mc = getMC(sets)
    println("====USE TIME get mc: " + (System.currentTimeMillis() - time1))

    time1 = System.currentTimeMillis()
    val rowsLen = mc.map(_.size).sum
    println("====USE TIME get rows: " + (System.currentTimeMillis() - time1))


    time1 = System.currentTimeMillis()
    val ecMap = getAllEc(sc, stripped, rowsLen)
    println("====USE TIME get ec map: " + (System.currentTimeMillis() - time1))

    time1 = System.currentTimeMillis()
    val couples = getAllCouples(sc, mc)
    println("====USE TIME get couples: " + (System.currentTimeMillis() - time1))
    println("====Size couples: " + couples.count())

    /*
    time1 = System.currentTimeMillis()
    val ag = getAg(sc, couplesRdd, ecMap)
    println("====USE TIME get ag: " + (System.currentTimeMillis() - time1))
    println("====Size ag sets: " + ag.size)*/
    val ag: Set[Set[Int]] = Set(Set(1))
    ag
  }

  private def getStripPartitions(sc: SparkContext,
                                 rdd: RDD[(Array[String], Long)],
                                 attribute: Int): RDD[Set[Int]] = {
    val partitions = rdd.map(line => (line._1(attribute - 1), List(line._2.toInt)))
      .reduceByKey(_ ++ _).map(_._2.toSet)

    partitions
  }

  private def getMC(sets: mutable.HashSet[Set[Int]]): Array[Set[Int]] = {
    val temp = sets.toArray.map(x => (x.size, x)).sortWith((x,y) => x._1 > y._1)
    val order = temp.indices
    val res = mutable.ListBuffer.empty[Set[Int]]
    for(i <- order){
      var j = 0
      var f = true
      val loop_ = new Breaks
      loop_.breakable(
        while(j < i){
          val loop = new Breaks
          var flag = true
          loop.breakable(
            for(elem <- temp(i)._2){
              if(!temp(j)._2.contains(elem)){
                flag = false
                loop.break()
              }
            }
          )
          if(flag){
            f = false
            loop_.break()
          }
          j += 1
        }
      )
      if(f){
        res += temp(i)._2
      }
    }
    println("====MC: " + res.toList.length)
    res.toArray
  }

  private def getAllCouples(sc: SparkContext,
                            mc: Array[Set[Int]]): RDD[(Int, Int)] = {
    val couples = sc.parallelize(mc).flatMap(set => getCouples(set))
      .distinct().persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val couples = mc.flatMap(set => getCouples(set))
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
//    val rows = sc.parallelize(mc).flatMap(_.toList).collect()
    rows.distinct
  }

/*  private def getAllEc(sc: SparkContext,
                       stripped: Array[Array[Set[Int]]],
                       rows: Array[Int]): mutable.HashMap[Int, Set[(Int, Int)]] = {
    val strippedBV = sc.broadcast(stripped)
    val ec = sc.parallelize(rows).map(r => getEc(strippedBV, r)).collect()
    val ecMap = mutable.HashMap.empty[Int, Set[(Int, Int)]]
    time1 = System.currentTimeMillis()
    ec.foreach(x => ecMap.put(x._1, x._2))
    println("====USE TIME get ec: " + (System.currentTimeMillis() - time1))
    strippedBV.unpersist()

    ecMap
  }*/

//  private def getAllEc(sc: SparkContext,
//                       stripped: Array[Array[Set[Int]]],
//                       rowsLen: Int): Array[Set[(Int, Int)]] = {
//    val strippedWithIndex = stripped.zipWithIndex
//    val ec = sc.parallelize(strippedWithIndex).map(p => getEc(p, rowsLen)).collect()
//    val ecArr = new Array[mutable.HashSet[(Int, Int)]](rowsLen)
//    Range(0, rowsLen).foreach(i => combineEc(ec, ecArr, i))
//    ecArr.map(_.toSet)
//  }

  private def getEc(partition: (Array[Set[Int]], Int),
                    rows: Int): (Array[Int], Int) = {
    val ec = new Array[Int](rows)
    val len = partition._1.length
    Range(0, len).foreach(i => {
      var j = 0
      var nonFind = true
      while (nonFind && j < len) {
        if (partition._1(j) contains i) {
          ec(i) = j
          nonFind = false
          j += 1
        }
      }
      if (j == len) ec(i) = -1
    })

    (ec, partition._2)
  }

//  def combineEc(ec: Array[(Array[Int], Int)],
//                ecArr: Array[mutable.HashSet[(Int, Int)]],
//                i: Int): Unit = {
//    ec.foreach(x => {
//      val tmp = x._1(i)
//      if (tmp != -1) ecArr(i).add((tmp, x._2))
//    })
//  }

  private def getAllEc(sc: SparkContext,
                       stripped: Array[Array[Set[Int]]],
                       rowsLen: Int): Array[Array[(Int, Int)]] = {
    val strippedNum = stripped.length
    val strippedDict = sc.parallelize(stripped.zipWithIndex, strippedNum).map(x => (x._2, buildStrippedDict(x._1))).collect().sortWith((x, y) => x._1 < y._1).map(x => x._2)
    val strippedDictBV = sc.broadcast(strippedDict)
    val data = stripped(0).flatten
    val res = sc.parallelize(data).map(x => getEc(x, strippedDictBV)).collect()
    res
  }

  def buildStrippedDict(arr: Array[Set[Int]]): mutable.HashMap[Int, Int] = {
    val dict = mutable.HashMap.empty[Int, Int]
    arr.zipWithIndex.foreach(set => set._1.foreach(x => dict += x -> set._2))
    dict
  }

  def getEc(x: Int, dict: Broadcast[Array[mutable.HashMap[Int, Int]]]):Array[(Int, Int)] ={
    val res = mutable.ListBuffer.empty[(Int, Int)]
    val d = dict.value
    for(i <- d.indices){
      res += x -> (d(i)(x) + 1)
    }
    res.toArray
  }
  /*  private def getEc(strippedBV: Broadcast[Array[Array[Set[Int]]]],
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
    }*/

  private def getAg(sc: SparkContext,
                    couplesRdd: RDD[(Int, Int)],
                    ecMap: mutable.HashMap[Int, Set[(Int, Int)]]): Set[Set[Int]] = {
    val ecMapBV = sc.broadcast(ecMap)
    val ag = couplesRdd.map(couple => getCommon(ecMapBV, couple._1, couple._2)).collect()
    ecMapBV.unpersist()

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
