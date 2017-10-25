package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-09-28
  * Time: 8:34 PM
  */
object FDUtils {

  def readAsDF(ss: SparkSession, filePath: String): (Int, DataFrame) = {
    val df = ss.read.csv(filePath)
    val colSize = df.first.length
    (colSize, df.toDF(createNewColumnName(colSize): _*))
  }

  def createNewColumnName(colSize: Int): Array[String] = {
    val names = new Array[String](colSize)
    Range(0, colSize).foreach(i => names(i) = (i + 1).toString)

    names
  }

  def readAsRdd(sc: SparkContext, filePath: String): RDD[Array[String]] = {
    sc.textFile(filePath, sc.defaultParallelism * 4)
      .map(line => line.split(","))
  }

  def outPutFormat(minFD: Map[Set[Int], mutable.Set[Int]]): List[String] = {
    minFD.map(d => d._1.toList.sorted.map(x => "column" + x).mkString("[", ",", "]")
    + ":" + d._2.toList.sorted.map(x => "column" + x).mkString(",")).toList
  }

  def getDependencies(num: Int): mutable.HashMap[Set[Int], mutable.Set[Int]]= {
    val dependencies = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (i <- 1 to num) {
      val nums = Range(1, num + 1).filter(_ != i).toArray
      val subSets = FDUtils.getSubsets(nums)
      for (subSet <- subSets) {
        var value = dependencies.getOrElse(subSet, mutable.Set.empty[Int])
        value += i
        dependencies.update(subSet, value)
      }
    }

    dependencies
  }

  def getSubsets(nums: Array[Int]): List[Set[Int]] = {
    val numsLen = nums.length
    val subsetLen = 1 << numsLen
    var subSets: ListBuffer[Set[Int]] = new ListBuffer()

    for (i <- 0 until subsetLen) {
      val subSet = mutable.Set.empty[Int]
      for (j <- 0 until numsLen) {
        if (((i >> j) & 1) != 0) subSet += nums(j)
      }
      if (subSet.nonEmpty) subSets += subSet.toSet
    }

    subSets.toList
  }

  def getCandidateDependencies(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                               target: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val candidates = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (key <- dependencies.keys) {
      if (key contains target) {
        candidates += (key -> dependencies.get(key).get)
        dependencies -= key
      }
    }

    candidates
  }

  def getLevelFD(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                 ls: List[Set[Int]]): ListBuffer[(Set[Int], Int)] = {
    val fds = ListBuffer.empty[(Set[Int], Int)]
    for (lhs <- ls) {
      val rs = candidates.get(lhs)
      if (rs.isDefined) rs.get.toList.foreach(rhs => fds.append((lhs, rhs)))
    }
    fds
  }

  def getSameLhsFD(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                   ls: List[Set[Int]]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val sameLHS = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (lhs <- ls) {
      val rs = candidates.getOrElse(lhs, null)
      if (rs != null && rs.nonEmpty) sameLHS.update(lhs, rs)
    }
    sameLHS
  }

  def cutInSameLhs(sameLHS: mutable.HashMap[Set[Int], mutable.Set[Int]],
                   failFD: List[(Set[Int], Int)]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    failFD.foreach(fd => cut(sameLHS, fd._1, fd._2))
    sameLHS
  }

  def takeAttrLHS(arr: Array[String], attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr - 1) + " "))

    s.toString()
  }

  def takeAttrRHS(arr: Array[String], attributes: mutable.Set[Int]): Array[String] = {
    val res = new Array[String](16)
    attributes.foreach(attr => res(attr) = arr(attr - 1))
    res
  }

  def check(data: List[Array[String]], lhs: Set[Int], rhs: mutable.Set[Int]): List[(Set[Int],Int)] ={
    //val lSize = data.map(d => (FDUtils.takeAttributes(d, lhs),d)).groupBy(_._1).size
    val res = mutable.Set.empty[Int]
    val true_rhs = rhs.clone()
    val dict = mutable.HashMap.empty[String, Array[String]]
    data.foreach(d => {
      val left = takeAttrLHS(d, lhs)
      val right = takeAttrRHS(d, rhs)
      if(dict.contains(left)){
        for(i <- true_rhs){
          if(!dict(left)(i).equals(right(i))){
            true_rhs -= i
            res += i
          }
        }
      }
      else dict += left -> right
    })

    res.map(rhs => (lhs, rhs)).toList
  }

  def cut(map: mutable.HashMap[Set[Int], mutable.Set[Int]],
          lhs: Set[Int], rhs: Int) = {
    val ot = map.get(lhs)
    if (ot.isDefined) {
      val v = ot.get
      if (v contains rhs) {
        if (v.size == 1) map -= lhs
        else {
          v -= rhs
          map.update(lhs, v)
        }
      }
    }
  }

  def isSubset(x:Set[Int], y:Set[Int]):Boolean = {
    if(x.size >= y.size) false
    else{
      if(x ++ y == y)true
      else false
    }
  }

  def listToRddToDF(ss: SparkSession, partition: List[Array[String]],
                    schema: StructType): DataFrame = {
    val rowRdd = ss.sparkContext.parallelize(partition).map(l => Row.fromSeq(l))
    val df = ss.createDataFrame(rowRdd, schema).persist(StorageLevel.MEMORY_AND_DISK_SER)
    df
  }

  def createStructType(colSize: Int): StructType = {
    val arr = new Array[StructField](colSize)
    Range(0, colSize).foreach(i =>
      arr(i) = StructField((i + 1).toString, StringType, true))
    val struct = StructType(arr)
    struct
  }

}
