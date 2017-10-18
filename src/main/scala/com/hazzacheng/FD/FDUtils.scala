package com.hazzacheng.FD

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD

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

  def readAsRdd(sc: SparkContext, filePath: String): RDD[Array[String]] = {
    sc.textFile(filePath, sc.defaultParallelism * 4)
      .map(line => line.split(",").map(word => word.trim()))
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

  def takeAttrLHS(arr: Array[String], attributes: List[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr - 1)))

    s.toString()
  }

//  def takeAttrRHS(arr: Array[String], attributes: List[Int]): Array[String] = {
//    val res = new Array[String](16)
//    attributes.foreach(attr => res(attr) = arr(attr - 1))
//    res
//  }

  def takeAttrRHS(arr: Array[String], attributes: Int): String = {
    arr(attributes - 1)
  }

  def check(d:Array[String], lhs:List[Int], rhs:Int, dict:mutable.HashMap[String, String],isWrong: Accumulator[Int])={
    if(isWrong.value == 0){
      val left = takeAttrLHS(d, lhs)
      val right = takeAttrRHS(d, rhs)
      if(dict.contains(left)){
        if(!dict(left).equals(right)){
          isWrong += 1
        }
      }
      else dict += left -> right
    }
  }

//  def check(data: List[Array[String]], lhs: List[Int], rhs: List[Int]): List[Int] ={
//    //val lSize = data.map(d => (FDUtils.takeAttributes(d, lhs),d)).groupBy(_._1).size
//    val res = mutable.Set.empty[Int]
//    var true_rhs = rhs.toSet
//    val dict = mutable.HashMap.empty[String, Array[String]]
//    data.foreach(d => {
//      val left = takeAttrLHS(d, lhs)
//      val right = takeAttrRHS(d, rhs)
//      if(dict.contains(left)){
//        for(i <- true_rhs){
//          if(!dict(left)(i).equals(right(i))){
//            true_rhs -= i
//            res += i
//          }
//        }
//      }
//      else dict += left -> right
//    })
//
//    res.toList
//  }

  def cut(map: mutable.HashMap[Set[Int], mutable.Set[Int]],
          lhs: Set[Int], rhs: Int) = {
    val ot = map.get(lhs)
    if (ot != None) {
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


}
