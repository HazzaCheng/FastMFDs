package com.hazzacheng.FD

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-10-06
  * Time: 10:12 AM
  */

@RunWith(classOf[JUnitRunner])
class FDUtilsTest extends FunSuite {

  def arr2str(subSets: Array[String]): String = {

    val sortedSubSets = subSets.sorted
    sortedSubSets.reduce(_ + "\n" + _ )
  }


  test("getSubSets") {
    val nums = Array(1, 2, 4)
    val expected = Array(Set(1, 2, 4), Set(1, 2), Set(1, 4), Set(1),
      Set(2), Set(2, 4), Set(4)).map(subSet => subSet.toString())
    val res = FDUtils.getSubsets(nums).map(subSet => subSet.toString()).toArray
//    println(arr2str(expected))
//    println()
//    println(arr2str(res))
    assert(arr2str(expected) === arr2str(res))
  }

  test("getDependencies") {
    val num = 15
    val res = FDUtils.getDependencies(num)

    var size = 0
    res.foreach(size += _._2.size)
//    res.foreach(println)
//    println("Total hava " + size)
    assert(size === num * (1 << num - 1) - num)
  }

  test("getCandidateDependencies") {
    val num = 15
    val dependencies = FDUtils.getDependencies(num)

    val candidates = FDUtils.getCandidateDependencies(dependencies, 1)
    var size1 = 0
    var size2 = 0
    dependencies.foreach(size1 += _._2.size)
    candidates.foreach(size2 += _._2.size)
//    candidates.foreach(println)
//    println()
//    dependencies.foreach(println)
//    println("contains 1: " + size1 + " , " + "without 1: " + size2)
//    println()
    assert(size2 === (num - 1) * (1 << num - 2))
    assert(size1 === num * (1 << num - 1) - num - size2)

    val candidates2 = FDUtils.getCandidateDependencies(dependencies, 2)
    var size3 = 0
    var size4 = 0
    dependencies.foreach(size3 += _._2.size)
    candidates2.foreach(size4 += _._2.size)
//    candidates2.foreach(println)
//    println()
//    dependencies.foreach(println)
//    println("contains 2: " + size3 + " , " + "without 1,2: " + size4)
    assert(size4 === (num - 2) * (1 << num - 3) + (1 << num - 2))
    assert(size3 === num * (1 << num - 1) - num - size2 - size4)
  }

  test("check"){
    val l = List(Array("a","d","k","u"),Array("a","d","e","u"),Array("a","l","e","b"),
      Array("a","l","e","b"),Array("a","r","m","q"))

    assert(FDUtils.check(l,List(1),List(4,3)).toArray.apply(0) === 4)
    assert(FDUtils.check(l,List(1),List(4,3)).toArray.apply(1) === 3)
  }

  test("output") {
    val fdMin = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    fdMin += (Set.empty[Int] -> mutable.Set[Int](1, 2, 3))
    fdMin += (Set(1, 2) -> mutable.Set[Int](4, 5))

    val str = FDUtils.outPutFormat(fdMin.toMap)
//    str.foreach(println)
    assert(str.apply(0) === "[column1,column2]:column4,column5")
    assert(str.apply(1) === "[]:column1,column2,column3")
  }
}
