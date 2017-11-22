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
    val res = FDsUtils.getSubsets(nums).map(subSet => subSet.toString()).toArray
//    println(arr2str(expected))
//    println()
//    println(arr2str(res))
    assert(arr2str(expected) == arr2str(res))
  }

  test("getDependencies") {
    val num = 15
    val res = FDsUtils.getDependencies(num)

    var size = 0
    res.foreach(size += _._2.size)
//    res.foreach(println)
//    println("Total hava " + size)
    assert(size == num * (1 << num - 1) - num)
  }

  test("getCandidateDependencies") {
    val num = 15
    val dependencies = FDsUtils.getDependencies(num)
    var sum = 0
    dependencies.toList.foreach(fd => sum += fd._2.size)
    println(sum)
    val candidates = FDsUtils.getCandidateDependencies(dependencies, 1)
    var size1 = 0
    var size2 = 0
    dependencies.foreach(size1 += _._2.size)
    candidates.foreach(size2 += _._2.size)
//    candidates.foreach(println)
//    println()
//    dependencies.foreach(println)
//    println("contains 1: " + size1 + " , " + "without 1: " + size2)
//    println()
    assert(size2 == (num - 1) * (1 << num - 2))
    assert(size1 == num * (1 << num - 1) - num - size2)

    val candidates2 = FDsUtils.getCandidateDependencies(dependencies, 2)
    var size3 = 0
    var size4 = 0
    dependencies.foreach(size3 += _._2.size)
    candidates2.foreach(size4 += _._2.size)
//    candidates2.foreach(println)
//    println()
//    dependencies.foreach(println)
//    println("contains 2: " + size3 + " , " + "without 1,2: " + size4)
    assert(size4 == (num - 2) * (1 << num - 3) + (1 << num - 2))
    assert(size3 == num * (1 << num - 1) - num - size2 - size4)
  }




}
