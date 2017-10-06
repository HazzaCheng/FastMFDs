package com.hazzacheng.FD

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

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
class UtilsTest extends FunSuite {

  def arr2str(subSets: Array[String]): String = {

    val sortedSubSets = subSets.sorted
    sortedSubSets.reduce(_ + "\n" + _ )
  }


  test("getSubSets") {
    val nums = Array(1, 2, 4)
    val expected = Array(Set(1, 2, 4), Set(1, 2), Set(1, 4), Set(1),
      Set(2), Set(2, 4), Set(4)).map(subSet => subSet.toString())
    val res = Utils.getSubsets(nums).map(subSet => subSet.toString()).toArray
//    println(arr2str(expected))
//    println()
//    println(arr2str(res))
    assert(arr2str(expected) === arr2str(res))
  }

  test("getDependencies") {
    val num = 15
    val res = Utils.getDependencies(num)

    var size = 0
    res.foreach(size += _._2.size)
//    res.foreach(println)
//    println("Total hava " + size)
    assert(size === num * (1 << num - 1) - num)
  }
}
