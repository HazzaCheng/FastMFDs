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
  * Date: 2017-10-07
  * Time: 10:00 AM
  */
@RunWith(classOf[JUnitRunner])
class FDUtilsSpeedTest extends FunSuite {


  test("map2list") {
    val nums = 15
    val dependencies = FDUtils.getDependencies(nums)

    for (i <- 1 to nums) {
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhs = candidates.keySet.toList.groupBy(_.size)
      val keys = lhs.keys.toList.sortWith((x, y) => x > y)

      for (k <- keys) {
        val l = lhs.get(k).get
        println("size: " + k + " " + l)
      }

      println(i + " --- candidates: " + candidates.size + ", lhs: " + lhs.size)
    }
  }

  test("loopMap") {
    val nums = 15
    val dependencies = FDUtils.getDependencies(nums)

    for (i <- 1 to nums) {
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhs = candidates.keySet.toList.sortWith((x, y) => x.size > y.size)
      for (l <- lhs) {
        for (d <- candidates.get(l)) {
          d
        }
      }
    }

  }

}