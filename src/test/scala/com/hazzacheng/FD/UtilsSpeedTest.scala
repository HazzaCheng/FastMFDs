package com.hazzacheng.FD

import com.hazzacheng.FD.DependencyDiscovery.repart
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
  * Date: 2017-10-07
  * Time: 10:00 AM
  */
@RunWith(classOf[JUnitRunner])
class UtilsSpeedTest extends FunSuite {


  test("map2list") {
    val nums = 15
    val dependencies = Utils.getDependencies(nums)

    for (i <- 1 to nums) {
      val candidates = Utils.getCandidateDependencies(dependencies, i)
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
    val dependencies = Utils.getDependencies(nums)

    for (i <- 1 to nums) {
      val candidates = Utils.getCandidateDependencies(dependencies, i)
      val lhs = candidates.keySet.toList.sortWith((x, y) => x.size > y.size)
      for (l <- lhs) {
        for (d <- candidates.get(l)) {
          d
        }
      }
    }

  }
}
