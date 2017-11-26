package com.hazzacheng.FD

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable
import com.hazzacheng.FD.utils.RddCheckUtils.checkFD

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
  test("check"){
    val p = List(Array("ab", "ac", "df", "po", "mk"),
      Array("ab", "ac", "kp", "ql", "mk"),
      Array("af", "qe", "ty", "lp", "ks"),
      Array("af", "qe", "fv", "aa", "ks"))
    val map = mutable.HashMap.empty[Set[Int], Int]
    map += Set(1,2,5) -> 2

    val res = checkFD(p, map, Set(1,2), mutable.Set(3,4,5), 5)
    println(res._1.toString())
    println(res._2.toString())
  }

}
