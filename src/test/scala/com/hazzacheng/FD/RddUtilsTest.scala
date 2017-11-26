package com.hazzacheng.FD
import org.scalatest.FunSuite
import com.hazzacheng.FD.utils.RddUtils._
import scala.collection.mutable
/**
  * Created by Administrator on 2017/11/26.
  */
class RddUtilsTest extends FunSuite{
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
