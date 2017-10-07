package com.hazzacheng.FD

import com.hazzacheng.FD.DependencyDiscovery.FindMinFD
import com.hazzacheng.FD.DependencyDiscovery.check
import org.scalatest.FunSuite

import scala.collection.mutable

/**
  * Created by Administrator on 2017/10/7.
  */
class DependencyDiscoveryTest extends FunSuite {
  test("check"){
    val l = List(Array("a","d","k","u"),Array("a","d","e","u"),Array("a","l","e","b"),
      Array("a","l","e","b"),Array("a","r","m","q"))

    check(l,List(1),List(4,3)).foreach(println(_))

  }

  test("FindMinFD"){
    //val d = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    val d = mutable.HashMap((Set(1,2,4),Set(7,8)), (Set(1,2,3),Set(8)), (Set(1,2),Set(7)),
      (Set(1,2,5),Set(6,9)), (Set(2,9),Set(3,6,1)), (Set(5),Set(9)))
    for(k <- FindMinFD(d)){
      k._1.foreach(print(_))
      print("-->")
      k._2.foreach(print(_))
      print("\n")
    }
  }
}
