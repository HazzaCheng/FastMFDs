package com.hazzacheng.FD
import DependencyDiscovery.checkDependency
import scala.io.Source

import org.scalatest.FunSuite

/**
  * Created with IntelliJ IDEA.
  *
  * Description:
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-10-06
  * Time: 10:12 AM
  */
class DependencyDiscoveryTest extends FunSuite {

  test("cut leaves") {
    val nums = 4
    val dependencies = FDUtils.getDependencies(nums)
    val candidates = FDUtils.getCandidateDependencies(dependencies, 1)
    val failed = List((Set(1, 3), 4), (Set(1, 3, 4), 2))

    var size1 = 0
    var size2 = 0
    dependencies.foreach(x => size1 += x._2.size)
    candidates.foreach(x => size2 += x._2.size)

//    dependencies.foreach(println)
//    println()
//    candidates.foreach(println)
//    println()

    DependencyDiscovery.cutLeaves(dependencies, candidates, failed, 1)

    var size3 = 0
    var size4 = 0
    dependencies.foreach(x => size3 += x._2.size)
    candidates.foreach(x => size4 += x._2.size)

//    dependencies.foreach(println)
//    println()
//    candidates.foreach(println)
//    println()

    assert(size1 === size3 + 4)
    assert(size2 === size4 + 6)
  }

//  test("findMinFD") {
//    val d = mutable.HashMap((Set(1,2,4), mutable.Set(7,8)),
//      (Set(1,2,3), mutable.Set(8)),
//      (Set(1,2), mutable.Set(7)),
//      (Set(1,2,5), mutable.Set(6,9)),
//      (Set(2,9), mutable.Set(3,6,1)),
//      (Set(5), mutable.Set(9)))
//
//    val minFD = findMinFD(d,to)
//
////    for(k <- minFD) {
////      k._1.foreach(x => print(x + " "))
////      print("--> ")
////      k._2.foreach(x => print(x + " "))
////      print("\n")
////    }
//
//    var size = 0
//    minFD.foreach(x => size += x._2.size)
//
//    assert(size === 8)
//
//  }


  test("check"){
    val file = Source.fromFile("test")
    val data = file.getLines().map(line => line.split(",").map(word => word.trim())).toList
    data.foreach(d => {
      d(0) = "allldsad"
      d(5) = "muks"
    })
    //print(checkDependency(data, (Set(1,7),List(6,8,9))))

  }


}
