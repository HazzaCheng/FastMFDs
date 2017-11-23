package com.hazzacheng.FD

import com.hazzacheng.FD.FDsMine._
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
  * Date: 17-11-20 
  * Time: 10:13 AM
  */

@RunWith(classOf[JUnitRunner])
class FDsMineTest extends FunSuite {

  def arr2str(subSets: Array[String]): String = {

    val sortedSubSets = subSets.sorted
    sortedSubSets.reduce(_ + "\n" + _ )
  }

  test("getEqualAttr") {
    val fds = Array((1, 2), (3, 5), (2, 1), (3, 8), (5, 3), (1, 10), (2, 6), (8, 3))
    val (equalAttrs, newFDs) = getEqualAttr(fds)
    val expected1 = List(Set(1, 2), Set(3, 5, 8))
    val expected2 = List((1, 10), (2, 6))

    assert(equalAttrs.toSet == expected1.toSet)
    assert(newFDs.toSet == expected2.toSet)
  }

  test("createNewOrders") {
    val equalAttr = List(Set(1, 2), Set(3, 4, 5))
    val singleLhsCount = List((1, 100), (2, 50), (3, 66), (4, 95), (5, 72))
    val colSize = 5
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(equalAttr, singleLhsCount, colSize)

    assert(equalAttrMap == Map(1 -> List(2), 4 -> List(3, 5)))
    assert(ordersMap == Map(1 -> 1, 2 -> 4))
    assert(orders.toSet == Set((1, 100), (2, 95)))
    assert(Set(2, 3, 5) == del.toSet)
  }

  test("getNewBottomFDs"){
    val withoutEqualAttr = Array((1, 3), (2, 3), (1, 4), (2, 4), (6, 7))
    val ordersMap = Map(1 -> 1, 2 -> 4, 6 -> 6, 7 -> 7)
    val equalAttrMap = Map(1 -> List(2), 4 -> List(3, 5))
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)

    assert(bottomFDs.toSet == Set((Set(1), 2), (Set(6), 7)))
  }

  test("getLongestLhs"){
    val newColSize = 3
    val topCandidates = getLongestLhs(newColSize)

    assert(topCandidates.toList == List((Set(1,2),3),(Set(1,3),2),(Set(2,3),1)))
  }

  test("cutInTopLevel"){
    val newColSize = 3
    val topCandidates = getLongestLhs(newColSize)
    val bottomFDs = Array((Set(1), 3))
    cutInTopLevel(topCandidates, bottomFDs)

    assert(topCandidates.toList == List((Set(1,3),2), (Set(2,3),1)))
  }


  test("getSubSets") {
    val nums = Array(1, 2, 4)
    val expected = Array(Set(1, 2, 4), Set(1, 2), Set(1, 4), Set(1),
      Set(2), Set(2, 4), Set(4)).map(subSet => subSet.toString())
    val res = getSubsets(nums).map(subSet => subSet.toString()).toArray

    assert(arr2str(expected) == arr2str(res))
  }


  test("getCandidates") {
    val colSize = 10
    val candidates = getCandidates(10)

    var size = 0
    candidates.foreach(size += _._2.size)
    //    println("Total hava " + size)

    assert(size == colSize * (1 << colSize - 1) - colSize)
  }

  test("removeTopAndBottom"){
    val newColSize = 4
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    val expected = List(
      (Set(1, 3), Set(2, 4)),
      (Set(1, 2), Set(3, 4)),
      (Set(1, 4), Set(2, 3)),
      (Set(2, 3), Set(1, 4)),
      (Set(2, 4), Set(1, 3)),
      (Set(3, 4), Set(1, 2))
    )

    assert(candidates.toSet == expected.toSet)
  }

  test("isSubset") {
    val s1 = Set(1)
    val b1 = Set(1, 3)
    assert(isSubSet(b1, s1) == true)

    val s2 = Set(1, 3, 4)
    val b2 = Set(1, 2, 3, 4, 5)
    assert(isSubSet(b2, s2) == true)

    val s3 = Set(1, 3, 4)
    val b3 = Set(1, 2, 4, 5)
    assert(isSubSet(b3, s3) == false)
  }

  test("cutInCandidates") {
    val newColSize = 4
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    val lhs = List(Set(1, 3), Set(3, 4))
    val rhs = 2

    cutInCandidates(candidates, lhs, rhs)
    val expected = List(
      (Set(1, 3), Set(4)),
      (Set(1, 2), Set(3, 4)),
      (Set(1, 4), Set(2, 3)),
      (Set(2, 3), Set(1, 4)),
      (Set(2, 4), Set(1, 3)),
      (Set(3, 4), Set(1))
    )

    assert(candidates.toSet == expected.toSet)
  }

  test("cutFromDownToTop"){
    val newColSize = 4
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    val bottomFDs = Array((Set(1), 3))

    cutFromDownToTop(candidates, bottomFDs)
    val expected = List(
      (Set(1, 3), Set(2, 4)),
      (Set(1, 2), Set(4)),
      (Set(1, 4), Set(2)),
      (Set(2, 3), Set(1, 4)),
      (Set(2, 4), Set(1, 3)),
      (Set(3, 4), Set(1, 2))
    )

    assert(candidates.toSet == expected.toSet)
  }

  test("cutFromTopToDown"){
    val newColSize = 4
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    val topFDs = mutable.Set((Set(1, 2, 3), 4))

    cutFromTopToDown(candidates, topFDs)
    val expected = List(
      (Set(1, 3), Set(2)),
      (Set(1, 2), Set(3)),
      (Set(1, 4), Set(2, 3)),
      (Set(2, 3), Set(1)),
      (Set(2, 4), Set(1, 3)),
      (Set(3, 4), Set(1, 2))
    )

    assert(candidates.toSet == expected.toSet)
  }

  test("getTargetCandidates"){
    var candidates = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    candidates += Set(1,2) -> mutable.Set(3,4)
    candidates += Set(1,3) -> mutable.Set(2,4)
    candidates += Set(1,4) -> mutable.Set(2,3)
    candidates += Set(1) -> mutable.Set(2,3,4)
    candidates += Set(2) -> mutable.Set(1,3,4)
    candidates += Set(1,2,3) -> mutable.Set(4)
    val common = 1
    val level = 2
    val expected = List(
      (Set(1, 3), Set(2, 4)),
      (Set(1, 2), Set(3, 4)),
      (Set(1, 4), Set(2, 3))
    )

    val toChecked = getTargetCandidates(candidates, common, level).toList
    assert(toChecked == expected)
  }

  test("recoverAllFds"){
    val results = List((Set(1, 2, 3), 4))
    val equalAttrMap = Map(1 -> List(2), 3 -> List(4, 8))
    val ordersMap = Map(1 -> 1, 2 -> 3, 3 -> 5, 4 -> 6)
    val fds = recoverAllFDs(results, equalAttrMap, ordersMap)

    fds.foreach(println)

    val expected = Map(
      Set(1, 3, 5) -> List(6),
      Set(1, 4, 5) -> List(6),
      Set(1, 8, 5) -> List(6),
      Set(2, 3, 5) -> List(6),
      Set(2, 4, 5) -> List(6),
      Set(2, 8, 5) -> List(6),
      Set(1) -> List(2),
      Set(2) -> List(1),
      Set(3) -> List(4, 8),
      Set(4) -> List(3, 8),
      Set(8) -> List(3, 4)
    )

    assert(expected.toSet == fds.toSet)
  }
}
