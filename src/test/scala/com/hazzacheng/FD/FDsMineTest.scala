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

  test("get equal attributes") {
    val fds = Array((1, 2), (3, 5), (2, 1), (3, 8), (5, 3), (1, 10), (2, 6), (8, 3))
    val answer = FDsMine.getEqualAttr(fds)
    val expected = List((1, 2), (3, 5), (3, 8))

    //assert(answer.toSet == expected.toSet)
  }

  test("create new orders") {
    val equalAttr = List((1, 2), (3, 4))
    val singleLhsCount = List((1, 100), (2, 50), (3, 66), (4, 95))
    val colSize = 4
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(equalAttr, singleLhsCount, colSize)
    assert(ordersMap == Map(1 -> 1, 2 -> 4))
    assert(orders.toList == List((1, 100), (2, 95)))
    assert(List(1, 2) == del)
  }

  test("get equal Attr"){
    val singleFDs = Array((1,2),(2,1),(1,3),(2,3),(3,4),(4,3))
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    assert(equalAttr == List((1,2),(3,4)))
    assert(withoutEqualAttr.toList == List((1,3),(2,3)))
  }

  test("get bottom FDs"){
    val withoutEqualAttr = Array((1, 3), (2, 3))
    val ordersMap = Map(1 -> 1, 2 -> 4)
    val equalAttr = List((1,2),(3,4))
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttr)
    assert(bottomFDs.toList == List((Set(1), 2)))
  }

  test("get longest Lhs"){
    val newColSize = 3
    val topCandidates = getLongestLhs(newColSize)
    assert(topCandidates.toList == List((Set(1,2),3),(Set(1,3),2),(Set(2,3),1)))
  }

  test("cut in top level"){
    val newColSize = 3
    val topCandidates = getLongestLhs(newColSize)
    val bottomFDs = Array((Set(1), 3))
    cutInTopLevel(topCandidates, bottomFDs)
    assert(topCandidates.toList == List((Set(1,3),2), (Set(2,3),1)))
  }

  test("remove top and bottom"){
    val newColSize = 3
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    assert(candidates.toList == List((Set(1,3),Set(2)), (Set(1,2),Set(3)),
      (Set(2),Set(1,3)), (Set(3),Set(1, 2)), (Set(2, 3),Set(1)), (Set(1),Set(2, 3))))
  }

  test("cut from down to top"){
    val newColSize = 3
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    val bottomFDs = Array((Set(1), 3))
    cutFromDownToTop(candidates, bottomFDs)
    assert(candidates.toList == List((Set(1, 3),Set(2)), (Set(2),Set(1, 3)), (Set(3),Set(1, 2)), (Set(2, 3),Set(1)), (Set(1),Set(2, 3))))
  }

  test("cut from top to down"){
    val newColSize = 3
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    val topFDs = mutable.Set((Set(1,2),3))
    cutFromTopToDown(candidates, topFDs)
    assert(candidates.toList == List((Set(1, 3),Set(2)), (Set(2),Set(1)), (Set(3),Set(1, 2)), (Set(2, 3),Set(1)), (Set(1),Set(2))))
  }

  test("get target candidates"){
    var candidates = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    candidates += Set(1,2) -> mutable.Set(3,4)
    candidates += Set(1,3) -> mutable.Set(2,4)
    candidates += Set(1,4) -> mutable.Set(2,3)
    candidates += Set(1) -> mutable.Set(2,3,4)
    candidates += Set(2) -> mutable.Set(1,3,4)
    candidates += Set(1,2,3) -> mutable.Set(4)
    val common = 1
    val level = 2
    val toChecked = getTargetCandidates(candidates, common, level).toList
    assert(toChecked == List((Set(1, 3),Set(2, 4)), (Set(1, 2),Set(3, 4)), (Set(1, 4),Set(2, 3))))
  }

  test("recover all fds"){
    val results = List((Set(1,2,3),4))
    val equalAttrMap = Map((1,2),(3,4))
    val ordersMap = Map((1,1),(2,3),(3,5),(4,6))
    val fds = recoverAllFDs(results, equalAttrMap, ordersMap)
    println(fds)
  }
}
