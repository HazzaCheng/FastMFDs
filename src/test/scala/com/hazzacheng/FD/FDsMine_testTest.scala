package com.hazzacheng.FD
import com.hazzacheng.FD.FDsMine_test._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

/**
  * Created by Administrator on 2017/11/21.
  */
class FDsMine_testTest extends FunSuite{
    test("get equal attributes") {
        val fds = Array((1, 2), (3, 5), (2, 1), (3, 8), (5, 3), (1, 10), (2, 6), (8, 3))
        val (equalAttrs, newFDs) = FDsMine.getEqualAttr(fds)
        val expected1 = List((1, 2), (3, 5), (3, 8))
        val expected2 = List((1, 10), (2, 6))

        assert(equalAttrs.toSet == expected1.toSet)
        assert(newFDs.toSet == expected2.toSet)
    }

    test("create new orders") {
        val equalAttr = List((1, 2), (3, 4))
        val singleLhsCount = List((1, 100), (2, 50), (3, 66), (4, 95))
        val colSize = 4
        val (equalAttrMap, ordersMap, orders, del) = createNewOrders(equalAttr, singleLhsCount, colSize)

        assert(equalAttrMap == Map(1 -> 2, 4 -> 3))
        assert(ordersMap == Map(1 -> 1, 2 -> 4))
        assert(orders.toList == List((1, 100), (2, 95)))
        assert(List(1, 2) == del)
    }

    test("get bottom FDs"){
        val withoutEqualAttr = Array((1, 3), (2, 3), (1, 4), (2, 4), (6, 7))
        val ordersMap = Map(1 -> 1, 2 -> 4, 6 -> 6, 7 -> 7)
        val equalAttrMap = Map(1 -> 2, 4 -> 3)
        val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)

        assert(bottomFDs.toList == List((Set(1), 2), (Set(6), 7)))
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
        val map = Map(Set(1, 3, 5) -> List(6), Set(2, 4, 5) -> List(6), Set(2, 3, 5) -> List(6), Set(1, 4, 5) -> List(6))
        assert(fds == map)
    }

    test("get cut FDs Map and update"){
        val candidates =  mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
        val failFDs = mutable.Set[(Set[Int], Int)]
        val partWithFailFDs = ""
        val levelMap = ""
        val level = ""
        //val cuttedFDsMap = getCuttedFDsMap(candidates, failFDs)
        //updateLevelMap(cuttedFDsMap, partWithFailFDs, levelMap, level)
    }
}
