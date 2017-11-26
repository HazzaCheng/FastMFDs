package com.hazzacheng.FD
import org.scalatest.FunSuite
import com.hazzacheng.FD.utils.CandidatesUtils._
import com.hazzacheng.FD.utils.DataFrameUtils._
import com.hazzacheng.FD.utils.RddUtils._
import scala.collection.mutable

/**
  * Created by Administrator on 2017/11/26.
  */
class CandidatesUtilsTest extends FunSuite{
  test("getSubSets") {
    val nums = Array(1, 2, 4)
    val expected = Array(Set(1, 2, 4), Set(1, 2), Set(1, 4), Set(1),
      Set(2), Set(2, 4), Set(4)).map(subSet => subSet.toString())
    val res = getSubsets(nums).map(subSet => subSet.toString()).toArray

    //assert(arr2str(expected) == arr2str(res))
  }

  test("getCandidates") {
    val colSize = 15
    val candidates = getCandidates(15)

    var size = 0
    candidates.foreach(size += _._2.size)
    //    println("Total hava " + size)

    assert(size == colSize * (1 << colSize - 1) - colSize)
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

    cutFromTopToDown(candidates, topFDs.toArray)
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


}
