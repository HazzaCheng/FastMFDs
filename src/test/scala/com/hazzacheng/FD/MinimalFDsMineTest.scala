package com.hazzacheng.FD
import org.scalatest.FunSuite
import com.hazzacheng.FD.MinimalFDsMine._
import scala.collection.mutable
/**
  * Created by Administrator on 2017/11/26.
  */
class MinimalFDsMineTest extends FunSuite{
  test("create new orders"){
    val lessAttrsCountMap = mutable.HashMap.empty[Set[Int], Int]
    val equalAttr = List(Set(1,2))
    val singleLhsCount = List((1,20),(2,50),(3,66),(4,120))
    val colSize = 4
    val twoAttrsCount = List(((1,2),20),((1,4),120))
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(lessAttrsCountMap, equalAttr, singleLhsCount, colSize, twoAttrsCount)
//    println(equalAttrMap.toList.toString())
//    println(ordersMap.toList.toString())
//    println(orders.toList.toString())
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

  test("getLongestLhs"){
    val newColSize = 3
    val topCandidates = getLongestLhs(newColSize)

    assert(topCandidates.toList == List((Set(1,2),3),(Set(1,3),2),(Set(2,3),1)))
  }

  test("getNewBottomFDs"){
    val withoutEqualAttr = Array((1, 3), (2, 3), (1, 4), (2, 4), (6, 7))
    val ordersMap = Map(1 -> 1, 2 -> 4, 6 -> 6, 7 -> 7)
    val equalAttrMap = Map(1 -> List(2), 4 -> List(3, 5))
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)

    assert(bottomFDs.toSet == Set((Set(1), 2), (Set(6), 7)))
  }

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
}
