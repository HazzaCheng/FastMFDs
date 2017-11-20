package com.hazzacheng.FD

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

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

    assert(answer.toSet == expected.toSet)
  }

  test("create new orders") {

  }
}
