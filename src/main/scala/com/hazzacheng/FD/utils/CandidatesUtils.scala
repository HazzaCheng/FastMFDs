package com.hazzacheng.FD.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-11-26 
  * Time: 1:01 PM
  */
object CandidatesUtils {

  def getCandidates(num: Int): mutable.HashMap[Set[Int], mutable.Set[Int]]= {
    val dependencies = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (i <- 1 to num) {
      val nums = Range(1, num + 1).filter(_ != i).toArray
      val subSets = getSubsets(nums)
      for (subSet <- subSets) {
        var value = dependencies.getOrElse(subSet, mutable.Set.empty[Int])
        value += i
        dependencies.update(subSet, value)
      }
    }

    dependencies
  }

  def getSubsets(nums: Array[Int]): List[Set[Int]] = {
    val numsLen = nums.length
    val subsetLen = 1 << numsLen
    var subSets: ListBuffer[Set[Int]] = new ListBuffer()

    for (i <- 0 until subsetLen) {
      val subSet = mutable.Set.empty[Int]
      for (j <- 0 until numsLen) {
        if (((i >> j) & 1) != 0) subSet += nums(j)
      }
      if (subSet.nonEmpty) subSets += subSet.toSet
    }

    subSets.toList
  }

  def getTargetCandidates(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                          common: Int,
                          level: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val res = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (key <- candidates.keys) {
      if (key.size == level && key.contains(common)) {
        res.put(key, candidates(key))
        candidates.remove(key)
      }
    }

    res
  }

  def getLevelCandidates(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                         level: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val res = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (key <- candidates.keys) {
      if (key.size == level) {
        res.put(key, candidates(key))
        candidates.remove(key)
      }
    }

    res
  }

  def removeTopAndBottom(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                         colSize: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val newCandidates = candidates.filter(x => x._1.size != colSize - 1 && x._1.size!= 1)

    newCandidates
  }

  def cutInTopLevels(topLevels: mutable.Set[(Set[Int], Int)],
                     minimal: Array[(Set[Int], Int)]): Unit = {
    if (topLevels.isEmpty || minimal.isEmpty) return
    minimal.foreach{x =>
      val del = topLevels.filter(y => isSubSet(y._1, x._1) && y._2 == x._2)
      del.foreach(topLevels.remove(_))
    }
  }

  def cutFromDownToTop(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                       minimal: Array[(Set[Int], Int)]): Unit = {
    if (minimal.isEmpty || candidates.isEmpty) return
    for (fd <- minimal) {
      val lend = fd._1.size
      val lhs = candidates.keys.filter(x => x.size > lend && isSubSet(x, fd._1)).toList
      cutInCandidates(candidates, lhs, fd._2)
    }
  }


  def cutFromTopToDown(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                       failFDs: Array[(Set[Int], Int)]): Unit = {
    if (candidates.isEmpty || failFDs.isEmpty) return
    for (fd <- failFDs) {
      val lhs = candidates.keys.filter(x => isSubSet(fd._1, x)).toList
      cutInCandidates(candidates, lhs, fd._2)
    }
  }

  def cutInCandidates(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                      lhs: List[Set[Int]],
                      rhs: Int): Unit = {
    for (l <- lhs) {
      val value = candidates(l)
      if (value contains rhs) {
        if (value.size == 1) candidates.remove(l)
        else {
          value.remove(rhs)
          candidates.update(l, value)
        }
      }
    }
  }

  def getCutted(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                fd: (Set[Int], Int)): mutable.HashSet[(Set[Int], Int)] = {
    val res = mutable.HashSet.empty[(Set[Int], Int)]

    val lhs = candidates.keys.filter(x => isSubSet(fd._1, x)).toList
    res ++= getCuttedInCandidates(candidates, lhs, fd._2)

    res
  }

  def getCuttedInCandidates(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                            lhs: List[Set[Int]],
                            rhs: Int): List[(Set[Int], Int)]= {
    val cutted = mutable.ListBuffer.empty[(Set[Int], Int)]

    for (l <- lhs) {
      val value = candidates(l)
      if (value contains rhs) {
        cutted.append((l, rhs))
        if (value.size == 1) candidates.remove(l)
        else {
          value.remove(rhs)
          candidates.update(l, value)
        }
      }
    }

    cutted.toList
  }

  def isSubSet(big: Set[Int], small: Set[Int]): Boolean = {
    small.toList.foreach(s => if (!big.contains(s)) return false)

    true
  }

  def getCuttedFDsMap(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                      failFDs: Set[(Set[Int], Int)]
                     ): mutable.HashMap[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]] = {
    val map = mutable.HashMap.empty[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]]
    // TODO: set vs hashset
    failFDs.foreach{x =>
      val cutted = getCutted(candidates, x)
      if (cutted.nonEmpty) {
        map.put(x, cutted)
      }
    }

    map
  }
}
