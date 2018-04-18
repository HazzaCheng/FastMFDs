package com.hazzacheng.FD.utils

import org.apache.spark.SparkContext

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

  def getCandidatesParallel(sc: SparkContext, num: Int
                           ): mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]] = {
    val dependencies = mutable.HashMap.empty[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]]

    for (i <- 1 to num) {
      val nums = Range(1, num + 1).filter(_ != i).toArray
      val subSets = getSubsetsParallel(sc, nums)
      for (subSet <- subSets) {
        val len = subSet.size
        val temp = dependencies.getOrElse(len, mutable.HashMap.empty[Set[Int], mutable.Set[Int]])
        var value = temp.getOrElse(subSet, mutable.Set.empty[Int])
        value += i
        if (subSet.nonEmpty) {
          temp.update(subSet, value)
          dependencies.update(len, temp)
        }
      }
    }

    dependencies
  }

  def getSubsetsParallel(sc: SparkContext, nums: Array[Int]): List[Set[Int]] = {
    val numsLen = nums.length
    val subsetLen = 1 << numsLen
    val subSets = mutable.ListBuffer.empty[Set[Int]]
    if (numsLen <= 18) {
      for (i <- 0 until subsetLen) {
        val subSet = mutable.Set.empty[Int]
        for (j <- 0 until numsLen) {
          if (((i >> j) & 1) != 0) subSet += nums(j)
        }
        if (subSet.nonEmpty) subSets.append(subSet.toSet)
      }
      subSets.toList
    }
    else {
      val numsLenBV = sc.broadcast(numsLen)
      val numsArrBV = sc.broadcast(nums)
      val subsetLenRange = splitRange(subsetLen, 1000)
      sc.parallelize(subsetLenRange).map(x => (x, mutable.ListBuffer.empty[Set[Int]])).map(x => {
        for (i <- x._1._1 until x._1._2) {
          val subSet = mutable.Set.empty[Int]
          for (j <- 0 until numsLenBV.value) {
            if (((i >> j) & 1) != 0) subSet += numsArrBV.value(j)
          }
          x._2.append(subSet.toSet)
        }
        x._2.toList
      }).flatMap(x => x).collect().toList
    }
  }

  def splitRange(num: Int, splitNum: Int): List[(Int, Int)] = {
    val res = mutable.ListBuffer.empty[(Int, Int)]
    val len = num / splitNum
    for (i <- 0 to splitNum) {
      if ((i + 1) * len <= num) {
        res.append((i * len, i * len + len))
      }
      else res.append((i * len, num))
    }
    res.toList
  }

  def getCandidates(num: Int): mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]] = {
    val dependencies = mutable.HashMap.empty[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]]
    for (i <- 1 to num) {
      val nums = Range(1, num + 1).filter(_ != i).toArray
      val subSets = getSubsets(nums)
      for (subSet <- subSets) {
        val len = subSet.size
        val temp = dependencies.getOrElse(len, mutable.HashMap.empty[Set[Int], mutable.Set[Int]])
        var value = temp.getOrElse(subSet, mutable.Set.empty[Int])
        value += i
        temp.update(subSet, value)
        dependencies.update(len, temp)
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

  def getToCheckedSize(toChecked: List[(Set[Int], mutable.Set[Int])]): Int = {
    var sum = 0

    toChecked.foreach(x => sum += x._2.size)

    sum
  }

  def getTargetCandidates(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                          common: Int,
                          level: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val res = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (key <- candidates(level).keys) {
      if (key.contains(common)) {
        res.put(key, candidates(level)(key))
        candidates(level).remove(key)
      }
    }
    res
  }


  def getTargetCandidates(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                          common: Int,
                          level: Int,
                          cols: Set[Int]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val res = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

    for (key <- candidates(level).keys) {
      if (key.contains(common) && key.exists(!cols.contains(_))) {
        val vals = candidates(level)(key).partition(cols.contains)
        res.put(key, vals._1)
        if (vals._2.isEmpty) candidates(level).remove(key)
        else candidates(level).update(key, vals._2)
      }
    }

    res
  }

  def getLevelCandidates(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                         level: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val res = candidates(level)
    candidates.remove(level)
    res
  }

  def removeTopAndBottom(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                         colSize: Int): mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]] = {
    candidates.remove(colSize - 1)
    candidates.remove(1)
    candidates
  }


  def cutInTopLevels(topLevels: mutable.Set[(Set[Int], Int)],
                     minimal: Array[(Set[Int], Int)]): Unit = {
    if (topLevels.isEmpty || minimal.isEmpty) return
    minimal.foreach { x =>
      val del = topLevels.filter(y => isSubSet(y._1, x._1) && y._2 == x._2)
      del.foreach(topLevels.remove)
    }
  }


  def cutFromDownToTop(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                       minimal: Array[(Set[Int], Int)]): Unit = {
    if (minimal.isEmpty || candidates.isEmpty) return
    for (fd <- minimal) {
      val lend = fd._1.size
      val lhs = candidates.filter(x => x._1 > lend).values.toList.flatMap(x => x.keys.toList.filter(y => isSubSet(y, fd._1)))
      //      val lhs = candidates.keys.filter(x => x.size > lend && isSubSet(x, fd._1)).toList
      cutInCandidates(candidates, lhs, fd._2)
    }
  }


  def cutFromTopToDown(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                       failFDs: Array[(Set[Int], Int)]): Unit = {
    if (candidates.isEmpty || failFDs.isEmpty) return
    for (fd <- failFDs) {
      val lhs = candidates.values.toList.flatMap(x => x.keys.filter(y => isSubSet(fd._1, y)).toList)
      //      val lhs = candidates.keys.filter(x => isSubSet(fd._1, x)).toList
      cutInCandidates(candidates, lhs, fd._2)
    }
  }

  def cutInCandidates(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                      lhs: List[Set[Int]],
                      rhs: Int): Unit = {
    for (l <- lhs) {
      val value = candidates(l.size)(l)
      if (value contains rhs) {
        if (value.size == 1) candidates(l.size).remove(l)
        else {
          value.remove(rhs)
          candidates(l.size).update(l, value)
        }
      }
    }
  }

  /*  def getCutted(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
              fd: (Set[Int], Int)): mutable.HashSet[(Set[Int], Int)] = {
  val res = mutable.HashSet.empty[(Set[Int], Int)]

  val lhs = candidates.keys.filter(x => isSubSet(fd._1, x)).toList
  res ++= getCuttedInCandidates(candidates, lhs, fd._2)

  res
}*/

  def getCuttedNotDel(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                      fd: (Set[Int], Int)): mutable.HashSet[(Set[Int], Int)] = {
    val res = mutable.HashSet.empty[(Set[Int], Int)]
    val len = fd._1.size

    val lhs = candidates(len + 1).keys.filter(x => isSubSet(x, fd._1)).toList
    //    val lhs = candidates.keys.filter(x => x.size == len + 1 && isSubSet(x, fd._1)).toList
    res ++= getCuttedInCandidatesNotDel(candidates, lhs, fd._2)

    res
  }

  /*  def getCuttedInCandidates(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
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
}*/

  def getCuttedInCandidatesNotDel(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                                  lhs: List[Set[Int]],
                                  rhs: Int): List[(Set[Int], Int)] = {
    val cutted = mutable.ListBuffer.empty[(Set[Int], Int)]

    for (l <- lhs) {
      val value = candidates(l.size)(l)
      if (value contains rhs) cutted.append((l, rhs))
    }

    cutted.toList
  }

  def isSubSet(big: Set[Int], small: Set[Int]): Boolean = {
    small.toList.foreach(s => if (!big.contains(s)) return false)

    true
  }

  def getCuttedFDsMap(candidates: mutable.HashMap[Int, mutable.HashMap[Set[Int], mutable.Set[Int]]],
                      failFDs: Set[(Set[Int], Int)]
                     ): mutable.HashMap[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]] = {
    val map = mutable.HashMap.empty[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]]
    failFDs.foreach { x =>
      val cutted = getCuttedNotDel(candidates, x)
      if (cutted.nonEmpty) {
        map.put(x, cutted)
      }
    }

    map
  }

  def findMinFD(topFDs: mutable.Set[(Set[Int], Int)]): Unit = {
    val sortedFDs = topFDs.toList.sortBy(_._1.size)

    for (fd <- sortedFDs) {
      if (topFDs contains fd) {
        val len = fd._1.size
        topFDs.toList.filter(x => x._1.size > len && x._2 == fd._2).foreach { x =>
          if (isSubSet(x._1, fd._1)) topFDs.remove(x)
        }
      }
    }
  }

}
