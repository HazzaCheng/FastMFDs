package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-10-06
  * Time: 9:01 AM
  */

object FDsMine {
  private val parallelScaleFactor = 4
  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def findOnSpark(sc: SparkContext,
                  df: DataFrame,
                  colSize: Int,
                  filePath: String): Map[Set[Int], List[Int]] = {
    val results = mutable.ListBuffer.empty[(Set[Int], Int)]

    // get fds with single lhs
    val (singleFDs, singleLhsCount) = getBottomFDs(df, colSize)
    df.unpersist()
    // get equal attributes
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    // get new orders
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(equalAttr, singleLhsCount, colSize)
    val newColSize = colSize - equalAttr.length
    // create the new single lhs fds
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)
    // check the fds with the longest lhs
    val topCandidates = getLongestLhs(newColSize)
    cutInTopLevel(topCandidates, bottomFDs)
    val topFDs = getTopFDs(df, topCandidates)
    // get all candidates FD without bottom level and top level
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    // cut from bottom level and top level
    cutFromDownToTop(candidates, bottomFDs)
    cutFromTopToDown(candidates, topFDs)

    // create the RDD
    val rdd = FDsUtils.readAsRdd(sc, filePath, del)

    // get all the partitions by common attributes
    val partitions = new Array[RDD[scala.List[Array[String]]]](newColSize)
    for (i <- orders)
      partitions(i._1 - 1) = repart(sc, rdd, i._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // TODO: need to test different StorageLevel


    for (level <- 2 until (newColSize - 1)) {
      for (common <- orders) {
        val partitionSize = common._2
        val partitionRDD = partitions(common._1 - 1)
        val toChecked = getTargetCandidates(candidates, common._1, level).toList

        if (toChecked.nonEmpty) {
          val minimalFDs = getMinimalsFDs(sc, partitionRDD, toChecked, results, partitionSize, newColSize)
          if (minimalFDs.nonEmpty) {
            cutFromDownToTop(candidates, minimalFDs)
            if (topFDs.nonEmpty) cutInTopLevel(topFDs, minimalFDs)
          }
        }
      }
    }

    // check empty lhs
    val emptyFD = mutable.ListBuffer.empty[Int]
    emptyFD ++= orders.filter(_._2.toInt == 1).map(_._1)
    if (emptyFD.nonEmpty)
      emptyFD.toList.foreach(rhs => results.append((Set.empty[Int], rhs)))

    // check the top level
    if (topFDs.nonEmpty) results ++= topFDs

    // recover all fds
    val fds = recoverAllFDs(results.toList, equalAttrMap, ordersMap)

    fds
  }

  private def getBottomFDs(df: DataFrame,
                           colSize: Int): (Array[(Int, Int)], List[(Int, Int)]) = {
    val lhs = getSingleLhsCount(df, colSize)
    val whole = getTwoAttributesCount(df, colSize)

    val map = whole.groupBy(_._2).map(x => (x._1, x._2.map(_._1)))
    val res = mutable.ListBuffer.empty[(Int, (Int, Int))]

    lhs.foreach{x =>
      map.get(x._2) match {
        case Some(sets) =>
          sets.filter(t => t._1 == x._1 || t._2 == x._1).foreach(t => res.append((x._1, t)))
        case None => None
      }
    }
    val fds = res.toArray.map(x => if (x._1 == x._2._1) (x._1, x._2._2) else (x._1, x._2._1))

    (fds, lhs)
  }

  private def getSingleLhsCount(df: DataFrame, colSize: Int): List[(Int, Int)] = {
    val res = df.columns.map(col => df.groupBy(col).count().count())
      .zipWithIndex.map(x => (x._2, x._1.toInt))

    res.toList
  }

  private def getTwoAttributesCount(df: DataFrame, colSize: Int): List[((Int, Int), Int)] = {
    val columns = df.columns
    val tuples = mutable.ListBuffer.empty[((Int, Int), (String, String))]

    for (i <- 0 until (colSize - 1))
      for (j <- (i + 1) until colSize)
        tuples.append(((i, j), (columns(i), columns(j))))

    val res = tuples.toList.map(x =>
      (x._1, df.groupBy(x._2._1, x._2._2).count().count().toInt))

    res
  }

  def getEqualAttr(fds: Array[(Int, Int)]): (List[(Int, Int)], Array[(Int, Int)]) = {
    val len = fds.length
    val tmp = mutable.ListBuffer.empty[(Int, Int)]

    for (i <- 0 until (len - 1))
      for (j <- i + 1 until len)
        if (fds(i) == fds(j).swap)
          tmp.append(fds(i))
    val res = tmp.toList
    val newFds = fds.filter(x => !res.contains(x) && !res.contains(x.swap))

    (res, newFds)
  }

  def createNewOrders(equalAttr: List[(Int, Int)],
                      singleLhsCount: List[(Int, Int)],
                      colSize: Int): (Map[Int, Int], Map[Int, Int], Array[(Int, Int)], List[Int]) = {
    val maps = singleLhsCount.toMap
    val equalAttrMap = mutable.Map.empty[Int, Int]
    val ordersMap = mutable.Map.empty[Int, Int]
    val tmp = mutable.Set.empty[Int]
    val del = mutable.ListBuffer.empty[Int]
    Range(1, colSize + 1).foreach(tmp.add(_))

    equalAttr.foreach{x =>
      if (maps(x._1) > maps(x._2)) {
        equalAttrMap.put(x._1, x._2)
        tmp.remove(x._2)
        del.append(x._2 - 1)
      } else {
        equalAttrMap.put(x._2, x._1)
        tmp.remove(x._1)
        del.append(x._1 - 1)
      }
    }

    var count = 1
    for (i <- tmp.toList.sorted) {
      ordersMap.put(count, i)
      count += 1
    }

    val orders = ordersMap.toArray.map(x => (x._1, maps(x._2)))
      .sortWith((x, y) => x._2 > y._2)

    (equalAttrMap.toMap, ordersMap.toMap, orders, del.toList.sorted)
  }

  def getNewBottomFDs(singleFDs: Array[(Int, Int)],
                      ordersMap: Map[Int, Int],
                      equalAttrMap: Map[Int, Int]): Array[(Set[Int], Int)] = {
    val equalAttrs = equalAttrMap.values.toSet
    val swappedMap = ordersMap.map(x => (x._2, x._1))
    val fds = singleFDs.filter(x => !equalAttrs.contains(x._1) && !equalAttrs.contains(x._2))
      .map(x => (Set[Int](swappedMap(x._1)), swappedMap(x._2)))

    fds
  }

  def getLongestLhs(colSize: Int): mutable.Set[(Set[Int], Int)] = {
    val res = mutable.Set.empty[(Set[Int], Int)]
    val range = Range(1, colSize + 1)

    for (i <- 1 to colSize)
      res.add(range.filter(_ != i).toSet, i)

    res
  }

  private def getTopFDs(df: DataFrame,
                        topCandidates: mutable.Set[(Set[Int], Int)]): mutable.Set[(Set[Int], Int)] = {
    val cols = df.columns

    val topFDs = topCandidates.filter{x =>
      val lhs = x._1.map(y => cols(y - 1)).toArray
      val whole = df.groupBy(cols(x._2 - 1), lhs: _*).count().count()
      val left = df.groupBy(lhs.last, lhs.init: _*).count().count()

      whole == left
    }

    topFDs
  }

  private def repart(sc: SparkContext,
                     rdd: RDD[Array[String]],
                     attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(_._2)

    partitions
  }

  private def getMinimalsFDs(sc: SparkContext,
                             partitionsRDD: RDD[List[Array[String]]],
                             fds: List[(Set[Int], mutable.Set[Int])],
                             res: mutable.ListBuffer[(Set[Int], Int)],
                             partitionSize: Int,
                             colSize: Int): Array[(Set[Int], Int)] = {
    val fdsBV = sc.broadcast(fds)
    val candidates = partitionsRDD.flatMap(p => checkEachPartition(fdsBV, p, colSize))
      .map(x => (x, 1)).reduceByKey(_ + _).collect()
    val minimalFDs = candidates.filter(_._2 == partitionSize).map(_._1)

    res ++= minimalFDs
    fdsBV.unpersist()
    minimalFDs
  }

  private def checkEachPartition(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                 partition: List[Array[String]],
                                 colSize: Int): List[(Set[Int], Int)] = {
    val minimalFDs = fdsBV.value.flatMap(fd => checkFD(partition, fd._1, fd._2, colSize))

    minimalFDs
  }

  private def checkFD(partition: List[Array[String]],
                      lhs: Set[Int],
                      rhs: mutable.Set[Int],
                      colSize: Int): List[(Set[Int], Int)] = {
    val true_rhs = rhs.clone()
    val dict = mutable.HashMap.empty[String, Array[String]]

    partition.foreach(line => {
      val left = takeAttrLHS(line, lhs)
      val right = takeAttrRHS(line, true_rhs, colSize)
      val value = dict.getOrElse(left, null)
      if (value != null) {
        for (i <- true_rhs.toList)
          if (!value(i).equals(right(i))) true_rhs.remove(i)
      } else dict.put(left, right)
      if (true_rhs.isEmpty) return List()
    })

    true_rhs.map(r => (lhs, r)).toList
  }


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

  def removeTopAndBottom(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                         colSize: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val newCandidates = candidates.filter(x => x._1.size != colSize - 1 && x._1.size!= 1)

    newCandidates
  }

  def cutInTopLevel(topLevel: mutable.Set[(Set[Int], Int)],
                    minimal: Array[(Set[Int], Int)]): Unit = {
    minimal.foreach{x =>
      val del = topLevel.filter(y => isSubSet(y._1, x._1) && y._2 == x._2)
      del.foreach(topLevel.remove(_))
    }
  }

  def cutFromDownToTop(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                       minimal: Array[(Set[Int], Int)]): Unit = {
    for (fd <- minimal) {
      val lend = fd._1.size
      val lhs = candidates.keys.filter(x => x.size > lend && isSubSet(x, fd._1)).toList
      cutInCandidates(candidates, lhs, fd._2)
    }
  }


  def cutFromTopToDown(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                       topFDs: mutable.Set[(Set[Int], Int)]) = {
    for (fd <- topFDs.toList) {
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

  def isSubSet(big: Set[Int], small: Set[Int]): Boolean = {
    small.toList.foreach(s => if (!big.contains(s)) return false)

    true
  }

  private def takeAttrLHS(arr: Array[String],
                          attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.toList.foreach(attr => s.append(arr(attr - 1)))

    s.toString()
  }

  private def takeAttrRHS(arr: Array[String],
                          attributes: mutable.Set[Int],
                          colSize: Int): Array[String] = {
    val res = new Array[String](colSize + 1)
    attributes.toList.foreach(attr => res(attr) = arr(attr - 1))
    res
  }

  def recoverAllFDs(results: List[(Set[Int], Int)],
                    equalAttrMap: Map[Int, Int],
                    ordersMap: Map[Int, Int]): Map[Set[Int], List[Int]]  = {
    val fds = mutable.ListBuffer.empty[(Set[Int], Int)]

    val tmp = results.map{x =>
      val lhs = x._1.map(ordersMap(_))
      val rhs = ordersMap(x._2)
      (lhs, rhs)
    }

    val equalAttrs = equalAttrMap.keySet
    for (fd <- tmp) {
      val list = mutable.ListBuffer.empty[mutable.ListBuffer[Int]]
      list.append(mutable.ListBuffer.empty[Int])
      fd._1.foreach {i =>
        if (equalAttrs contains i) {
          val copy = mutable.ListBuffer.empty[mutable.ListBuffer[Int]]
          list.foreach(x => {
            val y = x.toList
            copy.append(mutable.ListBuffer.empty[Int])
            y.foreach(y0 => copy.last += y0)
          })
          list.foreach(_.append(i))
          copy.foreach(_.append(equalAttrMap(i)))
          list ++= copy
        } else list.foreach(_.append(i))
      }
      fds ++= list.map(x => (x.toSet, fd._2))
    }

    for (fd <- fds.toList)
      if (equalAttrs contains fd._2)
        fds.append((fd._1, equalAttrMap(fd._2)))

    equalAttrMap.toList.foreach{x =>
      fds.append((Set[Int](x._1), x._2))
      fds.append((Set[Int](x._2), x._1))
    }

    fds.toList.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
  }
}
