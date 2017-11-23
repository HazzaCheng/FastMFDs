package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
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

object FDsMine_test {
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
    // get equal attributes
    val (equalAttr, withoutEqualAttr) = getEqualAttr(singleFDs)
    // get new orders
    val (equalAttrMap, ordersMap, orders, del) = createNewOrders(equalAttr, singleLhsCount, colSize)
    val newColSize = orders.length
    // create the new single lhs fds
    val bottomFDs = getNewBottomFDs(withoutEqualAttr, ordersMap, equalAttrMap)
    // check the fds with the longest lhs
    val topCandidates = getLongestLhs(newColSize)
    cutInTopLevel(topCandidates, bottomFDs)
    val topFDs = getTopFDs(df, topCandidates)
    df.unpersist()
    // get all candidates FD without bottom level and top level
    val candidates = removeTopAndBottom(getCandidates(newColSize), newColSize)
    // cut from bottom level and top level
    cutFromDownToTop(candidates, bottomFDs)
    cutFromTopToDown(candidates, topFDs)

    // create the RDD
    val rdd = FDsUtils.readAsRdd(sc, filePath, del)

    // create the map which save cutted leaves
    val wholeMap = mutable.HashMap
      .empty[Int, mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]]


    // get all the partitions by common attributes
    val partitions = new Array[RDD[(String, List[Array[String]])]](newColSize)
    for (common <- orders) {
      wholeMap.put(common._1, mutable.HashMap.empty[String, mutable.HashSet[(Set[Int], Int)]])
      partitions(common._1 - 1) = repart(sc, rdd, common._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      // TODO: need to test different StorageLevel
    }

    for (level <- 2 until (newColSize - 1)) {
      for (common <- orders) {
        val partitionSize = common._2
        val partitionRDD = partitions(common._1 - 1)
        val toChecked = getTargetCandidates(candidates, common._1, level).toList
        val levelMap = wholeMap(common._1)

        if (toChecked.nonEmpty) {
          val (minimalFDs, failFDs, partWithFailFDs) =
            getMinimalsFDs(sc, partitionRDD, toChecked, results, partitionSize, newColSize, levelMap)
          if (minimalFDs.nonEmpty) {
            cutFromDownToTop(candidates, minimalFDs)
            if (topFDs.nonEmpty) cutInTopLevel(topFDs, minimalFDs)
          }
          if (failFDs.nonEmpty) {
            val cuttedFDsMap = getCuttedFDsMap(candidates, failFDs)
            updateLevelMap(cuttedFDsMap, partWithFailFDs, levelMap, level)
          }
        }

        wholeMap.update(common._1, levelMap)
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
      .zipWithIndex.map(x => (x._2 + 1, x._1.toInt))

    res.toList
  }

  private def getTwoAttributesCount(df: DataFrame, colSize: Int): List[((Int, Int), Int)] = {
    val columns = df.columns
    val tuples = mutable.ListBuffer.empty[((Int, Int), (String, String))]

    for (i <- 0 until (colSize - 1))
      for (j <- (i + 1) until colSize)
        tuples.append(((i + 1, j + 1), (columns(i), columns(j))))

    val res = tuples.toList.map(x =>
      (x._1, df.groupBy(x._2._1, x._2._2).count().count().toInt))

    res
  }

  def getEqualAttr(fds: Array[(Int, Int)]): (List[Set[Int]], Array[(Int, Int)]) = {
    val len = fds.length
    val tmp = mutable.HashSet.empty[(Int, Int)]

    for (i <- 0 until (len - 1))
      for (j <- i + 1 until len)
        if (fds(i) == fds(j).swap)
          tmp.add(fds(i))
    val newFds = fds.filter(x => !tmp.contains(x) && !tmp.contains(x.swap))

    val res = mutable.ListBuffer.empty[Set[Int]]
    val sets = tmp.map(fd => Set[Int](fd._1, fd._2))
    var setsArr = sets.toArray
    while (setsArr.nonEmpty) {
      var set = setsArr.last
      sets.remove(set)
      setsArr.init.foreach{x =>
        if ((set & x).nonEmpty) {
          set = (set | x)
          sets.remove(x)
        }
      }
      res.append(set)
      setsArr = sets.toArray
    }


    (res.toList, newFds)
  }

  def createNewOrders(equalAttr: List[Set[Int]],
                      singleLhsCount: List[(Int, Int)],
                      colSize: Int): (Map[Int, List[Int]], Map[Int, Int], Array[(Int, Int)], List[Int]) = {
    val maps = singleLhsCount.toMap
    val equalAttrMap = mutable.Map.empty[Int, List[Int]]
    val ordersMap = mutable.Map.empty[Int, Int]
    val tmp = mutable.Set.empty[Int]
    val del = mutable.ListBuffer.empty[Int]
    Range(1, colSize + 1).foreach(tmp.add(_))

    equalAttr.foreach{x =>
      val maxAttr = x.maxBy(y => maps(y))
      val smallAttr = x.filter(_ != maxAttr).toList
      del ++= smallAttr
      tmp --= smallAttr
      equalAttrMap.put(maxAttr, smallAttr)
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
                      equalAttrMap: Map[Int, List[Int]]): Array[(Set[Int], Int)] = {
    val equalAttrs = equalAttrMap.values.flatMap(_.toSet).toSet
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
             attribute: Int): RDD[(String, List[Array[String]])] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _)

    partitions
  }

  private def getMinimalsFDs(sc: SparkContext,
                             partitionsRDD: RDD[(String, List[Array[String]])],
                             fds: List[(Set[Int], mutable.Set[Int])],
                             res: mutable.ListBuffer[(Set[Int], Int)],
                             partitionSize: Int,
                             colSize: Int,
                             levelMap: mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]
                            ): (Array[(Set[Int], Int)], Set[(Set[Int], Int)], Array[(String, List[(Set[Int], Int)])]) = {
    val fdsBV = sc.broadcast(fds)
    val levelMapBV = sc.broadcast(levelMap)
    val tuplesRDD = partitionsRDD.map(p => checkEachPartition(fdsBV, levelMapBV, p, colSize)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val duplicatesRDD = tuplesRDD.flatMap(x => x._2).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val candidates = duplicatesRDD.map(x => (x, 1)).reduceByKey(_ + _).collect()
    // TODO: local vs parallel
    val minimalFDs = candidates.filter(_._2 == partitionSize).map(_._1)
    res ++= minimalFDs

    val failFDs = duplicatesRDD.collect().distinct.toSet -- minimalFDs
    val partWithFailFDs = tuplesRDD.collect()

    tuplesRDD.unpersist()
    duplicatesRDD.unpersist()
    fdsBV.unpersist()

    (minimalFDs, failFDs, partWithFailFDs)
  }

  private def checkEachPartition(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
                                 levelMapBV: Broadcast[mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]]],
                                 partition: (String, List[Array[String]]),
                                 colSize: Int): (String, List[(Set[Int], Int)]) = {
    val levelMap = levelMapBV.value.getOrElse(partition._1, mutable.HashSet.empty[(Set[Int], Int)])
    val minimalFDs = fdsBV.value.flatMap(fd => checkFD(partition._2, levelMap, fd._1, fd._2, colSize))

    (partition._1, minimalFDs)
  }

  private def checkFD(partition: List[Array[String]],
                      levelMap: mutable.HashSet[(Set[Int], Int)],
                      lhs: Set[Int],
                      rhs: mutable.Set[Int],
                      colSize: Int): List[(Set[Int], Int)] = {
    val true_rhs = rhs.clone()
    val tmp = mutable.Set.empty[Int]
    val dict = mutable.HashMap.empty[String, Array[String]]

    rhs.foreach{r =>
      if (levelMap contains (lhs, r)) {
        tmp.add(r)
        true_rhs.remove(r)
      }
    }

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

    true_rhs ++= tmp

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
                       topFDs: mutable.Set[(Set[Int], Int)]): Unit = {
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

  private def isSubSet(big: Set[Int], small: Set[Int]): Boolean = {
    small.toList.foreach(s => if (!big.contains(s)) return false)

    true
  }

  private def takeAttrLHS(arr: Array[String],
                  attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.toList.foreach(attr => s.append(arr(attr - 1) + " "))

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
                    equalAttrMap: Map[Int, List[Int]],
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
      for (attr <- fd._1) {
        if (equalAttrs contains attr) {
          val temp = list.toList.map(_.clone())
          list.foreach(_.append(attr))
          for (ll <- temp) {
            val add = equalAttrMap(attr).map{x =>
              val clone = ll.clone()
              clone.append(x)
              clone
            }
            list ++= add
          }
        } else list.foreach(_.append(attr))
      }
      fds ++= list.map(x => (x.toSet, fd._2))
    }

    for (fd <- fds.toList)
      if (equalAttrs contains fd._2)
        equalAttrMap(fd._2).foreach(x => fds.append((fd._1, x)))

    val equalClass = equalAttrMap.toList.map(x => x._1 :: x._2)
    equalClass.foreach{ec =>
      ec.foreach{x =>
        ec.foreach{y =>
          if (x != y) fds.append((Set[Int](x), y))
        }
      }
    }

    fds.toList.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
  }

  def updateLevelMap(cuttedFDsMap: mutable.HashMap[(Set[Int], Int), mutable.HashSet[(Set[Int], Int)]],
                     partWithFailFDs: Array[(String, List[(Set[Int], Int)])],
                     levelMap: mutable.HashMap[String, mutable.HashSet[(Set[Int], Int)]],
                     level: Int): Unit = {
    partWithFailFDs.foreach{x =>
      val cutted = levelMap.getOrElse(x._1, mutable.HashSet.empty[(Set[Int], Int)])
        .filter(_._1.size > level)

      x._2.foreach{y =>
        val value = cuttedFDsMap.getOrElse(y, mutable.HashSet.empty[(Set[Int], Int)])
        if (value.nonEmpty) cutted ++= value
      }

      if (cutted.nonEmpty) levelMap.update(x._1, cutted)
    }
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
