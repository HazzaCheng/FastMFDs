package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]],
                  colSize: Int, orders: Array[(Int, Long)]): Map[Set[Int], List[Int]] = {
    val dependencies = FDUtils.getDependencies(colSize)
    val results = mutable.HashSet.empty[(Set[Int], Int)]

    // get all the partitions by common attributes
    val partitions = new Array[RDD[scala.List[Array[String]]]](colSize)
    for (i <- orders)
      partitions(i._1 - 1) = repart(sc, rdd, i._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      // TODO: need to test different StorageLevel

    // check the shortest lhs and get the equal attributes
    val singleLhsMinimalFds = checkSingleLHS(sc, partitions, orders, colSize)



    for (i <- orders) {
      val partitionSize = i._2.toInt
      time2 = System.currentTimeMillis()
      val candidates = getCandidateDependencies(dependencies, i._1)
      //val lhsWithSize = candidates.toList.groupBy(_._1.size)
      //val keys = lhsWithSize.keys.toList.sortWith((x, y) => x < y)

//      val partitionsRDD = repart(sc, rdd, i._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val partitionsRDD = partitions(i._1 - 1)
      for (k <- 1 until colSize) {
//        val fds = lhsWithSize.getOrElse(k, List())
        val fds = candidates.toList.filter(_._1.size == k)

        println("====Size lhs in "  + i + " " + k + " " + fds.size)
        if (fds.nonEmpty) {
          time1 = System.currentTimeMillis()
          val minimalFDs = checkPartitionsRDD(sc, partitionsRDD, fds, results, partitionSize, colSize)
          println("====Minimal FDs in " + i + " " + k)
          minimalFDs.foreach(println)
          println("====Paritions in " + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))

          time1 = System.currentTimeMillis()
          println("====Size successed:"  + i + " " + k + " " + minimalFDs.size)
          if (minimalFDs.nonEmpty) {
            cutLeaves(candidates, results, minimalFDs)
            println("====Cut Leaves in " + k + " Use Time=============" + (System.currentTimeMillis() - time1))
          }
        }
      }

      println("====Common Attr in " + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }


    // check empty lhs
    val emptyFD = mutable.ListBuffer.empty[Int]
    emptyFD ++= orders.filter(_._2.toInt == 1).map(_._1)
    if (emptyFD.nonEmpty)
      emptyFD.toList.foreach(rhs => results.add((Set.empty[Int], rhs)))

    results.toList.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
  }

  def repart(sc: SparkContext,
             rdd: RDD[Array[String]],
             attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(_._2)

    partitions
  }

  private def checkSingleLHS(sc: SparkContext,
                       partitions: Array[RDD[scala.List[Array[String]]]],
                       orders: Array[(Int, Long)],
                       colSize: Int): List[(Set[Int], Int)] = {
    val res = mutable.ListBuffer.empty[(Set[Int], Int)]

    for (commonAttr <- orders) {
      val candidates = getSingleLhsFDs(commonAttr._1, colSize)
      val partitionsSize = commonAttr._2.toInt
      val partitionRDD = partitions(commonAttr._1 - 1)
      getMinimalsFDs(sc, partitionRDD, List(candidates), res, partitionsSize, colSize)
    }

    res.toList
  }

  private def getSingleLhsFDs(lhs: Int,
                             colSize: Int): (Set[Int], mutable.Set[Int]) = {
    val fds = mutable.Set.empty[Int]
    Range(1, colSize + 1).filter(_ != lhs).foreach(fds.add(_))

    (Set[Int](lhs), fds)
  }

  def getMinimalsFDs(sc: SparkContext,
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

  def checkEachPartition(fdsBV: Broadcast[List[(Set[Int], mutable.Set[Int])]],
               partition: List[Array[String]],
               colSize: Int): List[(Set[Int], Int)] = {
    val minimalFDs = fdsBV.value.flatMap(fd => checkFD(partition, fd._1, fd._2, colSize))

    minimalFDs
  }

  def checkFD(partition: List[Array[String]],
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

  def getEqualAttributes(fds: List[(Set[Int], Int)]): List[(Int, Int)] = {
    val res = mutable.HashSet.empty[(Int, Int)]
  }

  def getDependencies(num: Int): mutable.HashMap[Set[Int], mutable.Set[Int]]= {
    val dependencies = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (i <- 1 to num) {
      val nums = Range(1, num + 1).filter(_ != i).toArray
      val subSets = FDUtils.getSubsets(nums)
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

  def getCandidateDependencies(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                               target: Int): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val candidates = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (key <- dependencies.keys) {
      if (key contains target) {
        candidates += (key -> dependencies.get(key).get)
        dependencies -= key
      }
    }

    candidates
  }


  def cutLeaves(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                results: mutable.HashSet[(Set[Int], Int)],
                successed: Array[(Set[Int], Int)]): Unit = {
    for (d <- successed) {
      val lend = d._1.size
      val lhs = candidates.keys.filter(x => x.size > lend && isSubSet(x, d._1)).toList
      val nonMinimal = results.toList.filter(x =>
        x._1.size > lend && isSubSet(x._1, d._1) && x._2 == d._2)
      cutInCandidates(candidates, lhs, d._2)
      nonMinimal.foreach(fd => results.remove(fd))
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

  def takeAttrLHS(arr: Array[String],
                  attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.toList.foreach(attr => s.append(arr(attr - 1)))

    s.toString()
  }

  def takeAttrRHS(arr: Array[String],
                  attributes: mutable.Set[Int],
                  colSize: Int): Array[String] = {
    val res = new Array[String](colSize + 1)
    attributes.toList.foreach(attr => res(attr) = arr(attr - 1))
    res
  }

}
