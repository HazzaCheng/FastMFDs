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

object DependencyDiscovery {
  private val parallelScaleFactor = 4
  //private val spiltLen = 10000
  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def findOnSpark(ss: SparkSession, rdd: RDD[Array[String]], colSize: Int, spiltLen: Int): Map[Set[Int], mutable.Set[Int]] = {
    val dependencies = FDUtils.getDependencies(colSize)
    val schema = FDUtils.createStructType(colSize)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    //val nums1 = Array(2, 4, 3, 6, 7, 5, 8, 10, 9, 1)
    for (i <- 1 to colSize) {
      time2 = System.currentTimeMillis()
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)

      val partitionsRDD = repart(ss, rdd, i).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val smallPartitionsRDD = partitionsRDD.filter(_.length < spiltLen).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val bigPartitions = partitionsRDD.filter(_.length >= spiltLen).collect()
      partitionsRDD.unpersist()
      val bigPartDF = bigPartitions.map(p => FDUtils.listToRddToDF(ss, p, schema))

      val bigPartitionsLen = bigPartitions.length
      if (bigPartitionsLen == 1 && smallPartitionsRDD.isEmpty()
        || bigPartitionsLen == 0) emptyFD += i
      println("====Small Partitions Size: " + smallPartitionsRDD.count())
      println("====Big Partitions Size: " + bigPartitionsLen)

      for (k <- keys) {
        val ls = lhsAll(k)
        val fds = FDUtils.getSameLhsFD(candidates, ls)
        val failed: ListBuffer[(Set[Int], Int)] = ListBuffer.empty

        checkSmallPartitions(ss, smallPartitionsRDD, fds, failed)
        checkBigPartitions(ss, bigPartDF, fds, failed)

        time1 = System.currentTimeMillis()
        cutLeaves(dependencies, candidates, failed.toList, i)
        println("===========Cut Leaves" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " + (System.currentTimeMillis() - time1))
      }

      results ++= candidates
      smallPartitionsRDD.unpersist()
      println("===========Common Attr" + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }

    time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(ss.sparkContext, results)
    println("===========FindMinFD Use Time=============" + System.currentTimeMillis() + " " + time1 + " " +(System.currentTimeMillis() - time1))
    if (emptyFD.nonEmpty) results += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def checkSmallPartitions(ss: SparkSession,
                           smallPartitionsRDD: RDD[List[Array[String]]],
                           fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
                           failed: ListBuffer[(Set[Int], Int)]): Unit = {
    val fdsBV = ss.sparkContext.broadcast(fds)
    val failFD = smallPartitionsRDD.flatMap(p =>
      checkDependenciesInSmall(fdsBV, p)).collect().distinct.toList
    failed ++= failFD
    fdsBV.unpersist()
    FDUtils.cutInSameLhs(fds, failFD)
  }

  def checkBigPartitions(ss: SparkSession,
                         bigPartDF: Array[DataFrame],
                         fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
                         failed: ListBuffer[(Set[Int], Int)]): Unit = {
    for (partDF <- bigPartDF) {
      if (fds.nonEmpty) {
        val fdsList = fds.toList
        val failFD = fdsList.flatMap(fds => checkDependenciesInBig(partDF, fds))
        failed ++= failFD
        FDUtils.cutInSameLhs(fds, failFD)
      }
    }
  }

  def repart(ss: SparkSession, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(t => t._2)//.repartition(sc.defaultParallelism * parallelScaleFactor)

    partitions
  }

  def checkDependenciesInSmall(fdsBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
                        partition: List[Array[String]]): List[(Set[Int], Int)] = {
    val res = fdsBV.value.toList.flatMap(fd => FDUtils.check(partition, fd._1, fd._2))
    res
  }

  def checkDependenciesInBig(partDF: DataFrame,
                      fds: (Set[Int], mutable.Set[Int])): List[(Set[Int], Int)] = {
    val lhs = fds._1.toList.map(_.toString)
    val lhsCount = partDF.groupBy(lhs.head, lhs.tail: _*).count().count()
    val failedRhs = fds._2.toList.filter(rhs =>
      lhsCount != partDF.groupBy(rhs.toString, lhs: _*).count().count())
    failedRhs.map(rhs => (fds._1, rhs))
  }

  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                failed: List[(Set[Int], Int)], commonAttr: Int) = {
    for (d <- failed) {
      val subSets = FDUtils.getSubsets(d._1.toArray)
      for (subSet <- subSets) {
        if (subSet contains commonAttr) FDUtils.cut(candidates, subSet, d._2)
        else FDUtils.cut(dependencies, subSet, d._2)
      }
    }

  }

  def findMinFD(sc:SparkContext,
                fd:mutable.HashMap[Set[Int], mutable.Set[Int]]):  Map[Set[Int], mutable.Set[Int]] = {
    val fdList = fd.toList
    val data = fdList.groupBy(_._1.size).map(f => (f._1, f._2.toMap))
    val index = data.keys.toList.sortWith((x, y) => x < y)
    val dataBV = sc.broadcast(data)
    val indexBV = sc.broadcast(index)
    val rdd = sc.parallelize(fdList.map(f => (f._1.size, f)), sc.defaultParallelism * parallelScaleFactor)
    val minFD = rdd.map(f => getMinFD(dataBV, f, indexBV)).filter(_._2.size > 0).collect()

    minFD.toMap
  }

  def getMinFD(dataBV: Broadcast[Map[Int, Map[Set[Int], mutable.Set[Int]]]],
               f:(Int, (Set[Int], mutable.Set[Int])), index:Broadcast[List[Int]]): (Set[Int], mutable.Set[Int]) = {
    for(i <- index.value){
      if(i >= f._1) return f._2
      for(fd <- dataBV.value(i))
        if(FDUtils.isSubset(fd._1, f._2._1)) f._2._2 --= fd._2
    }
    f._2
  }

}
