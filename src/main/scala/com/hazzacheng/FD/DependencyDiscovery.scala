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

object DependencyDiscovery {
  private val parallelScaleFactor = 4
  //private val spiltLen = 10000
  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]],
                  colSize: Int, orders: Array[Int], spiltLen: Int): Map[Set[Int], mutable.Set[Int]] = {
    val dependencies = FDUtils.getDependencies(colSize)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (i <- orders) {
      time2 = System.currentTimeMillis()
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val dict = mutable.HashMap.empty[String, mutable.HashMap[Set[Int], Int]]

      val partitionsRDD = repart(sc, rdd, i).persist(StorageLevel.MEMORY_AND_DISK_SER)
//      val smallPartitionsRDD = partitionsRDD.filter(_.length < spiltLen).persist(StorageLevel.MEMORY_AND_DISK_SER)
//      val bigPartitions = partitionsRDD.filter(_.length >= spiltLen).collect()
//      partitionsRDD.unpersist()
//
//      val bigPartitionsLen = bigPartitions.length
//      if (bigPartitionsLen == 1 && smallPartitionsRDD.isEmpty()
//        || bigPartitionsLen == 0) emptyFD += i
//      println("====Small Partitions Size: " + smallPartitionsRDD.count())
//      println("====Big Partitions Size: " + bigPartitionsLen)
      for (k <- keys) {
        val ls = lhsAll(k)
        val fds = FDUtils.getSameLhsFD(candidates, ls)
        if (fds.nonEmpty) {
          val failed: ListBuffer[(Set[Int], Int)] = ListBuffer.empty
          time1 = System.currentTimeMillis()
          checkSmallPartitions(sc, partitionsRDD, fds, failed, dict)
          println("===========Paritions Small" + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))
          //        time1 = System.currentTimeMillis()
          //        checkBigPartitions(sc, bigPartitions, fds, failed)
          //        println("===========Paritions Big" + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))

          time1 = System.currentTimeMillis()
          cutLeaves(dependencies, candidates, failed.toList, i)
          println("===========Cut Leaves" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " + (System.currentTimeMillis() - time1))
        }
      }

      results ++= candidates
//      smallPartitionsRDD.unpersist()
      println("===========Common Attr" + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }

    time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(sc, results)
    println("===========FindMinFD Use Time=============" + System.currentTimeMillis() + " " + time1 + " " +(System.currentTimeMillis() - time1))
    if (emptyFD.nonEmpty) results += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def checkSmallPartitions(sc: SparkContext,
                           smallPartitionsRDD: RDD[(String, List[Array[String]])],
                           fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
                           failed: ListBuffer[(Set[Int], Int)],
                           dict:mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]): Unit = {
    val failedFD = mutable.ListBuffer.empty[(Set[Int], Int)]
    val fdsBV = sc.broadcast(fds)
    val dictBV = sc.broadcast(dict)
    val tupleInfo = smallPartitionsRDD.map(p =>
      checkDependenciesInSmall(fdsBV, dictBV, p)).collect()
    dict.clear()
    tupleInfo.foreach(tuple => {
      dict += tuple._2 -> tuple._3
      failedFD ++= tuple._1
    })
    failed ++= failedFD.distinct.toList
    fdsBV.unpersist()
    dictBV.unpersist()
  }

//  def checkBigPartitions(sc: SparkContext,
//                         bigPartitions: Array[List[Array[String]]],
//                         fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
//                         failed: ListBuffer[(Set[Int], Int)]): Unit = {
//    for (partition <- bigPartitions) {
//      if (fds.nonEmpty) {
//        val partitionBV = sc.broadcast(partition)
//        val fdsList = fds.toList
//        val failFD = sc.parallelize(fdsList).flatMap(fd =>
//          checkDependenciesInBig(partitionBV, fd)).collect().distinct.toList
//        failed ++= failFD
//        FDUtils.cutInSameLhs(fds, failFD)
//        partitionBV.unpersist()
//      }
//    }
//  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[(String, List[Array[String]])] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _)//.map(t => t._2).zipWithIndex()

    partitions
  }

  def checkDependenciesInSmall(fdsBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
                               dictBV: Broadcast[mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]],
                               partition: (String, List[Array[String]])
                              ): (List[(Set[Int], Int)], String, mutable.HashMap[Set[Int], Int]) = {
    val dict = mutable.HashMap.empty[Set[Int], Int]
    val map = dictBV.value.getOrElse(partition._1, mutable.HashMap.empty[Set[Int], Int])
    val failed = mutable.ListBuffer.empty[(Set[Int],Int)]
    val res = fdsBV.value.toList.map(fd =>
      FDUtils.check(partition._2, fd._1, fd._2, map))
    res.foreach(r => {
      failed ++= r._1
      dict.update(r._2._1, r._2._2)
    })
    (failed.toList.distinct, partition._1, dict)
  }

//  def checkDependenciesInBig(partitionBV: Broadcast[List[Array[String]]],
//                      fd: (Set[Int], mutable.Set[Int])): List[(Set[Int], Int)] = {
//    val res = FDUtils.check(partitionBV.value, fd._1, fd._2)
//    res
//  }

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
