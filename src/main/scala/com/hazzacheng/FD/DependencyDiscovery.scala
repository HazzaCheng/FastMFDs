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
  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]],
                  colSize: Int, orders: Array[(Int, Long)]): Map[Set[Int], mutable.Set[Int]] = {
    val dependencies = FDsUtils.getDependencies(colSize)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]

//    val temp = orders.head
//    orders(0) = orders.last
//    orders(orders.length - 1) = temp
    var sum = 0
    for (i <- orders) {
      time2 = System.currentTimeMillis()
      val candidates = FDsUtils.getCandidateDependencies(dependencies, i._1)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val dict = mutable.HashMap.empty[String, mutable.HashMap[Set[Int], Int]]

      val partitionsRDD = repart(sc, rdd, i._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var partitionsLocal: Array[List[Array[String]]] = Array()
      //if (i._2 <= 20) partitionsLocal = partitionsRDD.collect().map(x => x._2)
      for (k <- keys) {
        val ls = lhsAll(k)
        val fds = FDsUtils.getSameLhsFD(candidates, ls)
        println("========Size- lhs:"  + i + " " + k + " " + fds.size)
        println("========Size- depend:"  + i + " " + k + " " + FDsUtils.getDependenciesNums(fds))
        if (fds.nonEmpty) {
          sum += 1
          //val failed: ListBuffer[(Set[Int], Int)] = ListBuffer.empty
//          if (i._2 > 20) {
//            time1 = System.currentTimeMillis()
//            checkPartitions(sc, partitionsRDD, fds, failed, dict)
//            println("===========Paritions Small" + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))
//          } else {
//            time1 = System.currentTimeMillis()
//            checkLocalPartitions(sc, partitionsLocal, fds, failed, i._1, k)
//            println("===========Paritions Big" + i + " " + k + " Use Time=============" + (System.currentTimeMillis() - time1))
//          }
          val fdsBV = sc.broadcast(fds)
          val parallelDatas = partitionsRDD.flatMap(pair => parallelData(pair,fdsBV))
          val dictBV = sc.broadcast(dict)
          val info = parallelDatas.flatMap(pd => {
            FDsUtils.check(pd._1._2,pd._2._1,pd._2._2)
          }).collect().toList
          val failed = info.distinct
          time1 = System.currentTimeMillis()
          println("========Size- failed:"  + i + " " + k + " " + failed.size)
          if (failed.nonEmpty) {
            val leaves = cutLeaves(dependencies, candidates, failed, i._1)
            println("===========Size- reduce leaves " + i + " " + k + " " + leaves)
            println("===========Cut Leaves" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " + (System.currentTimeMillis() - time1))
          }
        }
      }

      results ++= candidates
      println("===========Common Attr" + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }
    println("============Tasks============" + sum + "======================")
    time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(sc, results)
    println("===========FindMinFD Use Time=============" + System.currentTimeMillis() + " " + time1 + " " +(System.currentTimeMillis() - time1))
    if (emptyFD.nonEmpty) results += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[(String, List[Array[String]])] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _)

    partitions
  }

  def parallelData(pair:(String, List[Array[String]]), fdsBV:Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]])
        :List[((String, List[Array[String]]), (Set[Int],mutable.Set[Int]))] = {
    val res = mutable.ListBuffer.empty[((String, List[Array[String]]), (Set[Int],mutable.Set[Int]))]
    fdsBV.value.foreach(fd => res.append((pair, fd)))
    res.toList
  }

  def checkPartitions(sc: SparkContext,
                      partitionsRDD: RDD[(String, List[Array[String]])],
                      fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
                      failed: ListBuffer[(Set[Int], Int)],
                      dict:mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]): Unit = {
    val failedFD = mutable.ListBuffer.empty[(Set[Int], Int)]
    val fdsBV = sc.broadcast(fds)
    val dictBV = sc.broadcast(dict)
    val tupleInfo = partitionsRDD.map(p =>{
      println("===Check fds: " + fds.size + "================")
      println("======Check Array Num:" + p._2.size + "=============")
      println("======Cur Time: " + System.currentTimeMillis() + "===========")
      val time = System.currentTimeMillis()
      val r = checkFDs(fdsBV, dictBV, p)
      println("=======Time: " + (System.currentTimeMillis() - time) + "================")
      r
    }).collect()
    dict.clear()
    tupleInfo.foreach(tuple => {
      dict.put(tuple._2, tuple._3)
      failedFD ++= tuple._1
    })
    failed ++= failedFD.toList.distinct
    fdsBV.unpersist()
    dictBV.unpersist()
  }

  def checkFDs(fdsBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
               dictBV: Broadcast[mutable.HashMap[String, mutable.HashMap[Set[Int], Int]]],
               partition: (String, List[Array[String]])
              ): (List[(Set[Int], Int)], String, mutable.HashMap[Set[Int], Int]) = {
    val dict = mutable.HashMap.empty[Set[Int], Int]
    val map = dictBV.value.getOrElse(partition._1, mutable.HashMap.empty[Set[Int], Int])
    val failed = mutable.ListBuffer.empty[(Set[Int],Int)]
    val res = fdsBV.value.toList.map(fd =>
      FDsUtils.checkEach(partition._2, fd._1, fd._2, map))
    res.foreach(r => {
      failed ++= r._1
      dict.put(r._2._1, r._2._2)
    })
    (failed.toList.distinct, partition._1, dict)
//    val res = FDUtils.checkAll(partition._2, fdsBV.value, map)
//    (res._1, partition._1, res._2)
  }

  def checkLocalPartitions(sc: SparkContext,
                           partitions: Array[List[Array[String]]],
                           fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
                           failed: ListBuffer[(Set[Int], Int)], i: Int, k: Int): Unit = {
    var j = 1
    for (partition <- partitions) {
      if (fds.nonEmpty) {
        val partitionBV = sc.broadcast(partition)
        val fdsList = fds.toList
        val fdsRDD = sc.parallelize(fdsList)
        println("====fds RDD partitions Num: " + fdsRDD.getNumPartitions + "=====")
        val failFD = fdsRDD.flatMap(fd =>{
          println("====Check fd: " + fd + "================")
          println("====Check Array Num: " + partitionBV.value.size + "======")
          val time = System.currentTimeMillis()
          println("====Cur Time: " + System.currentTimeMillis() + "=======")
          val r = checkFDs(partitionBV, fd)
          println("=======Time: " + (System.currentTimeMillis() - time) + "================")
          r
        }).collect().distinct.toList
        if (failFD.nonEmpty) {
          failed ++= failFD
          println("========Size- before cut same lhs:"  + i + " " + k + " " + j + " " + FDsUtils.getDependenciesNums(fds))
          FDsUtils.cutInSameLhs(fds, failFD)
          println("========Size- failed In Partition:"  + i + " " + k + " " + j + " " + failed.size)
          println("========Size- after cut same lhs:"  + i + " " + k + " " + j + " " + FDsUtils.getDependenciesNums(fds))
        }
        partitionBV.unpersist()
        j += 1
      }
    }
  }

  def checkFDs(partitionBV: Broadcast[List[Array[String]]],
               fd: (Set[Int], mutable.Set[Int])): List[(Set[Int], Int)] = {
    val res = FDsUtils.check(partitionBV.value, fd._1, fd._2)
    res
  }


//  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
//                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
//                failed: List[(Set[Int], Int)], commonAttr: Int) = {
//    for (d <- failed) {
//      val subSets = FDUtils.getSubsets(d._1.toArray)
//      for (subSet <- subSets) {
//        if (subSet contains commonAttr) FDUtils.cut(candidates, subSet, d._2)
//        else FDUtils.cut(dependencies, subSet, d._2)
//      }
//    }
//
//  }

  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                failed: List[(Set[Int], Int)], commonAttr: Int) :Int = {
    var sum = 0
    for (d <- failed) {
      val subSets = FDsUtils.getSubsets(d._1.toArray)
      for (subSet <- subSets) {
        if (subSet contains commonAttr) sum += FDsUtils.cut(candidates, subSet, d._2)
        else sum += FDsUtils.cut(dependencies, subSet, d._2)
      }
    }
    sum
  }



  def findMinFD(sc:SparkContext,
                fd:mutable.HashMap[Set[Int], mutable.Set[Int]]): Map[Set[Int], mutable.Set[Int]] = {
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
        if(FDsUtils.isSubset(fd._1, f._2._1)) f._2._2 --= fd._2
    }
    f._2
  }

}
