package com.hazzacheng.FD

import com.hazzacheng.FD.FDUtils.{takeAttrLHS, takeAttrRHS}
import org.apache.spark.{Accumulator, SparkContext}
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

  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]]): Map[Set[Int], mutable.Set[Int]] = {
    val nums = rdd.first().length
    val dependencies = FDUtils.getDependencies(nums)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    //val nums1 = Array(2, 4, 3, 6, 7, 5, 8, 10, 9, 1)
    for (i <- 1 to nums) {
      time2 = System.currentTimeMillis()
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      time1 = System.currentTimeMillis()
      val partitions = repart(sc, rdd, i).collect().sortBy(_.length)
      println("===========Sort " + i + " Use Time=============" + (System.currentTimeMillis() - time1))
      //      println("===========Partitioner=============" + partitions.partitioner)
      //      println("===========Partitions " + i + " Size ============= Total" + partitions.count())
      //      partitions.map(p => p.length).collect().foreach(x => println("Size for " + i + " " + x))
      time1 = System.currentTimeMillis()
      if (partitions.length == 1) emptyFD += i
      println("===========Partitions " + i + "count Use Time=============" + (System.currentTimeMillis() - time1))

      for (k <- keys) {
        val ls = lhsAll(k)
        val fds = FDUtils.getSameLhsFD(candidates, ls)
        val failed: ListBuffer[(Set[Int], Int)] = ListBuffer.empty
        var flag = true
        for (partition <- partitions) {
          println("=====================My Partition Size============" + partition.size)
          if (fds.nonEmpty) {
            val partitionBV = sc.broadcast(partition)
            val f = fds.toList
            if(flag){
              println("Height:" + k + " || " + f.size)
              flag = false
            }
            val failFD = sc.parallelize(fds.toList)
              .flatMap(fd => checkDependency(partitionBV, fd)).collect().distinct
//

            FDUtils.cutInSameLhs(fds, failFD)
            failed ++= failFD
            partitionBV.unpersist() //pay attention
          }
        }

        time1 = System.currentTimeMillis()
        //val failed = failedTemp.distinct
        println("===========Distinct" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " +(System.currentTimeMillis() - time1))
        //        val failed = sc.parallelize(ls).flatMap(lhs => checkDependencies(partitions, candidatesBV, lhs)).collect()
        time1 = System.currentTimeMillis()
        cutLeaves(dependencies, candidates, failed.toList, i)
        println("===========Cut Leaves" + k + " Use Time=============" + System.currentTimeMillis() + " " + time1 + " " + (System.currentTimeMillis() - time1))
      }
      //      partitions.unpersist()
      results ++= candidates
      println("===========Common Attr" + i + " Use Time=============" + (System.currentTimeMillis() - time2))
    }

    time1 = System.currentTimeMillis()
    val minFD = DependencyDiscovery.findMinFD(sc, results)
    println("===========FindMinFD Use Time=============" + System.currentTimeMillis() + " " + time1 + " " +(System.currentTimeMillis() - time1))
    if (emptyFD.nonEmpty) results += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def OldFindOnSpark(sc: SparkContext, rdd: RDD[Array[String]]):Map[Set[Int], mutable.Set[Int]]={
    val nums = rdd.first().length
    val dependencies = FDUtils.getDependencies(nums)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (i <- 1 to nums) {
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)
      val partitions = repartOld(sc, rdd, i).cache()
      if(partitions.count() == 1)emptyFD += i
      for(k <- keys){
        val ls = lhsAll(k)
        val candidatesBV = sc.broadcast(candidates)
        val lsBV = sc.broadcast(ls)
        val failed = partitions.flatMap(p => checkDependencies(p,candidatesBV,lsBV)).collect().distinct.toList
        cutLeaves(dependencies,candidates,failed,i)
      }
      results ++= candidates
    }
    var minFD = DependencyDiscovery.findMinFD(sc, results)
    if (emptyFD.nonEmpty) minFD += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def repart(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      //.repartition(sc.defaultParallelism * parallelScaleFactor)
      .reduceByKey(_ ++ _).map(t => t._2)//.repartition(sc.defaultParallelism * parallelScaleFactor)


    partitions
  }

  def repartOld(sc: SparkContext, rdd: RDD[Array[String]], attribute: Int): RDD[List[Array[String]]] = {
    val partitions = rdd.map(line => (line(attribute - 1), List(line)))
      .reduceByKey(_ ++ _).map(t => t._2).repartition(sc.defaultParallelism * parallelScaleFactor)


    partitions
  }

  def checkDependency(partitionBV: Broadcast[List[Array[String]]],
                      fd: (Set[Int], mutable.Set[Int])): List[(Set[Int], Int)] = {
    val res = mutable.Set.empty[Int]
    var true_rhs = fd._2.toSet
    val lhs = fd._1.toList
    val rhs = fd._2.toList
    val dict = mutable.HashMap.empty[String, Array[String]]
    partitionBV.value.foreach(d => {
      val left = takeAttrLHS(d, lhs)
      val right = takeAttrRHS(d, rhs)
      if(dict.contains(left)){
        for(i <- true_rhs){
          if(!dict(left)(i).equals(right(i))){
            true_rhs -= i
            res += i
          }
        }
      }
      else dict += left -> right
    })

    res.map(x => (fd._1, x)).toList
  }

  def check(d:Array[String], lhs:List[Int], rhs:Int, dict:mutable.HashMap[String, String],isWrong: Accumulator[Int])={

    val left = takeAttrLHS(d, lhs)
    val right = takeAttrRHS(d, rhs)
    if(dict.contains(left)){
      if(!dict(left).equals(right)){
        isWrong.add(1)
      }
    }
    else dict += left -> right

  }

    def checkDependencies(p: List[Array[String]],
                          candidatesBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
                          lsBV: Broadcast[List[Set[Int]]]): List[(Set[Int], Int)] = {
      println("===========My Size=============" + p.length)
      val failed = new ListBuffer[(Set[Int], Int)]()
      for (lhs <- lsBV.value) {
        val existed = candidatesBV.value.get(lhs)
        if (existed != None) {
          val rs = existed.get.toList
          val fail = FDUtils.check(p, lhs.toList, rs).map(rhs => (lhs, rhs))
          failed ++= fail
        }
      }

      failed.toList
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
