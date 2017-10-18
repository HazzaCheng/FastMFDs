package com.hazzacheng.FD

import com.hazzacheng.FD.DependencyDiscovery.{time1, time2}
import com.hazzacheng.FD.FDUtils.{takeAttrLHS, takeAttrRHS}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by Administrator on 2017/10/17.
  */
object DistributedFD {
  private val parallelScaleFactor = 4
  def findOnSpark(sc: SparkContext, rdd: RDD[Array[String]]): Map[Set[Int], mutable.Set[Int]] = {
    val nums = rdd.first().length
    val dependencies = FDUtils.getDependencies(nums)
    val emptyFD = mutable.Set.empty[Int]
    val results = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    val partitions = rdd.repartition(sc.defaultParallelism * parallelScaleFactor).cache()
    for (i <- 1 to nums) {
      val candidates = FDUtils.getCandidateDependencies(dependencies, i)
      val lhsAll = candidates.keySet.toList.groupBy(_.size)
      val keys = lhsAll.keys.toList.sortWith((x, y) => x > y)

      for (k <- keys) {
        val candidatesBV = sc.broadcast(candidates)
        val ls = lhsAll(k)
        val lsBV = sc.broadcast(ls)
        //val failedTemp = partitions.flatMap(p => checkDependencies(p, candidatesBV, lsBV)).collect()
        val failedTemp = partitions.mapPartitions(d => makeDistributedMidData(d,candidatesBV,lsBV).toIterator).reduceByKey((x,y) => checkDMData(x,y))
        .map(f => {
          f._2._2.clear()
          (f._1, candidatesBV.value(f._1) -- f._2._1)
        }).collect()

        val failed = ArrayBuffer.empty[(Set[Int],Int)]
        for(arr <- failedTemp){
          for(r <- arr._2){
            failed += arr._1 -> r
          }
        }
        cutLeaves(dependencies, candidates, failed.toArray, i)

      }
      results ++= candidates
    }
    partitions.unpersist()

    val minFD = DependencyDiscovery.findMinFD(sc, results)
    if (emptyFD.size > 0) results += (Set.empty[Int] -> emptyFD)

    minFD
  }

  def makeDistributedMidData(data:Iterator[Array[String]],
                             candidatesBV: Broadcast[mutable.HashMap[Set[Int], mutable.Set[Int]]],
                             lsBV: Broadcast[List[Set[Int]]]):
  mutable.HashMap[Set[Int],(mutable.Set[Int], mutable.HashMap[String, Array[String]])]={
    val result = mutable.HashMap.empty[Set[Int],(mutable.Set[Int], mutable.HashMap[String, Array[String]])]
    for (lhs <- lsBV.value) {
      val existed = candidatesBV.value.get(lhs)
      if (existed.isDefined) {
        val rs = existed.get
        val rTuple = (rs, mutable.HashMap.empty[String, Array[String]])
        result += lhs -> rTuple
      }
    }
    data.foreach(t => {
      for (lhs <- lsBV.value) {
        val existed = candidatesBV.value.get(lhs)
        if (existed.isDefined) {
          val rTuple = result(lhs)

          val rhs = rTuple._1
          val dict = rTuple._2

          val left = takeAttrLHS(t, lhs.toList)
          val right = takeAttrRHS(t, rhs.toList)

          if (dict.contains(left)) {
            for (i <- rhs) {
              if (!dict(left)(i).equals(right(i))) {
                rhs -= i
              }
            }
          }
          else dict += left -> right

        }
      }
    })

    result
  }

  def checkDMData(x:(mutable.Set[Int], mutable.HashMap[String, Array[String]]),
                  y:(mutable.Set[Int], mutable.HashMap[String, Array[String]])):
                  (mutable.Set[Int], mutable.HashMap[String, Array[String]]) = {
    x._1 --= y._1
    y._2.foreach(hm => {
      if(x._2.contains(hm._1)){
        for(i <- x._1){
          if(x._2(hm._1)(i) != hm._2(i)){
            x._1 -= i
          }
        }
      }
      else x._2 += hm
    })
    x
  }



  def cutLeaves(dependencies: mutable.HashMap[Set[Int], mutable.Set[Int]],
                candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                failed: Array[(Set[Int], Int)], commonAttr: Int) = {
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
