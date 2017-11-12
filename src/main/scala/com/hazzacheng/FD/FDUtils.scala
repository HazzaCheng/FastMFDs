package com.hazzacheng.FD

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-09-28
  * Time: 8:34 PM
  */
object FDUtils {
  def getColSizeAndOreders(ss: SparkSession, filePath: String): (Int, Array[(Int, Long)]) = {
    val df = ss.read.csv(filePath)
    val colSize = df.first.length
    val orders = df.columns.map(col => df.groupBy(col).count().count())
      .zipWithIndex.sortWith((x, y) => x._1 > y._1)
    df.unpersist()
    (colSize, orders.map(x => (x._2 + 1, x._1)))
  }

  def createNewColumnName(colSize: Int): Array[String] = {
    val names = new Array[String](colSize)
    Range(0, colSize).foreach(i => names(i) = (i + 1).toString)

    names
  }



  def readAsRdd(sc: SparkContext, filePath: String): RDD[Array[String]] = {
    sc.textFile(filePath, sc.defaultParallelism * 4)
      .map(line => line.split(",").map(word => word.trim()))
  }

  def readAsRddWithIndex(sc: SparkContext,
                         filePath: String): ((RDD[(Array[String], Long)]), Int) = {
    val rdd = sc.textFile(filePath, sc.defaultParallelism * 4)
      .map(line => line.split(",").map(word => word.trim()))
      .zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
    val colSize = rdd.first()._1.size

    (rdd, colSize)
  }


  def outPutFormat(minFD: Map[Set[Int], mutable.Set[Int]]): List[String] = {
    minFD.map(d => d._1.toList.sorted.map(x => "column" + x).mkString("[", ",", "]")
    + ":" + d._2.toList.sorted.map(x => "column" + x).mkString(",")).toList
  }

  def outPutFormat2(minFD: Map[Set[Int], List[Int]]): List[String] = {
    minFD.map(d => d._1.toList.sorted.map(x => "column" + x).mkString("[", ",", "]")
      + ":" + d._2.sorted.map(x => "column" + x).mkString(",")).toList
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

  def getLevelFD(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                 ls: List[Set[Int]]): ListBuffer[(Set[Int], Int)] = {
    val fds = ListBuffer.empty[(Set[Int], Int)]
    for (lhs <- ls) {
      val rs = candidates.get(lhs)
      if (rs.isDefined) rs.get.toList.foreach(rhs => fds.append((lhs, rhs)))
    }
    fds
  }

  def getSameLhsFD(candidates: mutable.HashMap[Set[Int], mutable.Set[Int]],
                   ls: List[Set[Int]]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    val sameLHS = mutable.HashMap.empty[Set[Int], mutable.Set[Int]]
    for (lhs <- ls) {
      val rs = candidates.getOrElse(lhs, null)
      if (rs != null && rs.nonEmpty) sameLHS.put(lhs, rs)
    }
    sameLHS
  }

  def cutInSameLhs(sameLHS: mutable.HashMap[Set[Int], mutable.Set[Int]],
                   failFD: List[(Set[Int], Int)]): mutable.HashMap[Set[Int], mutable.Set[Int]] = {
    failFD.foreach(fd => cut(sameLHS, fd._1, fd._2))
    sameLHS
  }

  def takeAttrLHS(arr: Array[String], attributes: Set[Int]): String = {
    val s = mutable.StringBuilder.newBuilder
    attributes.foreach(attr => s.append(arr(attr - 1) + " "))

    s.toString()
  }

  def takeAttrRHS(arr: Array[String], attributes: mutable.Set[Int]): Array[String] = {
    val res = new Array[String](16)
    attributes.foreach(attr => res(attr) = arr(attr - 1))
    res
  }

  def checkAll(lines: List[Array[String]],
               fds: mutable.HashMap[Set[Int], mutable.Set[Int]],
               dict: mutable.HashMap[Set[Int], Int]
              ): (List[(Set[Int], Int)], mutable.HashMap[Set[Int], Int]) = {
    val lhsMap = mutable.HashMap.empty[Set[Int], Int]
    val all = getFdFromMap(fds)
    val allLhs = fds.keys.toList
    //val distinct = all.groupBy(x => x._1 + x._2)
    val distinct = all.map(x => x._1 + x._2).distinct
    val (existed, nonExisted) = distinct.partition(x => dict.contains(x))
    val lhsCount = getCounts(lines, allLhs, nonExisted, dict)
    lhsCount.foreach(i => lhsMap.put(i._1, i._2))
    val failFD = all.filter(i => lhsMap(i._1) != dict(i._1 + i._2))

    (failFD, lhsMap)
  }

  def getCounts(lines: List[Array[String]],
                allLhs: List[Set[Int]],
                nonExisted: List[Set[Int]],
                dict: mutable.HashMap[Set[Int], Int]): List[(Set[Int], Int)] = {
    val counters1 = new Array[Int](allLhs.length)
    val counters2 = new Array[Int](nonExisted.length)
    val sets1 = allLhs.zipWithIndex
      .map(x => (x._2, x._1, mutable.HashSet.empty[String]))
    val sets2 = nonExisted.zipWithIndex
      .map(x => (x._2, x._1, mutable.HashSet.empty[String]))

    lines.foreach(line => {
      sets1.foreach(i => {
        val k = getKeyString(line, i._2.toList)
        if (!i._3.contains(k)) {
          i._3.add(k)
          counters1(i._1) += 1
        }
      })
      sets2.foreach(i => {
        val k = getKeyString(line, i._2.toList)
        if (!i._3.contains(k)) {
          i._3.add(k)
          counters2(i._1) += 1
        }
      })
    })
    sets2.foreach(i => dict.put(i._2, counters2(i._1)))
    val res = sets1.map(i => (i._2, counters1(i._1)))

    res
  }

  def getFdFromMap(fds: mutable.HashMap[Set[Int], mutable.Set[Int]]
                  ): List[(Set[Int], Int)] = {
    fds.toList.flatMap(fs => fs._2.toList.map(fd => (fs._1, fd)))
  }

  def checkEach(lines: List[Array[String]],
            lhs: Set[Int],
            rhs: mutable.Set[Int],
            dict: mutable.HashMap[Set[Int], Int]
           ): (List[(Set[Int],Int)], (Set[Int], Int)) = {

    val (existed, nonExisted) = rhs.partition(r => dict.contains(lhs + r))
    val (lhsSize, wrong) = getDistinctSizeAndWrong(lines, dict, lhs.toList, nonExisted.toList)
    val failRhs = wrong ++ existed.toList.filter(r => dict(lhs + r) != lhsSize)
    val failFD = failRhs.map(r => (lhs, r))

    (failFD, (lhs, lhsSize))
  }

  def getDistinctSizeAndWrong(lines: List[Array[String]],
                              dict: mutable.HashMap[Set[Int], Int],
                              lhs: List[Int],
                              nonExist: List[Int]): (Int, List[Int]) = {
    val set = mutable.HashSet.empty[String]
    var counter = 0
    val sets = nonExist.zipWithIndex.map(i =>
      (i._2, i._1, mutable.HashSet.empty[String]))
    val counters = new Array[Int](nonExist.length)
    lines.foreach{line =>
      val key = getKeyString(line, lhs)
      if (!set.contains(key)) {
        counter += 1
        set.add(key)
      }
      sets.foreach{i =>
        val k = getKeyString(line, i._2 :: lhs)
        if (!i._3.contains(k)) {
          counters(i._1) += 1
          i._3.add(k)
        }
      }
    }
    sets.foreach(i =>dict.update(lhs.toSet + i._2, counters(i._1)))
    val wrong = sets.filter(i => counter != counters(i._1)).map(_._2)
    (counter, wrong)
  }

  def getKeyString(line: Array[String], indexs: List[Int]): String = {
    val sb = StringBuilder.newBuilder
    indexs.sorted.foreach(i => sb.append(line(i - 1) + " "))
    sb.toString()
  }

  def check(data: List[Array[String]],
            lhs: Set[Int],
            rhs: mutable.Set[Int]): List[(Set[Int],Int)] ={
    //val lSize = data.map(d => (FDUtils.takeAttributes(d, lhs),d)).groupBy(_._1).size
    val res = mutable.Set.empty[Int]
    val true_rhs = rhs.clone()
    val dict = mutable.HashMap.empty[String, Array[String]]
    data.foreach(d => {
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

    res.map(rhs => (lhs, rhs)).toList
  }


//  def cut(map: mutable.HashMap[Set[Int], mutable.Set[Int]],
//          lhs: Set[Int], rhs: Int) = {
//    val ot = map.get(lhs)
//    if (ot.isDefined) {
//      val v = ot.get
//      if (v contains rhs) {
//        if (v.size == 1) map -= lhs
//        else {
//          v -= rhs
//          map.update(lhs, v)
//        }
//      }
//    }
//  }

  def cut(map: mutable.HashMap[Set[Int], mutable.Set[Int]],
          lhs: Set[Int], rhs: Int): Int = {
    var sum = 0
    val ot = map.get(lhs)
    if (ot.isDefined) {
      val v = ot.get
      if (v contains rhs) {
        if (v.size == 1){
          map -= lhs
          sum += 1
        }
        else {
          v -= rhs
          sum += 1
          map.update(lhs, v)
        }
      }
    }
    sum
  }

  def isSubset(x:Set[Int], y:Set[Int]):Boolean = {
    if(x.size >= y.size) false
    else{
      if(x ++ y == y)true
      else false
    }
  }

  def getDependenciesNums(fds: mutable.HashMap[Set[Int], mutable.Set[Int]]): Int = {
    var count = 0;
    fds.toList.foreach(x => count += x._2.size)
    count
  }


}
