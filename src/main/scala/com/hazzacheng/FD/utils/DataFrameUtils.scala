package com.hazzacheng.FD.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-11-26 
  * Time: 12:59 PM
  */
object DataFrameUtils {

  def getDataFrameFromCSV(ss: SparkSession,
                          numPartitions: Int,
                          filePath: String,
                          tmpFilePath: String
                         ): (DataFrame, Int, String) = {
    val sc = ss.sparkContext

    var temp = tmpFilePath
    if (!tmpFilePath.endsWith("/")) temp += "/"
    temp += System.currentTimeMillis()

    val time = System.currentTimeMillis()
    val rdd = sc.textFile(filePath).distinct().persist()
    val words = rdd.flatMap(_.split(",")).distinct().zipWithIndex()
      .collect().map(x => (x._1, x._2.toInt))
    //    val words = rdd.flatMap(_.split(",")).distinct().map(x => (x, x.hashCode)).collect()
    println("===== Words: " + words.length)
    val word2index = mutable.HashMap.empty[String, Int]
    words.foreach(x => word2index.put(x._1, x._2))
    val words2indexBV = sc.broadcast(word2index)
    rdd.map(x => x.split(",").map(words2indexBV.value(_)).mkString(",")).saveAsTextFile(temp)

    rdd.unpersist()
    println("===== Save File: " + (System.currentTimeMillis() - time) + "ms")

    val df = ss.read.csv(temp).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val colSize = df.first.length

    (df, colSize, temp)
  }

  def dfToRdd(df: DataFrame): RDD[Array[Int]] = {
    val len = df.columns.length
    val rdd = df.rdd.map(r => Range(0, len).map(r.getString(_).toInt).toArray)

    rdd
  }


  def getNewDF(df: DataFrame, numPartitions:Int, del: Set[Int]): DataFrame = {
    val cols = df.columns.zipWithIndex.filter(x => !del.contains(x._2 + 1)).map(_._1)
    val newDF = df.select(cols.head, cols.tail: _*)

    newDF
  }

  def getSelectedDF(ss: SparkSession,
                    df: DataFrame,
                    tmpFilePath: String,
                    numPartitions:Int,
                    notDel: Set[Int]): DataFrame = {
    var temp = tmpFilePath
    if (!tmpFilePath.endsWith("/")) temp += "/"
    temp += System.currentTimeMillis()

    val cols = df.columns.zipWithIndex.filter(x => notDel.contains(x._2 + 1)).map(_._1)
    df.select(cols.head, cols.tail: _*).distinct().write.csv(temp)//.repartition(numPartitions)

    val newDF = ss.read.csv(temp)

    newDF
  }

  private def getSingleColCount(df: DataFrame,
                                allSame: mutable.HashSet[Int]
                               ): Map[Int, Int] = {
    val (res, same) = df.columns.map(col => df.groupBy(col)
      .count()
      .count())
      .zipWithIndex.map(x => (x._2 + 1, x._1.toInt)).partition(_._2 != 1)

    same.foreach(x => allSame.add(x._1))

    res.toMap
  }

  def getTwoAttributesCount(df: DataFrame,
                            colSize: Int,
                            singleLhsCount :Map[Int, Int],
                            allSame: mutable.HashSet[Int]
                           ): List[((Int, Int), Int)] = {
    val columns = df.columns
    val tuples = mutable.ListBuffer.empty[((Int, Int), (String, String))]

    for (i <- 0 until (colSize - 1) if !allSame.contains(i + 1))
      for (j <- (i + 1) until colSize if !allSame.contains(j + 1))
        tuples.append(((i + 1, j + 1), (columns(i), columns(j))))

    val res = tuples.toList.map(x =>
      (x._1, df.groupBy(x._2._1, x._2._2)
        .count()
        .count().toInt))

    res
  }

  def getMinimalFDsOffset(df: DataFrame,
                          toChecked: List[(Set[Int], mutable.Set[Int])],
                          lessAttrsCountMap: mutable.HashMap[Set[Int], Int],
                          lessBiggerAttrsCountMap: mutable.HashMap[Set[Int], Int],
                          rhsCount: mutable.Map[Int, Int],
                          offset: Int
                         ): Array[(Set[Int], Int)] = {
    val fds = toChecked.flatMap(x => x._2.map((x._1, _))).toArray

    if (lessBiggerAttrsCountMap.nonEmpty
      && fds.last._1.size == lessBiggerAttrsCountMap.last._1.size) {
      lessAttrsCountMap.clear()
      lessAttrsCountMap ++= lessBiggerAttrsCountMap
      lessBiggerAttrsCountMap.clear()
    }

    val minimalFDs = fds.filter{fd =>
      val lhs = lessAttrsCountMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
      if (lhs >= rhsCount(fd._2 + offset)) {
        val whole = lessBiggerAttrsCountMap.getOrElseUpdate(fd._1 + fd._2, getAttrsCount(df, fd._1 + fd._2))
        lhs == whole
      } else false
    }

    minimalFDs.map(x => (x._1.map(y => y + offset), x._2 + offset))
  }

  def getFailFDsOffset(df: DataFrame,
                       toChecked: List[(Set[Int], mutable.Set[Int])],
                       moreAttrsCountMap: mutable.HashMap[Set[Int], Int],
                       moreSmallerAttrsCountMap: mutable.HashMap[Set[Int], Int],
                       topFDs: mutable.Set[(Set[Int], Int)],
                       rhsCount: mutable.Map[Int, Int],
                       offset: Int
                      ): Array[(Set[Int], Int)] = {
    val fds = toChecked.flatMap(x => x._2.map((x._1, _))).toArray

    if (moreSmallerAttrsCountMap.nonEmpty
      && fds.last._1.size + 1 == moreSmallerAttrsCountMap.last._1.size) {
      moreAttrsCountMap.clear()
      moreAttrsCountMap ++= moreSmallerAttrsCountMap
      moreSmallerAttrsCountMap.clear()
    }

    val failFDs = fds.filter{fd =>
      val whole = moreAttrsCountMap.getOrElseUpdate(fd._1 + fd._2, getAttrsCount(df, fd._1 + fd._2))
      if (whole >= rhsCount(fd._2 + offset)) {
        val lhs = moreSmallerAttrsCountMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
        lhs != whole
      } else true
    }

    val rightFDs = fds.toSet -- failFDs
    topFDs ++= rightFDs.map(x => (x._1.map(y => y + offset), x._2 + offset))

    failFDs.map(x => (x._1.map(y => y + offset), x._2 + offset))
  }

  def getBottomFDs(df: DataFrame,
                   colSize: Int,
                   allSame: mutable.HashSet[Int]
                  ): (Array[(Int, Int)], Map[Int, Int], List[((Int, Int), Int)]) = {
    val lhs = getSingleColCount(df, allSame)
    val whole = getTwoAttributesCount(df, colSize, lhs, allSame)
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

    (fds, lhs, whole)
  }

  def check(p: List[Array[Int]], i: Int): Boolean = {
    val target = p.last(i)

    p.exists(x => x(i) != target)
  }

  def getTopFDs(moreAttrsCountMap: mutable.HashMap[Set[Int], Int],
                df: DataFrame,
                topCandidates: mutable.Set[(Set[Int], Int)],
                rhsCount: mutable.Map[Int, Int]
               ): (mutable.Set[(Set[Int], Int)], Array[(Set[Int], Int)]) = {
    val cols = df.columns

    val (topFDs, wrong) = topCandidates.partition{x =>

      val lhs = x._1.map(y => cols(y - 1)).toArray
      val whole = moreAttrsCountMap.getOrElseUpdate(x._1 + x._2,
        df.groupBy(cols(x._2 - 1), lhs: _*).count().count().toInt)
      if (whole >= rhsCount(x._2)) {
        val left = moreAttrsCountMap.getOrElseUpdate(x._1,
          df.groupBy(lhs.head, lhs.tail: _*).count().count().toInt)

        whole == left
      } else false

    }

    (topFDs, wrong.toArray)
  }



  def getAttrsCount(df: DataFrame, attrs: Set[Int]): Int = {
    val cols = df.columns
    val attrsCols = attrs.toArray.map(x => cols(x - 1))
    val count = df.groupBy(attrsCols.head, attrsCols.tail: _*).count().count()
    count.toInt
  }

  def getMinimalFDs(df: DataFrame,
                    toChecked: List[(Set[Int], mutable.Set[Int])],
                    lessAttrsCountMap: mutable.HashMap[Set[Int], Int],
                    lessBiggerAttrsCountMap: mutable.HashMap[Set[Int], Int],
                    rhsCount: mutable.Map[Int, Int]
                   ): Array[(Set[Int], Int)] = {
    val fds = toChecked.flatMap(x => x._2.map((x._1, _))).toArray

    if (lessBiggerAttrsCountMap.nonEmpty
      && fds.last._1.size == lessBiggerAttrsCountMap.last._1.size) {
      lessAttrsCountMap.clear()
      lessAttrsCountMap ++= lessBiggerAttrsCountMap
      lessBiggerAttrsCountMap.clear()
    }

    val minimalFDs = fds.filter{fd =>
      val lhs = lessAttrsCountMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
      if (lhs >= rhsCount(fd._2)) {
        val whole = lessBiggerAttrsCountMap.getOrElseUpdate(fd._1 + fd._2, getAttrsCount(df, fd._1 + fd._2))
        lhs == whole
      } else false
    }

    minimalFDs
  }

  def getFailFDs(df: DataFrame,
                 toChecked: List[(Set[Int], mutable.Set[Int])],
                 moreAttrsCountMap: mutable.HashMap[Set[Int], Int],
                 moreSmallerAttrsCountMap: mutable.HashMap[Set[Int], Int],
                 rhsCount: mutable.Map[Int, Int]
                ): (Array[(Set[Int], Int)], Array[(Set[Int], Int)]) = {
    val fds = toChecked.flatMap(x => x._2.map((x._1, _))).toArray

    if (moreSmallerAttrsCountMap.nonEmpty
      && fds.last._1.size + 1 == moreSmallerAttrsCountMap.last._1.size) {
      moreAttrsCountMap.clear()
      moreAttrsCountMap ++= moreSmallerAttrsCountMap
      moreSmallerAttrsCountMap.clear()
    }

    val failFDs = fds.filter{fd =>
      val whole = moreAttrsCountMap.getOrElseUpdate(fd._1 + fd._2, getAttrsCount(df, fd._1 + fd._2))
      if (whole >= rhsCount(fd._2)) {
        val lhs = moreSmallerAttrsCountMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
        lhs != whole
      } else true
    }

    val rightFDs = fds.toSet -- failFDs

    (failFDs, rightFDs.toArray)
  }

}
