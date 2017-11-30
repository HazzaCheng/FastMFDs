package com.hazzacheng.FD.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def getDataFrameFromCSV(ss: SparkSession, filePath: String): DataFrame = {
    val df = ss.read.csv(filePath)

    df
  }

  def getColSize(df: DataFrame): Int = {
    val colSize = df.first.length

    colSize
  }

  def getNewDF(filePath: String, df: DataFrame, del: Set[Int]): DataFrame = {
    val cols = df.columns.zipWithIndex.filter(x => !del.contains(x._2 + 1)).map(_._1)
    val newDF = df.select(cols.head, cols.tail: _*)

    newDF
  }

  private def getSingleLhsCount(df: DataFrame, colSize: Int): List[(Int, Int)] = {
    val res = df.columns.map(col => df.groupBy(col).count().count())
      .zipWithIndex.map(x => (x._2 + 1, x._1.toInt))

    res.toList
  }

  def getTwoAttributesCount(df: DataFrame, colSize: Int): List[((Int, Int), Int)] = {
    val columns = df.columns
    val tuples = mutable.ListBuffer.empty[((Int, Int), (String, String))]

    for (i <- 0 until (colSize - 1))
      for (j <- (i + 1) until colSize)
        tuples.append(((i + 1, j + 1), (columns(i), columns(j))))

    val res = tuples.toList.map(x =>
      (x._1, df.groupBy(x._2._1, x._2._2).count().count().toInt))

    res
  }

  def getBottomFDs(df: DataFrame,
                   colSize: Int
                  ): (Array[(Int, Int)], List[(Int, Int)], List[((Int, Int), Int)]) = {
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

    (fds, lhs, whole)
  }

  def getTopFDs(attrsCountMap: mutable.HashMap[Set[Int], Int],
                df: DataFrame,
                topCandidates: mutable.Set[(Set[Int], Int)]
               ): (mutable.Set[(Set[Int], Int)], Array[(Set[Int], Int)]) = {
    val cols = df.columns

    val (topFDs, wrong) = topCandidates.partition{x =>
      val lhs = x._1.map(y => cols(y - 1)).toArray
      val whole = df.groupBy(cols(x._2 - 1), lhs: _*).count().count()
      val left = df.groupBy(lhs.head, lhs.tail: _*).count().count()

      attrsCountMap.put(x._1, left.toInt)
      attrsCountMap.put(x._1 + x._2, whole.toInt)

      whole == left
    }

    (topFDs, wrong.toArray)
  }

  def getAttrsCount(df: DataFrame, attrs: Set[Int]): Int = {
    val time0 = System.currentTimeMillis()
    val cols = df.columns
    val attrsCols = attrs.toArray.map(x => cols(x - 1))
    val count = df.groupBy(attrsCols.head, attrsCols.tail: _*).count().count()
    //println("====Test: groupBy size: " + attrs.size + " time: " + (System.currentTimeMillis() - time0))
    count.toInt
  }

  def getMinimalFDs(df: DataFrame,
                    toChecked: mutable.HashMap[Set[Int], mutable.Set[Int]],
                    lessAttrsCountMap: mutable.HashMap[Set[Int], Int]
                   ): Array[(Set[Int], Int)] = {
    val biggerMap = mutable.HashMap.empty[Set[Int], Int]
    val fds = toChecked.toList.flatMap(x => x._2.map((x._1, _))).toArray
    //println("=====fd all: " + fds.length + " " + fds.toList.toString())
    val minimalFDs = fds.filter{fd =>
      val lhs = lessAttrsCountMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
      val whole = biggerMap.getOrElseUpdate(fd._1 + fd._2, getAttrsCount(df, fd._1 + fd._2))
      //println("====fds: " + fd.toString() + " lhs" + lhs + " whole " + whole)
      lhs == whole
    }

    lessAttrsCountMap.clear()
    lessAttrsCountMap ++= biggerMap

    minimalFDs
  }

  def getFailFDs(df: DataFrame,
                 toChecked: mutable.HashMap[Set[Int], mutable.Set[Int]],
                 moreAttrsCountMap: mutable.HashMap[Set[Int], Int],
                 topFDs: mutable.Set[(Set[Int], Int)]
                ): Array[(Set[Int], Int)] = {
    val smallerMap = mutable.HashMap.empty[Set[Int], Int]
    val fds = toChecked.flatMap(x => x._2.map((x._1, _))).toArray

    val failFDs = fds.filter{fd =>
      val lhs = smallerMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
      val whole = moreAttrsCountMap.getOrElseUpdate(fd._1 + fd._2, getAttrsCount(df, fd._1 + fd._2))
      lhs != whole
    }

    val rightFDs = fds.toSet -- failFDs
    topFDs ++= rightFDs

    moreAttrsCountMap.clear()
    moreAttrsCountMap ++= smallerMap

    failFDs
  }



}
