package com.hazzacheng.FD.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

/*  def getDataFrameFromCSV(ss: SparkSession,
                          filePath: String,
                          tmpFilePath: String
                         ): (DataFrame, Int, String) = {
    val sc = ss.sparkContext

    var temp = tmpFilePath
    if (!tmpFilePath.endsWith("/")) temp += "/"
    temp += System.currentTimeMillis()

    val time = System.currentTimeMillis()
    val rdd = sc.textFile(filePath).distinct().persist()
    val words = rdd.flatMap(_.split(",")).distinct().zipWithIndex().collect().sortBy(_._2)
    println("===== Words: " + words.length)
    val words2index = mutable.HashMap.empty[String, Long]
    words.foreach(x => words2index.put(x._1, x._2))
    val words2indexBV = sc.broadcast(words2index)
    rdd.map(x => x.split(",").map(words2indexBV.value(_)).mkString(","))
    rdd.saveAsTextFile(temp)
    rdd.unpersist()
    println("===== Save File: " + (System.currentTimeMillis() - time) + "ms")

    var df = ss.read.csv(temp).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val colSize = df.first.length

    (df, colSize, temp)
  }*/

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
//    val colSize = rdd.first().split(",").length
    val words = rdd.flatMap(_.split(",")).distinct().zipWithIndex().collect()
      .sortBy(_._2).map(x => (x._1, x._2.toInt))
    println("===== Words: " + words.length)
    val words2index = mutable.HashMap.empty[String, Int]
    words.foreach(x => words2index.put(x._1, x._2))
    val words2indexBV = sc.broadcast(words2index)
    rdd.map(x => x.split(",").map(words2indexBV.value(_)).mkString(",")).saveAsTextFile(temp)
    /*val rowRDD = rdd.map(x => Row.fromSeq(x.split(",").map(words2indexBV.value(_))))
    val schema = createSchema(colSize)
    val df = castColumnTo(ss.createDataFrame(rowRDD, schema), IntegerType)
      .repartition(numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    if (df.first().length != colSize) println("====== WRONG!!!!")*/
    rdd.unpersist()
    println("===== Save File: " + (System.currentTimeMillis() - time) + "ms")

//    val df = castColumnTo(ss.read.csv(temp), IntegerType).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val df = ss.read.csv(temp).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val colSize = df.first.length

    (df, colSize, temp)
  }

  def dfToRdd(df: DataFrame): RDD[Array[Int]] = {
    val len = df.columns.length
    val rdd = df.rdd.map(r => Range(0, len).map(r.getString(_).toInt).toArray)

    rdd
  }

  def createSchema(colSize: Int): StructType = {
    var schema = new StructType()

    Range(0, colSize).foreach(x => schema = schema.add("c" + x, IntegerType, true))

    schema
  }

  def castColumnTo(df: DataFrame, tpe: DataType): DataFrame = {
    val cols = df.columns
    var temp = df

    cols.foreach(x => temp = temp.withColumn(x, temp(x).cast(tpe)))

    temp
  }

  def getSelectedDF(df: DataFrame, numPartitions:Int, del: Set[Int]): DataFrame = {
    val cols = df.columns.zipWithIndex.filter(x => !del.contains(x._2 + 1)).map(_._1)
    val newDF = df.select(cols.head, cols.tail: _*).distinct()//.repartition(numPartitions)

    newDF
  }

  private def getSingleLhsCount(df: DataFrame,
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

  def getBottomFDs(df: DataFrame,
                   colSize: Int,
                   allSame: mutable.HashSet[Int]
                  ): (Array[(Int, Int)], Map[Int, Int], List[((Int, Int), Int)]) = {
    val lhs = getSingleLhsCount(df, allSame)
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
                 topFDs: mutable.Set[(Set[Int], Int)],
                 rhsCount: mutable.Map[Int, Int]
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
      if (whole >= rhsCount(fd._2)) {
        val lhs = moreSmallerAttrsCountMap.getOrElseUpdate(fd._1, getAttrsCount(df, fd._1))
        lhs != whole
      } else true
    }

    val rightFDs = fds.toSet -- failFDs
    topFDs ++= rightFDs

    failFDs
  }

}
