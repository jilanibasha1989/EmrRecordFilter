package com.automation

import org.apache.spark.api.java.JavaSparkContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import java.text._
import java.io._
import java.util.TimeZone
import org.apache.spark.sql.functions.col

import scala.io.Source

import scala.io.Source.fromFile

object RecordFilter {

  var argsMap = new LinkedHashMap[String, String]

  def createS3OutputFile(spark: SparkSession, fileNamePattern: String, inputFileLoc: String, outputFileLoc: String, filter_required: String, filterCol: List[String], dateModifier: String) {

    import spark.implicits._

    //cst to gmt time zone convert UDF
    val func = udf(convertToGmt(_: String));

    // use s3n ! as input and write  output  s3 location
    val fileDf = spark.read.format("csv").option("header", "true").load(inputFileLoc)

    //collecting the columns from the Dataframe
    val collectCols = fileDf.columns.seq

    //
    if (filter_required == "N") {

      val cnvrtToGmtDF = fileDf.withColumn("newdatetimecol", func(col(dateModifier)))

      val updatedSchemeDF = updateSchema(cnvrtToGmtDF, collectCols, dateModifier)

      updatedSchemeDF.write.format("parquet").mode("append").option("compression", "snappy").save(outputFileLoc)

    } else {

      val filterRecordDf = filterRecord(fileDf, filterCol)

      val cnvrtToGmtDF = filterRecordDf.withColumn("newdatetimecol", func(col(dateModifier)))

      val updatedSchemeDF = updateSchema(cnvrtToGmtDF, collectCols, dateModifier)

      cnvrtToGmtDF.write.format("parquet").mode("append").option("compression", "snappy").save(outputFileLoc)
    }
    // cnvrtToGmtDF.select("")

    spark.stop()
  }

  //function to filter out the rows from the data and return dataframe
  def filterRecord(df: DataFrame, strList: List[String]): DataFrame = {

    import df.sparkSession.implicits._

    val filiterDf = df.filter(!col("SiteName").isin(strList: _*))

    return df;
  }

  def updateSchema(df: DataFrame, collectCols: IndexedSeq[String], dateColumn: String): DataFrame = {

    import df.sparkSession.implicits._

    val removeColumnFromList = collectCols.filter(_ == dateColumn).toArray
    val finalColumns = collectCols.toArray

    val removedColumnFilterDf = df.select(removeColumnFromList.map(col _): _*)

    val outputData = removedColumnFilterDf.withColumn(dateColumn, col("newdatetimecol")).select(finalColumns.map(col _): _*)

    return outputData;
  }

  // function which converts the given timestamp from cst to GMT
  def convertToGmt(str: String): String = {

    // input date Format
    val sourceFormat = new SimpleDateFormat("MM/dd/yy HH:mm");

    //setting up the Input date format as CST
    val cstTime = TimeZone.getTimeZone("CST");
    sourceFormat.setTimeZone(cstTime);

    //out put date format
    val gmtFormat = new SimpleDateFormat("MM/dd/yy HH:mm");
    val gmtTime = TimeZone.getTimeZone("GMT+00");

    //parsing the input date format to CST time zone
    val date1 = sourceFormat.parse(str.toString());

    //setting up the Date output format
    gmtFormat.setTimeZone(gmtTime);
    System.out.println("gmt:" + gmtFormat.format(date1));

    //return the final dateFormat
    return gmtFormat.format(date1);
  }

  //Main function which execute the program
  def main(args: Array[String]): Unit = {

    //configuring the spark setup
    val spark = SparkSession
      .builder
      .appName("RecorFilter")
      .master("local")
      .getOrCreate()

    //setup configuration
    spark.conf.set("spark.sql.shuffle.partitions", 1);

    // loading the properties files from resource folder
    val properties = this.getClass().getClassLoader().getResourceAsStream("config.properties")
    val propRecords = Source.fromInputStream(properties).getLines()

    //executing the files loading process
    propRecords.filter(_.contains("#")).foreach {
      lines =>
        //spliting the records into arrays to collect key and value
        val data = lines.split("#")

        //collecting the variable from properties file
        val fileVar = data(1).split(":")

        //reading the params to variables
        val sourcedata = data(0)
        val fileNamePattern = fileVar(0).split("=")(1)
        val inputFileLocation = fileVar(1).split("=")(1)
        val filterrequired = fileVar(2).split("=")(1)
        val filterColunn = fileVar(3).split("=")(1).split(",").toList
        val dateModifier = fileVar(4).split("=")(1)
        val outputFileLocation = fileVar(5).split("=")(1)

        println("soruce data : = " + sourcedata + " inputFileLocation : = " + inputFileLocation + " filterColunn : =  " + fileVar(1).split("=")(1) + " dateModifier :=  " + dateModifier + " outputFileLocation : = " + outputFileLocation)

        createS3OutputFile(spark, fileNamePattern, inputFileLocation, outputFileLocation, filterrequired, filterColunn, dateModifier)
    }

  }

}