package com.wordpress.technicado

import com.wordpress.technicado.Constants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FindMinimumTemperatures{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length != 0){
      println("USAGE: spark-submit --class com.wordpress.technicado.FindMinimumTemperatures " +
        "--master local[*] jars/weather-data-analysis-spark_2.11-0.1.jar ")
      System.exit(-1)
    }

    //read necessary properties
    ConfigUtil.readConfig("conf/weather-data.properties")

    // create spark context
    val sparkConf = new SparkConf
    sparkConf.setAppName(ConfigUtil.getString(FMT_JOB_NAME))
    val sparkContext = new SparkContext(sparkConf)

    val yearTemp: RDD[(String, Int)] = new WeatherDataAnalysis().process(sparkContext)

    println("yearly minimum temparatues are as follows: ")
    yearTemp.sortBy(_._2).collect.foreach(println)
  }
}
