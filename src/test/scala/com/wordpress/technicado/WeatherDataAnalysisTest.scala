package com.wordpress.technicado

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class WeatherDataAnalysisTest extends FunSpec with BeforeAndAfterAll {

  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var inputRDD: RDD[String] = _
  var weatherDataAnalysis: WeatherDataAnalysis = _
  var inPath: String = _
  var yearAndTempRDD: RDD[(String, String, Int)] = _
  var cleanYearAndTempRDD: RDD[(String, String, Int)] = _

  override def beforeAll(): Unit = {
    sparkConf = new SparkConf
    sparkConf.setMaster("local")
    sparkConf.setAppName("WeatherDataAnalysisTest")
    sparkContext = SparkContext.getOrCreate(sparkConf)
    weatherDataAnalysis = new WeatherDataAnalysis
    inPath = "file://" + new File("src/test/resources/1901.txt").getAbsolutePath
    inputRDD = sparkContext.textFile(inPath)
    yearAndTempRDD = weatherDataAnalysis.extractYearAndTemp(inputRDD)
    cleanYearAndTempRDD = weatherDataAnalysis.filterYearAndTemp(yearAndTempRDD)
  }

  override def afterAll(): Unit = {
    sparkConf = null
    sparkContext = null
    inputRDD = null

  }


  describe("WeatherDataAnalysisTest") {

    it("should filterYearAndTemp") {
      val data = Seq(("1901","1",-78), ("1901","1",-72), ("1901","1",-94))
      val expectedRDD = sparkContext.parallelize(data)
      assertResult(expectedRDD.collect)(cleanYearAndTempRDD.collect)
    }

    it("should extract Year And Temp") {
      val data = Seq(("1901","1",-78), ("1901","1",-72), ("1901","1",-94), ("1901","1",9999), ("1901","2",-56))
      val expectedRDD = sparkContext.parallelize(data)
      assertResult(expectedRDD.collect)(yearAndTempRDD.collect)
    }

  }
}
