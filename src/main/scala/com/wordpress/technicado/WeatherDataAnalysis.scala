package com.wordpress.technicado

import com.wordpress.technicado.Constants.FMT_INPUT_DATA
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WeatherDataAnalysis extends Serializable {

  val MISSING = 9999

  def process(sparkContext: SparkContext): RDD[(String, Int)] = {
    val inputData: RDD[String] = sparkContext.textFile(ConfigUtil.getString(FMT_INPUT_DATA))

    val yearAndTempRDD: RDD[(String, String, Int)] = extractYearAndTemp(inputData)

    val cleanYearAndTempRDD: RDD[(String, Int)] = filterYearAndTemp(yearAndTempRDD)
      .map(x => (x._1, x._3))

    cleanYearAndTempRDD.reduceByKey((a, b) => if(a > b) b else a)
  }

  def filterYearAndTemp(inputData: RDD[(String, String, Int)]): RDD[(String, String, Int)] = {
    inputData.filter(x => {
      x._3 != MISSING && x._2.matches("[01459]")
    })
  }

  def extractYearAndTemp(inputRDD: RDD[String]): RDD[(String, String, Int)] = {
    inputRDD.map(line => {
      val year = line.substring(15, 19)
      val quality = line.substring(92, 93)

      var airTemperature = 0
      if (line.charAt(87) == '+') // parseInt doesn't like leading plus signs
        airTemperature = line.substring(88, 92).toInt
      else airTemperature = line.substring(87, 92).toInt

      (year, quality, airTemperature)
    })
  }
}
