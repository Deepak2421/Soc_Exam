//import java.io.File
import java.sql.DriverManager
import com.crealytics.spark.excel._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SparkSession, expressions, _}
import org.apache.spark.{SparkConf, SparkContext, _}

//import org.apache.spark.{SparkConf}

object Poverty{
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

    val sc = new SparkContext(conf)


    val spark = SparkSession
      .builder()

      .appName("Aadhar Authentication")

      .master("local")

      .getOrCreate()
    import spark.implicits._
    val inputDF = spark.read.format("com.crealytics.spark.excel")
      .option("header","true")
      .load("C:\\Users\\Deepak\\Downloads\\scala-exercise-questions\\problem1\\PovertyEstimate1s.xls")
    val concat_DF= inputDF.withColumn("Area_name",concat(col("Area_name"),lit(" "),
      col("State")))
      val view_Concat = concat_DF.createOrReplaceTempView("Concatenated_Area_State")
    val odd = spark.sql("SELECT * FROM Concatenated_Area_State WHERE Urban_Influence_Code_2003 IN(SELECT Urban_Influence_Code_2003 FROM Concatenated_Area_State WHERE Urban_Influence_Code_2003%2 <> 0)").toDF()
    val view_Odd = odd.createOrReplaceTempView("Odd_UrbanInfluence")
    val even = spark.sql("SELECT * FROM Odd_UrbanInfluence WHERE Rural_urban_Continuum_Code_2013 IN(SELECT Rural_urban_Continuum_Code_2013 FROM Odd_UrbanInfluence WHERE Rural_urban_Continuum_Code_2013 % 2 = 0)")
    val view_Even = even.createOrReplaceTempView("Even_urban_Continum")
    val result_Table = spark.sql("Select State,Area_name,Urban_Influence_Code_2003,Rural_urban_Continuum_Code_2013 from Even_urban_Continum").show()


  }}