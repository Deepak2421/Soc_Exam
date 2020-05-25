import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
object Sensors1 {
  def main(args: Array[String]): Unit = {
    //System.setProperties("hadoop.home.dir")
    //Logger.getLogger("org.apache").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Sensor Analysis")
      .getOrCreate()
    val inputDF = spark.read.format("csv").option("header",true).option("inferschema",true).load("C:\\Users\\Deepak\\Downloads\\scala-exercise-questions\\problem4\\Sensor_data.csv")
    //val df2 = inputDF.show()
    val windowing_Col = Window.partitionBy("SENSORS", "MNEUMONIC", "DATA", "TIMESTAMP")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val windowing_Key = Window.partitionBy("SENSORS").orderBy("MNEUMONIC")
    import spark.implicits._
    val windowSpec = Window.partitionBy("Sensors").orderBy("Sensors")
    import org.apache.spark.sql.functions._
    val outputDF = inputDF.withColumn("End_Date", lead("Timestamp", 1).over(windowSpec)).withColumn("Sensor_change", $"DATA" +
      (lead("DATA", 1).over(windowSpec))).filter("Sensor_change <>0").withColumnRenamed("Timestamp","Start_Date").drop("Sensor_change").show()
      }
      }
