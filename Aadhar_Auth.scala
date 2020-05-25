//import java.io.File
import java.sql.DriverManager
import org.apache.spark.sql.Encoders
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, expressions, _}
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.{SparkConf}

object Aadhar_Auth{
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

    val sc = new SparkContext(conf)


    val spark = SparkSession
      .builder()

      .appName("Aadhar Authentication")

      .master("local")

      .getOrCreate()
    import spark.implicits._
val arg1 = args(0)
    val aadharDF = spark.read
      .format("csv")
      .option("header","true")
      .load(s"${arg1}") //pass the path of the file
    val view_Adhar = aadharDF.createOrReplaceTempView("Adhar_Auth")
    val result_Adhar = spark.sql("select * from Adhar_Auth where aua > 650000 and sa > 0 and  res_state_name != 'Delhi' ").show()

  }}