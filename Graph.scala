//import org.apache.commons.math3.geometry.spherical.twod.Edge
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
object Graph {
  def apply(myVertices: RDD[(VertexId, String)], myEdges:
  RDD[Edge[String]]) = ???

  def main(args: Array[String]): Unit = {
    //System.setProperties("hadoop.home.dir")
    //Logger.getLogger("org.apache").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("TestAPP")
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Graph Example")
      .getOrCreate()
    val myVertices = sc.makeRDD(Array((1L, "Socite General"), (2L,
      "Credit Agrocole"),
      (3L, "HSBC"), (4L, "Santander"), (5L, "UBS"),(6L, "RBS"), (7L,
        "DOUSTUE"),(8L, "BNP PARIBAS"), (9L, "BOURSORAMA")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, " "),
      Edge(2L, 3L, " "), Edge(3L, 4L, ""),Edge(1L, 5L, ""),
      Edge(5L, 6L, ""),Edge(6L, 7L, ""), Edge(2L, 8L, " "),Edge(2L, 9L, " ")))
    val myGraph = Graph(myVertices,myEdges)
    //val myGraph = Graph(myVertices, myEdges)
    // myGraph.vertices.collect
  }
}