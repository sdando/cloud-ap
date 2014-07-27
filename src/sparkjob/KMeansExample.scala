package sparkjob
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.SparkContext

object KMeansExample {

  def main(args: Array[String]): Unit = {
    args.foreach(println)
    val jobJar="job.jar"
    val master="spark://myUbuntu:7077"
    val sc=new SparkContext(master,"KMeans",System.getenv("SPARK_HOME"),Seq(jobJar))
    
    val hdfsFile="hdfs://myUbuntu:9000/user/zx/kmeans_data.txt"
    val data=sc.textFile(hdfsFile)
    val parsedData=data.map(_.split(" ").map(_.toDouble))
    
    val numIterations=10
    val numClusters=2
    
//    val clusters=KMeans.train(parsedData, numClusters, numIterations)
//    val WSSSE=clusters.computeCost(parsedData)
    
//    println("WSSSE= "+WSSSE)   
  }

}