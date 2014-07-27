package sparkjob;
import org.apache.spark._
import SparkContext._

object WordCount {
  def main(args: Array[String]) {
    if (args.length ==0 ){
      println("usage is org.test.WordCount <master>")
    }
    println("the args: ")
    args.foreach(println)

    val hdfsPath = "hdfs://myUbuntu:9000/user/zx/"
    val jobJar="job.jar"

    // create the SparkContext， args(0)由yarn传入appMaster地址
    val sc = new SparkContext(args(0), "WordCount",System.getenv("SPARK_HOME"), Seq(jobJar))

    val textFile = sc.textFile(hdfsPath + args(1))

    val result = textFile.flatMap(line => line.split("\\s+"))
        .map(word => (word, 1)).reduceByKey(_ + _)

    result.saveAsTextFile(hdfsPath + args(2))
  }
}

