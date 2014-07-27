package conversion

import org.apache.spark.{SparkContext,SparkConf}
import java.util.Random
import java.io.PrintWriter

object Sample {
  
  def judge(line: String, flg: Int): Boolean = {
    val tokens = line.split(",")
    var result = true
    if(flg == 0) {
      if(tokens(tokens.length - 1) != "normal.")
        result = false
    }//过滤非normal
    else {
      if(tokens(tokens.length - 1) == "normal.")
        result = false
    }//过滤normal
    result
  }
  
  def main(args: Array[String]): Unit = {
  	if(args.length < 4) {
        System.err.println("Usage: Sample <input> <output> <num1> <num2>")
	    System.exit(1)
  	}
    val conf = new SparkConf()
    	.setAppName("Sample")
    	.setMaster("spark://myUbuntu:7077")
	val sc = new SparkContext(conf)
    
    val data = sc.textFile(args(0)).cache
    val output = args(1)
    val num1 = args(2).toInt
    val num2 = args(3).toInt
    val random = new Random
    val normal = data.filter(p => judge(p, 0)).takeSample(true, num1, random.nextInt)
    val imnormal = data.filter(p => judge(p, 1)).takeSample(true, num2, random.nextInt)
    println("normal: " + normal.length)
    println("im: " + imnormal.length)
    val out = new PrintWriter(output)
    out.print(normal.mkString("\n"))
    out.println()
    out.print(imnormal.mkString("\n"))
    out.println()
    out.close()
    data.unpersist()
	sc.stop()
  }
}