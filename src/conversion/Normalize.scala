package conversion

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import java.io.PrintWriter

object Normalize {
  
  def parseVector(line: String): (Int, Array[Double]) = {
    val tokens = line.split(",")
    val key = tokens(0).toInt + 1
    val vector = tokens.slice(1, tokens.length).toArray.map(_.toDouble)
    (key, vector)
  }  
  
  def add(v1: Array[Double], v2: Array[Double]) = {
    var i = 0
    while(i < v1.length) {
      v1(i) += v2(i)
      i = i + 1
    }
    v1
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 5) {
        System.err.println("Usage: Sample <input> <output> <num1> <num2> <task>")
	    System.exit(1)
  	}
    val conf = new SparkConf()
    	.setAppName("Normalize")
	val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val output = args(1)
    val num1 = args(2).toInt
    val num2 = args(3).toInt
    val task = args(4).toInt
    
    val trans_input = data.map(parseVector).flatMap{case (id, vector) =>
            //return's length is the dimension of vector
            vector.zipWithIndex.map{case (value, index) =>
                val line = new Array[Double](num1)
                line(id - 1) = value     
                (index + 1, line)
            }
        }.reduceByKey(add(_, _), task)
        
    val norm_trans = trans_input.map{case (id, vector) => 
       var min = vector.min
       var max  = vector.max
       if(max != min)
           (id, vector.map(p => (p - min) / (max - min)))
       else 
           (id, vector)
       }
        
    val norms = norm_trans.flatMap{case (id, vector) =>
            //return's length is the dimension of vector
            vector.zipWithIndex.map{case (value, index) =>
                val line = new Array[Double](num2)
                line(id - 1) = value     
                (index + 1, line)
            }
        }.reduceByKey(add(_, _), 1).map{case (id, vector) =>
          (id - 1).toString + "," + vector.mkString(",")
        }
    norms.saveAsTextFile(output)
    sc.stop()
  }

}