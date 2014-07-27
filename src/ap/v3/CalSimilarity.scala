/*
 * Calculate Similarity between data instance.
 *  
 */

package ap.v3

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.AccumulatorParam
import breeze.linalg.SparseVector
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.Text
import ap.v2.Point


object CalSimilarity {

    type Pair = (Int, Array[Double])
    type SAR = (Int, Int, Double, Double, Double)

    implicit object SparVectorAP extends AccumulatorParam[SparseVector[Double]] {
        def zero(v: SparseVector[Double]) = SparseVector.zeros[Double](v.length)
        def addInPlace(v1: SparseVector[Double], v2: SparseVector[Double]) = {
            v1 += v2
            v1
        }
    }

	def parseVector(line: String): (Int, SparseVector[Double]) = {
        val tokens = line.split(",")
        val key = tokens(0).toInt + 1
        val values = tokens.slice(1, tokens.length).toArray.map(_.toDouble)
        val vector = SparseVector.zeros[Double](values.length)
        var index = 0
        for(value <- values) {
            vector(index) = value
            index += 1
        }
        (key, vector)
	}

/**
	def run(sc: SparkContext, args: Array[String]) = {
		if(args.length < 4){
			System.err.println("Usage: CalSimilarity <master> <input> <output> <num>")
			System.exit(1)
		}
        val input = sc.textFile(args(1))
        val output = args(2)
        val num = args(3).toInt
        val prefer = args(7).toDouble
        //generate tuple that needs to calculate
        val simTuples = input.map(parseVector).flatMap{case (id, vector) =>
        	for (i <- 1 to id; j <- id to num; if ((i == id) || (j == id && i < j)))
        	    yield ((i, j), vector)
        }.groupByKey()
        val sar = simTuples.flatMap{case ((x,y), tuples) =>
             if (tuples.size == 2) {
               val sim = - tuples.head.squaredDist(tuples.last)
               Array((x, y, sim, 0.0, 0.0),
                   (y, x, sim, 0.0, 0.0))
            }
             else Array((x, y, 0.0, 0.0, 0.0))       
        }
        val prefer2 = sar.map(p => p._3).filter(_ != 0.0).mean
        val sar2 = sar.map{p =>
                     if(p._1 == p._2)
                       (p._1, p._2, prefer2, p._4, p._5)
                     else
                       p
        }
        sar2.saveAsTextFile("sar-out")
        sar2
	}
*/


    def run(sc: SparkContext, args: Array[String]) = {
        if(args.length < 4){
            System.err.println("Usage: CalSimilarity <input> <output> <nums> <aConvict> <aMaxits> <damp> <preference> <tasks>")
            System.exit(1)
        }
        val input = sc.textFile(args(0))
        val output = args(1)
        val num = args(2).toInt
        val prefer = args(6).toDouble
        val tasks = args(7).toInt
        //every vector's squre sum
        var norms = sc.accumulator(SparseVector.zeros[Double](num))
        val trans_input = input.map(parseVector).flatMap{case (id, vector) =>
            //return's length is the dimension of vector
            var sumVector = SparseVector.zeros[Double](num)
            var sum = 0.0
            vector.foreach{p => 
                sum += p * p
            }
            sumVector(id - 1) = sum
            norms += sumVector
            vector.activeIterator.map{case (index, value) =>
                val line = SparseVector.zeros[Double](num) 
                line(id - 1) = value     
                (index + 1, line)
            }
        }.reduceByKey(_ + _, tasks)
        //activate execution
        trans_input.count()
        //sim_matrix.saveAsTextFile("sim_matrix")
        val vectorSum = sc.broadcast(norms.value)

        val sim_matrix = trans_input.flatMap{case (id, vector2) =>
            println("vector active size:" + vector2.activeSize)
            vector2.activeIterator.map{case (index, value) =>
                val line = SparseVector.zeros[Double](num) 
                vector2.activeIterator.filter(p => p._1 >= index).foreach{case (index2, value2) =>
                    line(index2) = value * value2
                }
                (index + 1, line)
            }
        }.reduceByKey(_ + _, tasks)

        val sar = sim_matrix.flatMap{case (id, vector3) => 
            val ret = new ArrayBuffer[SAR]()
            val start = id -1
            for (i <- start until vector3.length) {
                val newId = i + 1
                if(newId == id) {
                    ret += ((newId, newId, 0.0, 0.0, 0.0))
                }
                else {
                    val dist = 2 * vector3(i) - vectorSum.value(id - 1) - vectorSum.value(i)
                    ret += ((id, newId, dist, 0.0, 0.0))
                    ret += ((newId, id, dist, 0.0, 0.0))
                }
            }
            ret
        }

        val sim_mean = sar.filter(p => p._1 != p._2).map(p => p._3).mean
        println("mean: " + sim_mean)
        val sar2 = sar.map{p =>
                     if(p._1 == p._2)
                       (p._1, p._2, sim_mean, p._4, p._5)
                     else
                       p
        }
        sar2
    }

    def read(sc: SparkContext, args: Array[String]) = {
        if(args.length < 1){
            System.err.println("Usage: CalSimilarity <input>")
            System.exit(1)
        }
        val input = sc.sequenceFile[Text, Point](args(0))
        val sar = input.map(p => (p._2.x + 1, p._2.y + 1, p._2.sim.toDouble, p._2.avail.toDouble, p._2.res.toDouble))
        val sim_mean = sar.filter(p => p._1 != p._2).map(p => p._3).mean
        println("mean: " + sim_mean)
        val sar2 = sar.map{p =>
                     if(p._1 == p._2)
                       (p._1, p._2, sim_mean, p._4, p._5)
                     else
                       p
        }
        sar2
    }

	def main(args: Array[String]) {
        if(args.length < 3) {
            System.err.println("Usage: CalSimilarity <input> <outut> <nums>")
            System.exit(1)
        }
	  	val conf = new SparkConf()
            .setAppName("CalSimilarity")
		val sc = new SparkContext(conf)
		run(sc, args)
        sc.stop()
	}
}
