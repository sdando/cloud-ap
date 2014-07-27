package ap.v3

import org.apache.spark.{SparkContext,SparkConf}
import org.jblas.DoubleMatrix
import org.apache.spark.rdd.RDD
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger
import ap.v2.Timer
import java.util.Date

object APCluster {
  
  implicit object VectorAP extends AccumulatorParam[Array[Double]] {
    def zero(v: Array[Double]) = new Array(v.length)
    def addInPlace(v1: Array[Double], v2: Array[Double]) = {
        for(i <- 0 until v1.length) 
            v1(i) += v2(i)
            v1
    }
  }
  
  //(x,y,sim,avail,res)
  type SAR = (Int, Int, Double, Double, Double)
  var damp = 0.9
   /**
    * @param args master,input,output,num,aConvict,aMaxits,damp,preference
    * 
    */
  def main(args: Array[String]): Unit = {
  	if(args.length < 8) {
        System.err.println("Usage: APCluster <input> <output> <nums> <aConvict> <aMaxits> <damp> <preference> <tasks>")
	    System.exit(1)
  	}
    val conf = new SparkConf()
    	.setAppName("APCluster")
	val sc = new SparkContext(conf)
    sc.setCheckpointDir("CKP_DIR")
	run(sc, args)
	sc.stop()
  }
  
  def run(sc: SparkContext, args: Array[String]) {
    val input = args(0)
    val output = args(1)
    val num = args(2).toInt
    val convict = args(3).toInt
    val maxits= args(4).toInt
    damp = args(5).toDouble
    val prefer = args(6)
    // val readOrRun = args(7).toInt

    // sar = CalSimilarity.read(sc, args)
    // if(readOrRun == 1)
    PropertyConfigurator.configure("./log4j.properties")
    val log = Logger.getLogger("ap.v3.APCluster");
	log.info("\nAp Cluster Job Begin. ["+ new Date().toLocaleString()+"]");
    var sar = CalSimilarity.run(sc, args)

    var sarTemp = sar
    
    var bFinished = false
    var iTimes = 0
    var aE=new DoubleMatrix(num, convict)
	var bUnconverged = true
	
    val iterationStartTime = System.nanoTime()
    var iterationTime = 0.0
    val timer = new Timer
    
    val f1 = (p: SAR) => p._1
    val f2 = (p: SAR) => p._2
	while(!bFinished)
	{
	    iTimes += 1
	    timer.reset
	    timer.start
	    sarTemp = sar.groupBy(f1(_)).flatMap{p => 
	      calResponsibility(p._1, p._2.toSeq)}
	    sar = sarTemp.groupBy(f2(_)).flatMap{p => 
	      calAvailability(p._1, p._2.toSeq)}

	    if(iTimes % 10 == 0) {
	    	sar.checkpoint()
	    }

	    val examplars = sc.accumulator(Array.fill(num)(0.0))
	    sar.filter(p => p._1 == p._2).foreach{p =>
	      val examplar = Array.fill(num)(0.0)
	      if(p._4 + p._5 > 0.0)
	        examplar(p._1 - 1) = 1.0
	      examplars += examplar 
	    }
	    timer.end
	    iterationTime = (timer.duration().toDouble)/1000;
	    log.info("cluster-" + iTimes + ": " + iterationTime + "s");
	    
	    val icolumn = (iTimes - 1) % convict
		aE.putColumn(icolumn, new DoubleMatrix(examplars.value))
		
	    if (iTimes >= convict)
		{
		    val aSe = MatrixFunctions.aggregate(aE)
			var aTotal = 0
			for (i <- 0 until num)
			{
				val aValue =  aSe.get(i)
				if (aValue == convict || aValue == 0.0)
				{
					aTotal += 1
				}
			}
			
			bUnconverged = (aTotal != num)||(aSe.sum() == 0.0)
					
			if (!bUnconverged || iTimes == maxits)
			{
				bFinished = true
			}
		}
	}//end Iteration
    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    log.info("Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")
    
	println("Iteration: " + iTimes)
	if (bUnconverged) {
		println("reach the max Iteration times!")
	}

	val result = aE.getColumn((iTimes - 1) % convict)
	println("Examplar: ")
	for (i <- 0 until result.length) {
	    if (result.get(i) > 0.0) {
		    println("          " + (i + 1))
		}
	}
    println("Number of Clusters: " + result.sum().toInt)
    
  }//end function
  
  def calAvailability(key: Int, values: Seq[SAR]) = {
    var SumofMax = 0.0
    var rkk = 0.0
    var ret = new ArrayBuffer[SAR](values.length)
    for (pt <- values) {
	    if(pt._1 != key){
		    if(pt._5 > 0.0){
		        SumofMax += pt._5
		    }
		}
		else {
		    rkk = pt._5
		}
    }
		    
    var t = 0.0
    var sum = 0.0
    for (pt <- values) {
	    if(pt._1 != key){
		    sum = SumofMax
		    if(pt._5 > 0.0){
		        sum -= pt._5
		    }
		    sum += rkk
		    t = (damp * pt._4) + ((1 - damp) * math.min(0, sum))
		}
		else {
			t = (damp * pt._4) + ((1 - damp) * SumofMax)
		}
		val p = (pt._1, pt._2, pt._3, t, pt._5)
		ret += p
	}
    ret.toArray
  }
  
  def calResponsibility(key: Int, values: Seq[SAR]) = {
    var maxIndex = 0
	var max = Double.MinValue
	var secondMax = Double.MinValue
	var tmp = 0.0
	var t = 0.0
	var ret = ArrayBuffer[SAR]()
    for (pt <- values) {
        t = pt._3 + pt._4
	    if(t > max) {
	    	tmp = max
	    	max = t
	    	t = tmp
	    	maxIndex = pt._2
	    }
	    if(t > secondMax) {
	    	secondMax = t
	    }
    }    
    for (pt <- values) {
	    if(pt._2 != maxIndex) {
	    	t = (damp * pt._5) + ((1 - damp) * (pt._3 - max))
	    }
	    else {
	    	t = (damp * pt._5) + ((1 - damp) * (pt._3 - secondMax))
	    }
	    val p = (pt._1, pt._2, pt._3, pt._4, t)
        ret += p
    }
    ret.toArray
  }
}