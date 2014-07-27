package ap.v2;

import java.util.Date;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;

import common.HadoopUtil;

import conversion.InputDriver;


import similarity.measures.EuclideanDistanceSimilarity;

public class APCluster extends AbstractJob{

	static final String VECTOR_NAME="org.apache.mahout.math.RandomAccessSparseVector";
	static final String SIMILARITY_CLASSNAME=EuclideanDistanceSimilarity.class.getName();
	static final String VECTOR_PATH="vectors";
	static final String PREFERPATH="preference";
	static final String TEMP_DIR="temp";
	static final String SAR_DIR="SAR";
	static final String DAMP="damp";
	static final String NUMOFPOINTS="numOfPoints";
	static final String EXAMPLAR="examplar";
	private int m_aConvits; 
	private int m_aMaxits;
	private int m_aNumOfPointers;
	private String m_Lam;
	private Path m_inputPath;
	private Path m_outputPath;
	private Path m_tempPath;
	private Path m_preferPath;
	private double m_totalTime;
	private String m_n1;
	private String m_n2;
	/**
	 * @param args input,output,num,cardinality,aConvict,aMaxits,damp
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		APCluster instance=new APCluster();
		instance.run(args);
	}
	
    public int run(String[] args) throws Exception {
    	addInputOption();
    	addOutputOption();
    	addOption("numOfPoints", "n", "number of Points in the input data");
    	addOption("dimension","d","dimension of Points");
    	addOption("convict","c","number of unchanged times in the Iteration","3");
    	addOption("maxits","m","max number of Iteration","5");
    	addOption("damp","dp","damping factor","0.9");
    	addOption("noC","n1","number of CooccurrencesMapper","4");
    	addOption("noIr","n2","number of Iteration reducer","4");
    	
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        PropertyConfigurator.configure("./log4j.properties");
		m_inputPath=getInputPath();
		m_outputPath=getOutputPath();
		m_aNumOfPointers=Integer.parseInt(getOption(NUMOFPOINTS));
		String cardinality=getOption("dimension");
		m_aConvits=Integer.parseInt(getOption("convict"));
		m_aMaxits=Integer.parseInt(getOption("maxits"));
		m_Lam=getOption(DAMP);
		m_n1=getOption("noC");
		m_n2=getOption("noIr");
		

	    Configuration conf = new Configuration();
		Path vectorsPath=new Path(m_outputPath,VECTOR_PATH);
		HadoopUtil.delete(conf, vectorsPath);
		InputDriver.runJob(m_inputPath, vectorsPath, VECTOR_NAME);
		
		m_tempPath=new Path(m_outputPath,TEMP_DIR);
		m_preferPath=new Path(m_outputPath,PREFERPATH);
	    String startPhase="0";
	    String endPhase="3";
	    CalSimilarityJob rowSimilarityJob = new CalSimilarityJob();
	    rowSimilarityJob.setConf(conf);
	    Logger log=Logger.getLogger(APCluster.class);
	    log.info("\nAp Cluster Job Begin. ["+ new Date().toLocaleString()+"]");
	    rowSimilarityJob.run(new String[] { "--input", vectorsPath.toString(), "--output", m_preferPath.toString(),
	        "--numberOfColumns", cardinality, "--similarityClassname", SIMILARITY_CLASSNAME,
	        "--tempDir", m_tempPath.toString(),"--overwrite","--startPhase",startPhase,"--endPhase",endPhase,
	        "--numOfPoints",String.valueOf(m_aNumOfPointers),"--noC",m_n1});
	    Timer timer=new Timer();
	    timer.start();
	    runIteration(conf);   	
	    timer.end();
	    m_totalTime=((double)timer.duration())/1000;
	    log.info("Total run time: "+m_totalTime+"s");
    	return 0;
    }
	
    public int runIteration(Configuration conf) throws Exception{
	    boolean aFinished = false;
		int		aTimes = 0;
		CalResponsibilityJob calResponsibilityJob=new CalResponsibilityJob();
		CalAvailabilityJob calAvailabilityJob=new CalAvailabilityJob();
		CalExamplarJob calExamplarJob=new CalExamplarJob();
		String input=m_tempPath.toString();
		String output=m_outputPath+"/cluster-";
	    String prefer=HadoopUtil.readPrefer(m_preferPath, conf);
	    Matrix aE=new SparseRowMatrix(m_aConvits, m_aNumOfPointers);
	    Vector ae;
		boolean aUnconverged = true;
		double iterationTime;
		Logger log=Logger.getLogger(APCluster.class);
		Timer timer = new Timer() ;
		while (!aFinished)
		{
			aTimes++;           
			String outputPath=output+aTimes;
			timer.reset();
			timer.start();
			calResponsibilityJob.run(new String[] { "--input", input,"--output",outputPath,
					"--prefer",prefer.toString(),"--damp",m_Lam,"--noIr",m_n2});
			calAvailabilityJob.run(new String[] { "--input", outputPath,"--output",outputPath,
					"--damp",m_Lam,"--noIr",m_n2});
			calExamplarJob.run(new String[] { "--input", outputPath,"--output",outputPath,
					"--numOfPoints",String.valueOf(m_aNumOfPointers)});
			timer.end();
			iterationTime=((double)timer.duration())/1000;
			log.info("cluster-"+aTimes+": "+iterationTime+"s");
			Path examplarPath=new Path(outputPath,EXAMPLAR);
			ae=Vectors.readSequenceFile(examplarPath, conf);
			int aRows=(aTimes-1)%m_aConvits;
			aE.assignRow(aRows, ae);

			if (aTimes >=m_aConvits)
			{
				Vector aSe = MatrixFunctions.aggregate(aE);
				int aTotal = 0;
				for (int i = 0; i < m_aNumOfPointers; i++)
				{
					int aValue = (int) aSe.get(i);
					if (aValue == m_aConvits || aValue == 0)
					{
						aTotal += 1;
					}
				}
				aUnconverged = (aTotal != m_aNumOfPointers)||(aSe.zSum()==0.0);
			}			
			if (!aUnconverged || aTimes ==m_aMaxits )
			{
				aFinished = true;
			}
			input=outputPath;
		}
		
		System.out.println("finished!");
		System.out.println("iterated: " + aTimes);
		if(aUnconverged){
			System.out.println("reach the max Iteration times!");
		}

		Vector result=aE.viewRow((aTimes-1)%m_aConvits);
		System.out.println("Examplar: ");
		for(int i=0;i<result.size();i++){
			if(result.get(i)>0){
				System.out.println("          " + i);
			}
		}
		System.out.println("Number of Clusters: " + (int)result.zSum());

		return 0;
    }	
}
