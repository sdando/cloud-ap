package ap.v2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import common.HadoopUtil;



public class CalExamplarJob extends AbstractJob {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
	    addInputOption();
	    addOutputOption();
	    addOption(APCluster.NUMOFPOINTS,"","");
	    
	    Map<String,List<String>> parsedArgs = parseArguments(args);
	    if (parsedArgs == null) {
	        return -1;
	    }
	    
	    Path inputPath=new Path(getInputPath(), APCluster.SAR_DIR);
	    Path examplarPath=new Path(getOutputPath(),APCluster.EXAMPLAR);
	    
	    Configuration conf=new Configuration();
	    HadoopUtil.delete(conf, examplarPath);
	    JobConf jobConf=new JobConf(conf);
	    jobConf.setJobName(getOutputPath().getName()+"-"+getClass().getSimpleName());
	    jobConf.setJar("apJob.jar");
	    jobConf.set(APCluster.NUMOFPOINTS, getOption(APCluster.NUMOFPOINTS));
	    jobConf.setInputFormat(SequenceFileInputFormat.class);
	    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
	    FileInputFormat.setInputPaths(jobConf, inputPath);
	    FileOutputFormat.setOutputPath(jobConf, examplarPath);
	    jobConf.setMapperClass(EMapper.class);
	    jobConf.setCombinerClass(MergeVectorsCombiner.class);
	    jobConf.setReducerClass(MergeVectorsReducer.class);
	    jobConf.setMapOutputKeyClass(IntWritable.class);
	    jobConf.setMapOutputValueClass(VectorWritable.class);
	    jobConf.setOutputKeyClass(Text.class);
	    jobConf.setOutputValueClass(VectorWritable.class);
	    JobClient.runJob(jobConf);
	    
		return 0;
	}
	
	public static class EMapper extends MapReduceBase 
    	implements Mapper<Text,Point,IntWritable,VectorWritable> {
    	private int numOfPoints;
    	private Vector examplarVector;
    	
		public void configure(JobConf conf){
			numOfPoints=Integer.parseInt(conf.get(APCluster.NUMOFPOINTS));
		}

		public void map(Text row,Point pt,
			OutputCollector<IntWritable,VectorWritable> out,
			Reporter reporter) throws IOException {
		    if(pt.x==pt.y){
		    	examplarVector=new RandomAccessSparseVector(numOfPoints);
		    	if(pt.avail+pt.res>0){
			    	examplarVector.set(pt.x, 1.0);
		    	}
		    	else {
					examplarVector.set(pt.x, 0.0);
				}
		    	out.collect(new IntWritable(Integer.MIN_VALUE), new VectorWritable(examplarVector));
		    }
	    }
    }
	
    public static Vector merge(Iterator<VectorWritable> partialVectors){
        Vector accumulator = partialVectors.next().get();
        while (partialVectors.hasNext()) {
          VectorWritable v = partialVectors.next();
          if (v != null) {
            Iterator<Vector.Element> nonZeroElements = v.get().iterateNonZero();
            while (nonZeroElements.hasNext()) {
              Vector.Element nonZeroElement = nonZeroElements.next();
              accumulator.setQuick(nonZeroElement.index(), nonZeroElement.get());
            }
          }
        }
        return accumulator;
    }
	
    public static class MergeVectorsCombiner extends MapReduceBase 
    	implements Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>{
    	public void reduce(IntWritable row, Iterator<VectorWritable> partialVectors,
    			OutputCollector<IntWritable, VectorWritable> out,
    			Reporter reporter)throws IOException{
    			out.collect(row, new VectorWritable(merge(partialVectors)));
        }
    }

    public static class MergeVectorsReducer extends MapReduceBase
    	implements Reducer<IntWritable, VectorWritable, Text, VectorWritable>{

    	Text outKey=new Text();
    	public void reduce(IntWritable row, Iterator<VectorWritable> partialVectors,
    		OutputCollector<Text, VectorWritable> out,
    		Reporter reporter)throws IOException{
    			Vector partialVector = merge(partialVectors);
    			out.collect(outKey, new VectorWritable(partialVector));
    	}
    }
	
}
