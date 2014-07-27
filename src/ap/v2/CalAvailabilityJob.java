package ap.v2;

import java.io.IOException;
import java.util.ArrayList;
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

import common.HadoopUtil;




public class CalAvailabilityJob extends AbstractJob {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
	    addInputOption();
	    addOutputOption();
	    addOption(APCluster.DAMP,"","");
	    addOption(APCluster.NUMOFPOINTS,"","");
		addOption("noIr","n2","number of Iteration reducer","4");
	    
	    Map<String,List<String>> parsedArgs = parseArguments(args);
	    if (parsedArgs == null) {
	        return -1;
	    }
	    
	    String damp=getOption(APCluster.DAMP);
	    int nReducer=Integer.parseInt(getOption("noIr"));
	    Path inputPath=new Path(getInputPath(), APCluster.SAR_DIR+"-temp");
	    Path outputPath=new Path(getOutputPath(),APCluster.SAR_DIR);
	    
	    Configuration conf=new Configuration();
	    HadoopUtil.delete(conf, outputPath);
	    	    
	    JobConf jobConf=new JobConf(conf);
	    jobConf.setJobName(getOutputPath().getName()+"-"+getClass().getSimpleName());
	    jobConf.set(APCluster.DAMP, damp);
	    jobConf.setJar("apJob.jar");
//	    jobConf.set("mapred.min.split.size", "131072000");
	    jobConf.setInputFormat(SequenceFileInputFormat.class);
	    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
	    FileInputFormat.setInputPaths(jobConf, inputPath);
	    FileOutputFormat.setOutputPath(jobConf, outputPath);
	    jobConf.setMapperClass(AMapper.class);
	    jobConf.setReducerClass(AReducer.class);
	    jobConf.setMapOutputKeyClass(IntWritable.class);
	    jobConf.setMapOutputValueClass(Point.class);
	    jobConf.setOutputKeyClass(Text.class);
	    jobConf.setOutputValueClass(Point.class);
	    jobConf.setNumReduceTasks(nReducer);
	    JobClient.runJob(jobConf);
		return 0;
	}
	
	public static class AMapper extends MapReduceBase 
        implements Mapper<Text,Point,IntWritable,Point> {
    
		public void map(Text row,
            Point pt,
            OutputCollector<IntWritable,Point> out,
            Reporter reporter) throws IOException {
			out.collect(new IntWritable(pt.y), pt);
		}
	}
	  
    public static class AReducer extends MapReduceBase
    	implements Reducer<IntWritable, Point, Text, Point>{
    	private float damp;
	    Text outKey=new Text();
	    
		public void configure(JobConf conf){
			damp=Float.parseFloat(conf.get(APCluster.DAMP));
		}
		
	    public void reduce(IntWritable row, Iterator<Point> ptList,
		    	OutputCollector<Text, Point> out,
	            Reporter reporter)throws IOException{

	    	List<Point> pList=new ArrayList<Point>();
			float SumofMax=0;
			float rkk=0;
		    while(ptList.hasNext()){
		        Point pt=ptList.next().like();
		        if(pt.x!=row.get()){
		        	if(pt.res>0){
		        		SumofMax+=pt.res;
		        	}
		        }
		        else {
					rkk=pt.res;
				}
		        pList.add(pt);
		    }
		    
            for(Point p:pList){
		        if(p.x!=row.get()){
		        	float sum=SumofMax;
		        	if(p.res>0){
		        		sum-=p.res;
		        	}
		        	sum+=rkk;
		        	p.avail=(damp*p.avail)+((1-damp)*Math.min(0, sum));
		        }
		        else {
					p.avail=(damp*p.avail)+((1-damp)*SumofMax);
				}
		        out.collect(outKey, p);
		    }
		}//end of reduce
    }//end of class AReducer
}
