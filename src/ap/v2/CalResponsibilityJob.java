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



public class CalResponsibilityJob extends AbstractJob {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
	    addInputOption();
	    addOutputOption();
	    addOption("sarPath","","");
	    addOption("prefer","","");
	    addOption("damp","","");
		addOption("noIr","n2","number of Iteration reducer","4");
	    
	    Map<String,List<String>> parsedArgs = parseArguments(args);
	    if (parsedArgs == null) {
	        return -1;
	    }
	    
	    String prefer=getOption("prefer");
	    String damp=getOption("damp");
	    int nReducer=Integer.parseInt(getOption("noIr"));
	    Path sarPath=new Path(getInputPath(), APCluster.SAR_DIR);
	    Path outputPath=new Path(getOutputPath(), APCluster.SAR_DIR+"-temp");

	    Configuration conf=new Configuration();
	    HadoopUtil.delete(conf, outputPath);
	    JobConf jobConf=new JobConf(conf, CalResponsibilityJob.class);
	    jobConf.setJobName(getOutputPath().getName()+"-"+getClass().getSimpleName());
	    jobConf.setJar("apJob.jar");
	    jobConf.set(APCluster.PREFERPATH, prefer);    
	    jobConf.set(APCluster.DAMP, damp);
//	    jobConf.set("mapred.min.split.size", "131072000");
	    jobConf.setInputFormat(SequenceFileInputFormat.class);
	    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
	    FileInputFormat.setInputPaths(jobConf, sarPath);
	    FileOutputFormat.setOutputPath(jobConf, outputPath);
	    jobConf.setMapperClass(RMapper.class);
	    jobConf.setReducerClass(RReducer.class);
	    jobConf.setMapOutputKeyClass(IntWritable.class);
	    jobConf.setMapOutputValueClass(Point.class);
	    jobConf.setOutputKeyClass(Text.class);
	    jobConf.setOutputValueClass(Point.class);
	    jobConf.setNumReduceTasks(nReducer);
	    JobClient.runJob(jobConf);
		return 0;
	}
	
	public static class RMapper extends MapReduceBase 
	    implements Mapper<Text,Point,IntWritable,Point> {
	    
		float prefer;

		public void configure(JobConf conf){
			prefer=Float.parseFloat(conf.get(APCluster.PREFERPATH));
		}
		
		public void map(Text row,
                Point pt,
                OutputCollector<IntWritable,Point> out,
                Reporter reporter) throws IOException {
            if(pt.x==pt.y)
            	pt.sim=prefer;
            out.collect(new IntWritable(pt.x), pt);
	    }
	}
	
	public static class RReducer extends MapReduceBase 
        implements Reducer<IntWritable, Point, Text, Point>{
    
	    float prefer;
	    float damp;
	    Text outKey=new Text();
	
	    public void configure(JobConf conf){
	    	prefer=Float.parseFloat(conf.get(APCluster.PREFERPATH));
	    	damp=Float.parseFloat(conf.get(APCluster.DAMP));
	    }
	
	    public void reduce(IntWritable row, Iterator<Point> ptList,
	    	OutputCollector<Text, Point> out,
            Reporter reporter)throws IOException{
	    	int maxIndex=0;
	    	float max=-Float.MAX_VALUE;
	    	float second=-Float.MAX_VALUE;
	    	float tmp,t;
	    	List<Point> pList=new ArrayList<Point>();
	    	while(ptList.hasNext()){
	    		Point pt=ptList.next().like();	    		
	    		t=pt.avail+pt.sim;
	    		if(t>max){
	    			tmp=max;max=t;t=tmp;
	    			maxIndex=pt.y;
	    		}
	    		if(t>second){
	    			second=t;
	    		}
	    		pList.add(pt);
	    	}
	    	for(Point p:pList){
	    		if(p.y!=maxIndex){
	    			p.res=(damp*p.res)+((1-damp)*(p.sim-max));
	    		}
	    		else {
	    			p.res=(damp*p.res)+((1-damp)*(p.sim-second));
	    		}
	    		out.collect(outKey,p);
	    	}
	    }
	}
}
