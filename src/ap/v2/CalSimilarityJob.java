/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ap.v2;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import similarity.measures.VectorSimilarityMeasure;

public class CalSimilarityJob extends AbstractJob {

  public static final double NO_THRESHOLD = Double.MIN_VALUE;

  static final String SIMILARITY_CLASSNAME = CalSimilarityJob.class + ".distributedSimilarityClassname";
  static final String NUMBER_OF_COLUMNS = CalSimilarityJob.class + ".numberOfColumns";
  static final String MAX_SIMILARITIES_PER_ROW = CalSimilarityJob.class + ".maxSimilaritiesPerRow";
  static final String EXCLUDE_SELF_SIMILARITY = CalSimilarityJob.class + ".excludeSelfSimilarity";

  static final String THRESHOLD = CalSimilarityJob.class + ".threshold";
  static final String NORMS_PATH = CalSimilarityJob.class + ".normsPath";
  static final String MAXVALUES_PATH = CalSimilarityJob.class + ".maxWeightsPath";

  static final String NUM_NON_ZERO_ENTRIES_PATH = CalSimilarityJob.class + ".nonZeroEntriesPath";
  private static final int DEFAULT_MAX_SIMILARITIES_PER_ROW = 100;

  private static final int NORM_VECTOR_MARKER = Integer.MIN_VALUE;
  private static int m_n1;

  enum Counters { ROWS, COOCCURRENCES, PRUNED_COOCCURRENCES }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CalSimilarityJob(), args);
  }


  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption("numberOfColumns", "r", "Number of columns in the input matrix", false);
    addOption("similarityClassname", "s", "Name of distributed similarity class to instantiate, alternatively use "
        + "one of the predefined similarities (" + VectorSimilarityMeasures.list() + ')');
    addOption("maxSimilaritiesPerRow", "m", "Number of maximum similarities per row (default: "
        + DEFAULT_MAX_SIMILARITIES_PER_ROW + ')', String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ROW));
    addOption("excludeSelfSimilarity", "ess", "compute similarity of rows to themselves?", String.valueOf(false));
    addOption("threshold", "tr", "discard row pairs with a similarity value below this", false);
    addOption(DefaultOptionCreator.overwriteOption().create());
    addOption("numOfPoints", "nP","Number of Points",false);
	addOption("noC","n1","number of CooccurrencesMapper","4");

    Map<String,List<String>> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }

    int numberOfColumns;

    if (hasOption("numberOfColumns")) {
      // Number of columns explicitly specified via CLI
      numberOfColumns = Integer.parseInt(getOption("numberOfColumns"));
    } else {
      // else get the number of columns by determining the cardinality of a vector in the input matrix
      numberOfColumns = getDimensions(getInputPath());
    }

    String similarityClassnameArg = getOption("similarityClassname");
    String similarityClassname;
    try {
      similarityClassname = VectorSimilarityMeasures.valueOf(similarityClassnameArg).getClassname();
    } catch (IllegalArgumentException iae) {
      similarityClassname = similarityClassnameArg;
    }

    // Clear the output and temp paths if the overwrite option has been set
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      // Clear the temp path
      HadoopUtil.delete(getConf(), getTempPath());
      // Clear the output path
      HadoopUtil.delete(getConf(), getOutputPath());
    }

    boolean excludeSelfSimilarity = Boolean.parseBoolean(getOption("excludeSelfSimilarity"));
    double threshold = hasOption("threshold") ?
        Double.parseDouble(getOption("threshold")) : NO_THRESHOLD;
    m_n1=Integer.parseInt(getOption("noC"));

    Path weightsPath = getTempPath("weights");
    Path normsPath = getTempPath("norms.bin");
    Path pairwiseSimilarityPath= getTempPath(APCluster.SAR_DIR);

    AtomicInteger currentPhase = new AtomicInteger();

    //weights保存转置矩阵
    //MergeVectorsCombiner将分段的向量合成整个一段
    //VectorNormMapper求每个向量各维度的平方，结果也保存在向量中
    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      Job normsAndTranspose = prepareJob(getInputPath(), weightsPath, VectorNormMapper.class, IntWritable.class,
          VectorWritable.class, MergeVectorsReducer.class, IntWritable.class, VectorWritable.class);
      normsAndTranspose.setCombinerClass(MergeVectorsCombiner.class);
      Configuration normsAndTransposeConf = normsAndTranspose.getConfiguration();
      normsAndTransposeConf.set(THRESHOLD, String.valueOf(threshold));
      normsAndTransposeConf.set(NORMS_PATH, normsPath.toString());
      normsAndTransposeConf.set(SIMILARITY_CLASSNAME, similarityClassname);
      normsAndTransposeConf.set(APCluster.NUMOFPOINTS, getOption(APCluster.NUMOFPOINTS));
      boolean succeeded = normsAndTranspose.waitForCompletion(true);
      if (!succeeded) {
        return -1;
      }
    }
    
    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      Job pairwiseSimilarity = prepareJob(weightsPath, pairwiseSimilarityPath, CooccurrencesMapper.class,
          IntWritable.class, VectorWritable.class, SimilarityReducer.class, Text.class, Point.class);
      pairwiseSimilarity.setCombinerClass(VectorSumReducer.class);     
      Configuration pairwiseConf = pairwiseSimilarity.getConfiguration();
      pairwiseConf.set(THRESHOLD, String.valueOf(threshold));
      pairwiseConf.set(NORMS_PATH, normsPath.toString());
      pairwiseConf.set(SIMILARITY_CLASSNAME, similarityClassname);
      pairwiseConf.setInt(NUMBER_OF_COLUMNS, numberOfColumns);
      pairwiseConf.setBoolean(EXCLUDE_SELF_SIMILARITY, excludeSelfSimilarity);
      pairwiseConf.set(APCluster.NUMOFPOINTS, getOption(APCluster.NUMOFPOINTS));
      pairwiseConf.set("mapred.max.split.size", String.valueOf(spilitSize(pairwiseConf, weightsPath)));
      pairwiseConf.set("io.sort.mb", "300");
      pairwiseConf.set("io.sort.spill.percent", "0.9");
      pairwiseConf.set("mapred.reduce.tasks", "2");
      boolean succeeded = pairwiseSimilarity.waitForCompletion(true);
      if (!succeeded) {
        return -1;
      }
    }

    //求preference
    if (shouldRunNextPhase(parsedArgs, currentPhase)) {
      Job asMatrix = prepareJob(pairwiseSimilarityPath, getOutputPath(), TokenizerMapper.class,
          Text.class, FloatWritable.class, IntSumReducer.class, Text.class,
          FloatWritable.class);
      asMatrix.getConfiguration().set(APCluster.NUMOFPOINTS, getOption(APCluster.NUMOFPOINTS));
      boolean succeeded = asMatrix.waitForCompletion(true);
      if (!succeeded) {
        return -1;
      }
    }
    
    return 0;
  }
  
  public static long spilitSize(Configuration conf,Path input) throws Exception{
	  FileSystem hdfs=FileSystem.get(conf);
	  long fileSize=0;
	  for(FileStatus fileStatus:hdfs.listStatus(input)){
		  if(fileStatus.getPath().getName().contains("part-"))
			  fileSize+=fileStatus.getLen();
	  }
	  return fileSize/m_n1;
  }

  public static class VectorNormMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {

    private VectorSimilarityMeasure similarity;
    private Vector norms;
    private int numOfPoints;

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      similarity = ClassUtils.instantiateAs(ctx.getConfiguration().get(SIMILARITY_CLASSNAME),
          VectorSimilarityMeasure.class);
      norms = new RandomAccessSparseVector(Integer.MAX_VALUE);          //距离为欧式距离时为保存矩阵每行各项的平方的和
      numOfPoints=Integer.parseInt(ctx.getConfiguration().get(APCluster.NUMOFPOINTS));
    }

    @Override
    protected void map(IntWritable row, VectorWritable vectorWritable, Context ctx)
        throws IOException, InterruptedException {

      Vector rowVector = similarity.normalize(vectorWritable.get());

      Iterator<Vector.Element> nonZeroElements = rowVector.iterateNonZero();
      while (nonZeroElements.hasNext()) {
        Vector.Element element = nonZeroElements.next();
        RandomAccessSparseVector partialColumnVector = new RandomAccessSparseVector(numOfPoints);
        partialColumnVector.setQuick(row.get(), element.get());
        ctx.write(new IntWritable(element.index()), new VectorWritable(partialColumnVector));
      }

      norms.setQuick(row.get(), similarity.norm(rowVector));
      ctx.getCounter(Counters.ROWS).increment(1);
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
      super.cleanup(ctx);
      ctx.write(new IntWritable(NORM_VECTOR_MARKER), new VectorWritable(norms));
    }
  }
  
  //Vectors.merge把分段的向量合成一个
  public static class MergeVectorsCombiner extends Reducer<IntWritable,VectorWritable,IntWritable,VectorWritable> {
    @Override
    protected void reduce(IntWritable row, Iterable<VectorWritable> partialVectors, Context ctx)
        throws IOException, InterruptedException {
      ctx.write(row, new VectorWritable(Vectors.merge(partialVectors)));
    }
  }

  public static class MergeVectorsReducer extends Reducer<IntWritable,VectorWritable,IntWritable,VectorWritable> {

    private Path normsPath;

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      normsPath = new Path(ctx.getConfiguration().get(NORMS_PATH));
    }

    @Override
    protected void reduce(IntWritable row, Iterable<VectorWritable> partialVectors, Context ctx)
        throws IOException, InterruptedException {
      Vector partialVector = Vectors.merge(partialVectors);

      if (row.get() == NORM_VECTOR_MARKER) {
        Vectors.write(partialVector, normsPath, ctx.getConfiguration());
      } 
      else {
        ctx.write(row, new VectorWritable(partialVector));
      }
    }
  }
  //MergeVectorsReducer输出转置矩阵


  public static class CooccurrencesMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {

    private VectorSimilarityMeasure similarity;

    private int numOfPoints;

    private static final Comparator<Vector.Element> BY_INDEX = new Comparator<Vector.Element>() {
      @Override
      public int compare(Vector.Element one, Vector.Element two) {
        return Ints.compare(one.index(), two.index());
      }
    };

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      similarity = ClassUtils.instantiateAs(ctx.getConfiguration().get(SIMILARITY_CLASSNAME),
          VectorSimilarityMeasure.class);
      numOfPoints=Integer.parseInt(ctx.getConfiguration().get(APCluster.NUMOFPOINTS));
    }

    private boolean consider(Vector.Element occurrenceA, Vector.Element occurrenceB) {
//      int numNonZeroEntriesA = numNonZeroEntries.get(occurrenceA.index());
//      int numNonZeroEntriesB = numNonZeroEntries.get(occurrenceB.index());
//
//      double maxValueA = maxValues.get(occurrenceA.index());
//      double maxValueB = maxValues.get(occurrenceB.index());

      return true;
    }

    @Override
    protected void map(IntWritable column, VectorWritable occurrenceVector, Context ctx)
        throws IOException, InterruptedException {
      Vector.Element[] occurrences = Vectors.toArray(occurrenceVector);
      Arrays.sort(occurrences, BY_INDEX);

      int cooccurrences = 0;
      int prunedCooccurrences = 0;
      for (int n = 0; n < occurrences.length; n++) {
        Vector.Element occurrenceA = occurrences[n];
        Vector dots = new RandomAccessSparseVector(numOfPoints);
        for (int m = n; m < occurrences.length; m++) {
          Vector.Element occurrenceB = occurrences[m];
          if (consider(occurrenceA, occurrenceB)) {
            dots.setQuick(occurrenceB.index(), similarity.aggregate(occurrenceA.get(), occurrenceB.get()));
            cooccurrences++;
          } else {
            prunedCooccurrences++;
          }
        }
        ctx.write(new IntWritable(occurrenceA.index()), new VectorWritable(dots));
      }
      ctx.getCounter(Counters.COOCCURRENCES).increment(cooccurrences);
      ctx.getCounter(Counters.PRUNED_COOCCURRENCES).increment(prunedCooccurrences);
    }
  }


  public static class SimilarityReducer
      extends Reducer<IntWritable,VectorWritable,Text,Point> {

    private VectorSimilarityMeasure similarity;
    private int numberOfColumns;
    private Vector norms;
    private Text outKey=new Text();

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      similarity = ClassUtils.instantiateAs(ctx.getConfiguration().get(SIMILARITY_CLASSNAME),
          VectorSimilarityMeasure.class);
      numberOfColumns = ctx.getConfiguration().getInt(NUMBER_OF_COLUMNS, -1);
      Preconditions.checkArgument(numberOfColumns > 0, "Incorrect number of columns!");
      norms = Vectors.read(new Path(ctx.getConfiguration().get(NORMS_PATH)), ctx.getConfiguration());;
    }

    @Override
    protected void reduce(IntWritable row, Iterable<VectorWritable> partialDots, Context ctx)
        throws IOException, InterruptedException {
      Iterator<VectorWritable> partialDotsIterator = partialDots.iterator();
      Vector dots = partialDotsIterator.next().get();
      while (partialDotsIterator.hasNext()) {
        Vector toAdd = partialDotsIterator.next().get();
        Iterator<Vector.Element> nonZeroElements = toAdd.iterateNonZero();
        while (nonZeroElements.hasNext()) {
          Vector.Element nonZeroElement = nonZeroElements.next();
          dots.setQuick(nonZeroElement.index(), dots.getQuick(nonZeroElement.index()) + nonZeroElement.get());
        }
      }
      //前面类似于VectorSumReducer
      //遍历dots,利用xi*yi求x^2-y^2,x、y皆为向量
      double normA = norms.getQuick(row.get());
//    double partialSum=0;
      Iterator<Vector.Element> dotsWith = dots.iterateNonZero();
      while (dotsWith.hasNext()) {
        Vector.Element b = dotsWith.next();
        double similarityValue = similarity.similarity(b.get(), normA, norms.getQuick(b.index()), numberOfColumns);
//        partialSum+=similarityValue;
//        if (similarityValue >= treshold) {5
        ctx.write(outKey, new Point(row.get(), b.index(),(float)similarityValue, 0, 0));
        if(row.get()!=b.index()){
            ctx.write(outKey, new Point(b.index(), row.get(), (float)similarityValue, 0, 0));
        }
      }
//    if (excludeSelfSimilarity) {
//       similarities.setQuick(row.get(), partialSum);
//    }
//      ctx.write(row, new VectorWritable(similarities));
    }
  }
  
   // 求preference
    public static class TokenizerMapper 
        extends Mapper<Text,Point, Text, FloatWritable>{

      public void map(Text key, Point point, Context context
          ) throws IOException, InterruptedException {
    	  context.write(key,new FloatWritable(point.sim));  
      }
    }
  
    public static class IntSumReducer 
        extends Reducer<Text,FloatWritable,Text,FloatWritable> {
      private FloatWritable result = new FloatWritable();
      private int numOfPoints;
      
      protected void setup(Context ctx) throws IOException, InterruptedException {
          numOfPoints=Integer.parseInt(ctx.getConfiguration().get(APCluster.NUMOFPOINTS));
      }

      public void reduce(Text key, Iterable<FloatWritable> values, 
        Context context) throws IOException, InterruptedException {
          float sum = 0;
          for (FloatWritable val : values) {
          sum += val.get();
          }
          result.set(sum/(numOfPoints*(numOfPoints-1)));
          context.write(key, result);
      }
    }
  
}
