package mapredBatchLogreg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import Utils.AbstractHadoopJob;
import Utils.HadoopUtils;



/**
 * Computes the gradient for logistic regression (log-likelihood) Works parallel over records
 */
public class GradientJob extends AbstractHadoopJob {

  private static String JOB_NAME = "Batch-Gradient-Test";

  static final int REDUCE_TASKS = 1;

  private String inputFile;
  private String outputPath;
  private int labelDimension;
  private int numFeatures;
  
  static final String CONF_KEY_LABEL_DIMENSION = "label-dimension";
  static final String CONF_KEY_NUM_FEATURES = "num-features";

  private final VectorWritable weights;

  public GradientJob(
      String inputFile, 
      String outputPath,
      int labelDimension,
      int numFeatures) {
    this.inputFile = inputFile;
    this.outputPath = outputPath;
    this.labelDimension = labelDimension;
    this.numFeatures = numFeatures;

    Vector vec = new SequentialAccessSparseVector(numFeatures);
    this.weights = new VectorWritable(vec);
  }

  public int run(String[] args) throws Exception {
	  System.out.println("Janani is here");
    Job job = prepareJob(
        JOB_NAME, 
        REDUCE_TASKS, 
        GradientMapper.class, 
        GradientReducer.class, 
        NullWritable.class,
        VectorWritable.class,
        NullWritable.class,
        VectorWritable.class,
        TextInputFormat.class,
        SequenceFileOutputFormat.class,
        inputFile,
        outputPath);
    
    job.getConfiguration().set(CONF_KEY_LABEL_DIMENSION, Integer.toString(labelDimension));
    job.getConfiguration().set(CONF_KEY_NUM_FEATURES, Integer.toString(numFeatures));

    Path cachePath = new Path(job.getConfiguration().get("hadoop.tmp.dir") + "/initial_weights");
    HadoopUtils.writeVectorToDistCache(job.getConfiguration(), this.weights, cachePath);

    return job.waitForCompletion(false) ? 0 : 1;
  }

  public void setWeightVector(double initial) {
    this.weights.get().assign(initial);
  }

  public void setWeightVector(double[] weights) {
    this.weights.get().assign(weights);
  }

  public void setWeightVector(Vector weights) {
    this.weights.set(weights);
  }
  
  public String getOutputPath() {
    return this.outputPath;
  }

}