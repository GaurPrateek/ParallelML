package mapredLocalLogreg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.VectorWritable;

import Utils.AbstractHadoopJob;
import Utils.VectorSingleLabeledWritable;

public class EnsembleJob extends AbstractHadoopJob {

  private static String JOB_NAME = "train-local-logreg";
  
  private String inputFile;
  private String outputPath;
  private int partitions;
  private int labelDimension;
  private int numFeatures;
  
  static final String CONF_KEY_LABEL_DIMENSION = "label-dimension";
  static final String CONF_KEY_NUM_FEATURES = "num-features";

  
  public EnsembleJob(
      String inputFile,
      String outputPath,
      int partitions,
      int labelDimension,
      int numFeatures) {
    this.inputFile = inputFile;
    this.outputPath = outputPath;
    this.partitions = partitions;
    this.labelDimension = labelDimension;
    this.numFeatures = numFeatures;
  }

  public int run(String[] args) throws Exception {

    Job job = prepareJob(
        JOB_NAME, 
        partitions, 
        EnsembleMapper.class, 
        EnsembleReducer.class, 
        IntWritable.class,
        VectorSingleLabeledWritable.class,
        Text.class,
        VectorWritable.class,
        TextInputFormat.class,
        SequenceFileOutputFormat.class,
        inputFile,
        outputPath);
    
    job.getConfiguration().set(CONF_KEY_LABEL_DIMENSION, Integer.toString(labelDimension));
    job.getConfiguration().set(CONF_KEY_NUM_FEATURES, Integer.toString(numFeatures));
    
    cleanupOutputDirectory(outputPath);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

}
