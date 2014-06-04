package mapredBatchLogreg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import Utils.id.IDAndLabels;
import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;
import de.tuberlin.dima.ml.logreg.LogRegMath;


public class TrainingErrorMapper extends
    Mapper<LongWritable, Text, NullWritable, DoubleWritable> {
  
  private int labelDimension;

  private Vector w;
  private DoubleWritable trainingError = new DoubleWritable();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    
    labelDimension = Integer.parseInt(context.getConfiguration().get(TrainingErrorJob.CONF_KEY_LABEL_DIMENSION));

    // read initial weights
    Configuration conf = context.getConfiguration();
    Path[] iterationWeights = DistributedCache.getLocalCacheFiles(conf);

    if (iterationWeights == null) { throw new RuntimeException("No weights set"); }

    Path localPath = new Path("file://" + iterationWeights[0].toString());

    for (Pair<NullWritable, VectorWritable> weights : new SequenceFileIterable<NullWritable, VectorWritable>(
        localPath, conf)) {

      this.w = weights.getSecond().get();
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {

 
	String line = String.valueOf(value);


	
	Vector x=new RandomAccessSparseVector(781);
	int y1 = LibSvmVectorReader.readVectorSingleLabel(x, line);
	//System.out.println(y1);
	double y=(double)y1;
    this.trainingError.set(LogRegMath.computeSqError(x, this.w, y));

    context.write(NullWritable.get(), this.trainingError);
  }
}