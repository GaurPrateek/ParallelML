package logreghadoop;

import java.io.IOException;
import java.util.Random;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

public class EnsembleMapper extends Mapper<LongWritable, Text, IntWritable, VectorSingleLabeledWritable> {

	public static final int IDX_INPUT_LINE = 0;

	static final String CONF_KEY_NUM_FEATURES = "parameter.NUM_FEATURES";
	static final String CONF_KEY_NUM_PARTITIONS = "parameter.NUM_PARTITIONS";
	Random random = new Random();

	int numPartitions;
	int numFeatures;

	IntWritable curPartition = new IntWritable();
	VectorSingleLabeledWritable outputVector=new VectorSingleLabeledWritable();
	VectorWritable outputlabel=new VectorWritable();

	//private static AdaptiveLogger log = new AdaptiveLogger(
	//		Logger.getLogger(EnsembleMapper.class.getName()), 
	//		Level.DEBUG); 

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		//log.debug("Map Setup");

		numFeatures = Integer.parseInt(context.getConfiguration().get("num-features"));
		numPartitions = Integer.parseInt(context.getConfiguration().get("mapred.reduce.tasks"));
		//log.debug("- Number partitions (=number reducers): " + numberReducers);
		System.out.println("Prepare Map");
		System.out.println("- num partitions: " + numPartitions);
		System.out.println("- num features: " + numFeatures);
	}


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Randomly distribute to Reducers to get a random partitioning

		// TODO If Reducer uses SGD (Online, one-pass), we should also randomize the order within each partition!?

		// TODO Bug: Add custom partitioner to make sure that different partitions are sent to different reducers 
	
		String line = String.valueOf(value);

	
		Vector v=new RandomAccessSparseVector(numFeatures);
		int label = LibSvmVectorReader.readVectorSingleLabel(v, line);
		
		
		curPartition.set(random.nextInt(numPartitions));
		outputVector.setVector(v);
		outputVector.setLabels(label);

		context.write(curPartition, outputVector);
	}
}