package mapredLocalLogreg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import Utils.AdaptiveLogger;
import Utils.id.IDAndLabels;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

import de.tuberlin.dima.ml.RegressionModel;
import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;
import de.tuberlin.dima.ml.logreg.LogRegEnsembleModel;
import de.tuberlin.dima.ml.logreg.LogRegEnsembleModel.VotingSchema;
import de.tuberlin.dima.ml.validation.OnlineAccuracy;

public class EvalMapper extends Mapper<LongWritable, Text, Text, IntPairWritable> {

	private static AdaptiveLogger log = new AdaptiveLogger(
			Logger.getLogger(EvalMapper.class.getName()), 
			Level.DEBUG); 

	private double THRESHOLD = 0.5;
	private Map<String, RegressionModel> models = Maps.newHashMap();
	private Map<String, OnlineAccuracy> accuracies = Maps.newHashMap();
	private int labelDimension;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		log.debug("Eval Setup");

		labelDimension = Integer.parseInt(context.getConfiguration().get(EvalJob.CONF_KEY_LABEL_DIMENSION));

		// Read trained models from previous job output
		readEnsembleModels(context);
	}

	private void readEnsembleModels(Context context) throws IOException {

		String trainOuputPath = context.getConfiguration().get(EvalJob.CONF_KEY_TRAIN_OUTPUT);

		// TODO Make this generic for ensemble, global and majority. Build model classes for each with own prediction method
		Path dir = new Path(trainOuputPath);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FileStatus[] statusList = fs.listStatus(dir, new PathFilter() {
			public boolean accept(Path path) {
				if (path.getName().startsWith("part-r")) return true;
				else return false;
			}
		});

		ArrayList<Vector> ensembleModels = Lists.newArrayList();
	

		for (FileStatus status : statusList) {
			System.out.println("reading from++++++"+status.getPath().getName());
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), context.getConfiguration());
			try {
				Text newkey= new Text();
				IntWritable partitionId = new IntWritable();
				VectorWritable ensembleModel = new VectorWritable();
				while (reader.next(newkey, ensembleModel)) {
					String[] newkeys= newkey.toString().split("#");
					partitionId=new IntWritable(Integer.parseInt(newkeys[1]));
					ensembleModels.add((Vector)ensembleModel.get());
					System.out.println("- Ensemble-Model " + partitionId.get() + ": Non zeros: " + ensembleModel.get().getNumNonZeroElements());
				}
			} finally {
				Closeables.close(reader, true);
			}
		} 

		models.put("ensemble-majority", new LogRegEnsembleModel(ensembleModels, THRESHOLD, VotingSchema.MAJORITY_VOTE));
		accuracies.put("ensemble-majority", new OnlineAccuracy(THRESHOLD));
	//	models.put("ensemble-merged", new LogRegEnsembleModel(ensembleModels, THRESHOLD, VotingSchema.MERGED_MODEL));
	//	accuracies.put("ensemble-merged", new OnlineAccuracy(THRESHOLD));
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = String.valueOf(value);

		
		Vector v=new RandomAccessSparseVector(781);
		int label = LibSvmVectorReader.readVectorSingleLabel(v, line);
		
	
		
		for (Map.Entry<String, RegressionModel> model : models.entrySet()) {
			double prediction = model.getValue().predict(v);
			accuracies.get(model.getKey()).addSample(
					label,
					prediction);
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.debug("Cleanup: Write accuracy results");
		super.cleanup(context);
		// Write accuracy results
		for (Map.Entry<String, OnlineAccuracy> accuracy : accuracies.entrySet()) {
			context.write(
					new Text(accuracy.getKey()),
					new IntPairWritable(
							(int)accuracy.getValue().getTotal(),
							(int)accuracy.getValue().getCorrect()));
		}
	}
}