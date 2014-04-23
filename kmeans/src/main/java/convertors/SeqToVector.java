package convertors;



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;


public class SeqToVector {

	public static class RandomGenMapper 
	extends Mapper<Object, CentroidWritable, NullWritable, CentroidWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text("1");
		private DenseVector vec= new DenseVector(2);
		private VectorWritable vw = new VectorWritable();

		public void map(Object key, CentroidWritable value, Context context
				) throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}

	public static class IntSumReducer 
	extends Reducer<Text,VectorWritable,NullWritable,VectorWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<VectorWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (VectorWritable val : values) {
				context.write(NullWritable.get(), val);
			}
//			result.set(sum);
//			context.write(null, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(SeqToVector.class);
		job.setMapperClass(RandomGenMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(CentroidWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//		SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
