package testkmeans;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;



public class SyntheticClusteringDataGenerator {

	public static class RandomGenMapper 
	extends Mapper<Object, Text, NullWritable, VectorWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text("1");
		private DenseVector vec= new DenseVector(2);
		private VectorWritable vw = new VectorWritable();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			System.out.println("I have entered here");
			for(int i=1; i<=100 ; i++)
			{
				vec.set(0, Math.random());
				vec.set(1, Math.random());
				vw.set(vec);
				context.write(NullWritable.get(), vw);

			}
		}
	}

	public static class IntSumReducer 
	extends Reducer<NullWritable,VectorWritable,NullWritable,VectorWritable> {

		public void reduce(NullWritable key, Iterable<VectorWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (VectorWritable val : values) {
				context.write(NullWritable.get(), val);
				System.out.println("written "+NullWritable.get()+" "+ val);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: clustdatagenerator <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Synthtic Cluster Generator");
		job.setJarByClass(SyntheticClusteringDataGenerator.class);
		job.setMapperClass(RandomGenMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(FileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	job.waitForCompletion(true);
	}
}
