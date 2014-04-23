package datagenerator;


import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VectorWritable;


public class TextClusteringDataGen {

	public static class RandomGenMapper 
	extends Mapper<Object, Text, NullWritable, Text>{

		   
		private String point = "";
		private Random rand;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			rand = new Random();
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String str = value.toString();
			String[] tokens = str.split(" ");
			int dimensions = Integer.parseInt(tokens[0]);

			
			int offset=1;
		
			for(int i= 1; i<= Integer.parseInt(tokens[1]); i++)
			{
				for(int j=0; j<dimensions; j++)
				{
					if(point.length()>0){
						point+=",";
					}
					if(Math.random()>=0.5){
						offset=1;
					}
					else{
						offset=-1;
					}
					point = point + String.valueOf(Math.random()+offset);
				}
		
				context.write(NullWritable.get(), new Text(point));
			
				
			
				point="";
			}
		}
	}

	public static class IntSumReducer 
	extends Reducer<NullWritable,VectorWritable,NullWritable,VectorWritable> {
		
		public void reduce(IntWritable key, Iterable<VectorWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			System.out.println("i am here");
			for (VectorWritable val : values) {
				
				context.write(NullWritable.get(), val);
				
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: datagen <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Synthtic Cluster Generator");
		job.setJarByClass(TextClusteringDataGen.class);
		job.setMapperClass(RandomGenMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
