package mapredLocalKMeans;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;

public class StreamingKMeansMapRedRun {


	public static void writePointsToFile(List<Vector> points,
			String fileName,
			FileSystem fs,
			Configuration conf) throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}

	public static List<Vector> getPoints(String filepath) throws FileNotFoundException {
		List<Vector> points = new ArrayList<Vector>();
		Vector v;
		Scanner sc=  new Scanner(new File(filepath)).useDelimiter("\n");
		while(sc.hasNext()){
			String content = sc.next();

			v = new RandomAccessSparseVector(123);
			LibSvmVectorReader.readVectorSingleLabel(v, content);
			points.add(v);
		}

		return points;
	}

	public static void main(String args[]) throws Exception {

		int k = 3;

		List<Vector> vectors = getPoints("/Users/prateekgaur/Documents/iris.scale.txt");



		File testData = new File("/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/testdata");
		if (!testData.exists()) {
			testData.mkdir();
		}
		testData = new File("/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/testdata/points");
		if (!testData.exists()) {
			testData.mkdir();
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		writePointsToFile(vectors, "/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/testdata/points/file1", fs, conf);

		Path path = new Path("/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/testdata/clusters/part-00000");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, Text.class, Kluster.class);

		for (int i = 0; i < k; i++) {
			Vector vec = vectors.get(i);


			final Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();


		String[] args1 = new String[] {"-i","/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/testdata/points","-o","/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/output","--estimatedNumMapClusters","5","--searchSize","2","-k","3", "-testp", "0.2", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.CosineDistanceMeasure"};


		StreamingKMeansDriver.main(args1);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				new Path("/Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/mapredLocalKMeans/output/" + "part-r-00000"), conf);

		IntWritable key = new IntWritable();
		CentroidWritable value = new CentroidWritable();
		while (reader.next(key, value)) {
			System.out.println(value.toString() + " belongs to cluster "
					+ key.toString());
		}
		reader.close();
	}

}