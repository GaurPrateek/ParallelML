package mapreduceKMeans;

import java.io.File;
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
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;

public class BatchKMeansMain {


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

  public static List<Vector> getPoints(double[][] raw) {
    List<Vector> points = new ArrayList<Vector>();
    for (int i = 0; i < raw.length; i++) {
      double[] fr = raw[i];
      Vector vec = new RandomAccessSparseVector(fr.length);
      vec.assign(fr);
      points.add(vec);
    }
    return points;
  }

  public static void main(String args[]) throws Exception {
	  BatchKMeansMain.run(4, "/Users/prateekgaur/Desktop/a3a.txt");
  }
  
  
  // Have to check how to determine dynamically the size of RandomAccessSparseVector to initialize
  
  public static void run(int number_of_clusters, String filepath) throws Exception {

    int k = number_of_clusters;

    Vector v;
    List<Vector> vectors = new ArrayList<Vector>();
  Scanner sc=  new Scanner(new File(filepath)).useDelimiter("\n");
  while(sc.hasNext()){
    String content = sc.next();
    System.out.println(content);
    v = new RandomAccessSparseVector(123);
     LibSvmVectorReader.readVectorSingleLabel(v, content);
     vectors.add(v);
  }


    File testData = new File("testdata");
    if (!testData.exists()) {
      testData.mkdir();
    }
    testData = new File("testdata/points");
    if (!testData.exists()) {
      testData.mkdir();
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    writePointsToFile(vectors, "testdata/points/file1", fs, conf);

    Path path = new Path("testdata/clusters/part-00000");
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
        path, Text.class, Kluster.class);

    for (int i = 0; i < k; i++) {
      Vector vec = vectors.get(i);
   
      
      final Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
      writer.append(new Text(cluster.getIdentifier()), cluster);
    }
    writer.close();

    KMeansDriver.run(conf, new Path("testdata/points"), new Path("testdata/clusters"),
      new Path("output"), new EuclideanDistanceMeasure(), 0.001, 10,
      true, 0, false);

    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
        new Path("output/" + Cluster.CLUSTERED_POINTS_DIR
                 + "/part-m-00000"), conf);

    IntWritable key = new IntWritable();
    WeightedVectorWritable value = new WeightedVectorWritable();
    while (reader.next(key, value)) {
      System.out.println(value.toString() + " belongs to cluster "
                         + key.toString());
    }
    reader.close();
  }

}