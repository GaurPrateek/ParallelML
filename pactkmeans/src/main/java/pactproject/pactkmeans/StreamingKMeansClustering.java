package pactproject.pactkmeans;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;


public class StreamingKMeansClustering {
	
	public static final double[][] points = { {1, 1}, {2, 1}, {1, 2},
		{2, 2}, {3, 3}, {8, 8},
		{9, 8}, {8, 9}, {9, 9}};


	public static List<Vector> getPoints(double[][] raw) {
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
//			Vector vec = new RandomAccessSparseVector(fr.length);
//			vec.assign(fr);
			Vector vec = new DenseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}
		return points;
	}

	public static void main(String[] args) throws IOException {
		List<Vector> vectors = getPoints(points);
	    
	    File testData = new File("/home/faisal/drive/data/testdata");
	    if (!testData.exists()) {
	      testData.mkdir();
	    }
	    testData = new File("/home/faisal/drive/data/testdata/points");
	    if (!testData.exists()) {
	      testData.mkdir();
	    }
	    
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
	    ClusterHelper.writePointsToFile(vectors, conf, new Path("/home/faisal/drive/data/testdata/points/file1"));
	    
	    File resultDir = new File("/home/faisal/drive/data/SKM-Main-result");
	    if (resultDir.exists()) {
	    	resultDir.delete();
	    }
//		String[] args1 = new String[] {"-i","/home/faisal/drive/data/input","-o","/home/faisal/drive/data/SKM-Main-result/","--estimatedNumMapClusters","200","--searchSize","2","-k","3", "-testp", "0.2", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.CosineDistanceMeasure"};

		try {
			StreamingKMeansDriver.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
