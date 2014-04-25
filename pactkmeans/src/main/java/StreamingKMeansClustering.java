import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.HadoopUtil;
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
		
			Vector vec = new DenseVector(raw[i].length);
			vec.assign(raw[i]);
			points.add(vec);
		}
		return points;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String BASE_PATH = "/Users/prateekgaur/Desktop/km";
		String POINTS_PATH = BASE_PATH + "/points";
		String CLUSTERS_PATH = BASE_PATH + "/kmclusters";
		String OUTPUT_PATH = BASE_PATH + "/kmoutput";
		String SKMOUTPUT_PATH = BASE_PATH + "/skmoutput";
		Configuration conf = new Configuration();
        // Create input directories for data
        final File pointsDir = new File(POINTS_PATH);
      
        if (!pointsDir.exists()) {
            pointsDir.mkdir();
        }
        List<Vector> vectors = getPoints(points);
		FileSystem fs = FileSystem.get(conf);
		ClusterHelper.writePointsToFile(vectors, conf, new Path(POINTS_PATH));
		ClusterHelper.writeClusterInitialCenters(conf, vectors,new Path(CLUSTERS_PATH),2);

	     // Run K-means algorithm
        final Path inputPath = new Path(POINTS_PATH);
        final Path clustersPath = new Path(CLUSTERS_PATH);
        final Path outputPath = new Path(OUTPUT_PATH);
        HadoopUtil.delete(conf, outputPath);
  	  HadoopUtil.delete(conf, new Path(SKMOUTPUT_PATH));

		String[] args1 = new String[] {"-i",POINTS_PATH,"-o",SKMOUTPUT_PATH,"--estimatedNumMapClusters","4","--searchSize","2","-k","3", "-testp", "0.2", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.CosineDistanceMeasure"};
	
		  
	       
		   
	        KMeansDriver.run(conf, inputPath, clustersPath, outputPath, 0.001, 10, true, 0, false);

		// Read and print output values
		ClusterHelper.readAndPrintOutputValues(conf,OUTPUT_PATH);
		try {
	//		StreamingKMeansDriver.main(args1);
		//	ClusterHelper.readAndPrintOutputValues(conf,SKMOUTPUT_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
