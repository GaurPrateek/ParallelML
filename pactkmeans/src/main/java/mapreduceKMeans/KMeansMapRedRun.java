package mapreduceKMeans;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class KMeansMapRedRun {
 
    // ---- Static
 
    private static final Logger LOG = LoggerFactory.getLogger(KMeansMapRedRun.class);
    private static final String BASE_PATH = "/Users/prateekgaur/Desktop";
    private static final String POINTS_PATH = BASE_PATH + "/points";
    private static final String CLUSTERS_PATH = BASE_PATH + "/clusters";
    private static final String OUTPUT_PATH = BASE_PATH + "/output";
    private static final String SKMOUTPUT_PATH = BASE_PATH + "/skmoutput";
 
    public static void main(final String[] args) {
    	
        final KMeansMapRedRun application = new KMeansMapRedRun();
 
        try {
            application.start();
        }
        catch (final Exception e) {
            LOG.error("MahoutTryIt failed", e);
        }
    }
 
    // ---- Fields
 
    private final double[][] points =
        { { 1, 1 }, { 2, 1 }, { 1, 2 },
        { 2, 2 }, { 3, 3 }, { 8, 8 },
        { 9, 8 }, { 8, 9 }, { 9, 9 } };
 
    private final int numberOfClusters = 2;
 
    // ---- Methods
 
    private void start()
        throws Exception {
 
        final Configuration configuration = new Configuration();
 
        // Create input directories for data
        final File pointsDir = new File(POINTS_PATH);
        if (!pointsDir.exists()) {
            pointsDir.mkdir();
        }
 
        // read the point values and generate vectors from input data
       
       List  vectors = vectorize(points);
       
        // Write data to sequence hadoop sequence files
        writePointsToFile( vectors,configuration);
        
        // Write initial centers for clusters
        writeClusterInitialCenters(configuration, vectors,numberOfClusters);
      
        // Run K-means algorithm
        final Path inputPath = new Path(POINTS_PATH);
        final Path clustersPath = new Path(CLUSTERS_PATH);
        final Path outputPath = new Path(OUTPUT_PATH);
        HadoopUtil.delete(configuration, outputPath);
 
        String[] args1 = new String[] {"-i",POINTS_PATH,"-o",SKMOUTPUT_PATH,"--estimatedNumMapClusters","4","--searchSize","2","-k","3", "-testp", "0.2", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.CosineDistanceMeasure"};
    	
		
        KMeansDriver.run(configuration, inputPath, clustersPath, outputPath, 0.001, 10, true, 0, false);
 
        // Read and print output values
        readAndPrintOutputValues(configuration);
    //	StreamingKMeansDriver.main(args1);
    }
 
    public static void writePointsToFile(List<Vector> points, Configuration configuration)
        throws IOException {
 
        final Path path = new Path(POINTS_PATH + "/pointsFile");
        FileSystem fs = FileSystem.get(configuration);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, configuration,
                path, IntWritable.class, VectorWritable.class);
        
        
        int recNum = 0;
        final VectorWritable vec = new VectorWritable();
        
        for ( int i=0;i<points.size();i++) {
            
        	vec.set((Vector) points.get(i));
            writer.append(new IntWritable(recNum++), vec);
        }
 
        writer.close();
    }
 
    public static void writeClusterInitialCenters(final Configuration configuration, final List points, int numberOfClusters)
        throws IOException {
        final Path writerPath = new Path(CLUSTERS_PATH + "/part-00000");
 
        FileSystem fs = FileSystem.get(configuration);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, configuration,
                writerPath, Text.class, Kluster.class);
        
        for (int i = 0; i < numberOfClusters; i++) {
            final Vector vec = (Vector) points.get(i);
 
            // write the initial centers
            final Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
 
        writer.close();
    }
 
    private void readAndPrintOutputValues(final Configuration configuration)
        throws IOException {
        final Path input = new Path(OUTPUT_PATH + "/" + /*Cluster.CLUSTERED_POINTS_DIR +*/ "/part-r-00000");
 

        System.out.println("i am here"); 
        FileSystem fs = FileSystem.get(configuration);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, input,
                configuration);
        final IntWritable key = new IntWritable();
        final WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
 
        while (reader.next(key, value)) {
            System.out.println("{"+value.toString()+"} belongs to cluster {"+key.toString()+"}");
        }
        reader.close();
    }
 
    // Read the points to vector from 2D array
    public List vectorize(final double[][] raw) {
        final List points = new ArrayList();
 
        for (int i = 0; i < raw.length; i++) {
            final Vector vec = new RandomAccessSparseVector(raw[i].length);
            vec.assign(raw[i]);
            points.add(vec);
        }
 
        return points;
    }
}