package ClusterUtils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

public class ClusterHelper {

	public static void writePointsToFile(List<Vector> points, Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				IntWritable.class, VectorWritable.class);
		int recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new IntWritable(recNum++), vec);
		}
		
		writer.close();
	}
	 public static void writeClusterInitialCenters(final Configuration configuration, final List<Vector> points, Path writerPath, int numberOfClusters)
		        throws IOException {
		      
		 
		        FileSystem fs = FileSystem.get(configuration);
		        SequenceFile.Writer writer = new SequenceFile.Writer(fs, configuration,
		                writerPath, Text.class, Kluster.class);
		        
		        for (int i = 0; i < numberOfClusters; i++) {
		            final Vector vec =  points.get(i);
		 
		            // write the initial centers
		            final Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
		            writer.append(new Text(cluster.getIdentifier()), cluster);
		        }
		        System.out.println("I came here as well");
		        writer.close();
		    }

	public static List<List<Cluster>> readClusters(Configuration conf, Path output)
			throws IOException {
		List<List<Cluster>> Clusters = Lists.newArrayList();
		FileSystem fs = FileSystem.get(output.toUri(), conf);

		for (FileStatus s : fs.listStatus(output, new ClustersFilter())) {
			List<Cluster> clusters = Lists.newArrayList();
			for (ClusterWritable value : new SequenceFileDirValueIterable<ClusterWritable>(
					s.getPath(), PathType.LIST, PathFilters.logsCRCFilter(),
					conf)) {
				Cluster cluster = value.getValue();
				clusters.add(cluster);
			}
			Clusters.add(clusters);
		}
		return Clusters;
	}
	
	  public static void readAndPrintOutputValues(final Configuration configuration, String output)
		        throws IOException {
		        final Path input = new Path(output + "/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000");
		 
		    
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
	
}