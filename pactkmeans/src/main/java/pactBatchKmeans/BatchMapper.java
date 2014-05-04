package pactBatchKmeans;

/**
 * 
 * @author PRATEEK GAUR (PGAUR19@GMAIL.COM)
 *
 */

import java.util.Iterator;
import java.util.List;

import org.apache.mahout.common.distance.ChebyshevDistanceMeasure;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.MahalanobisDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.MinkowskiDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.common.distance.WeightedEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.WeightedManhattanDistanceMeasure;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.BruteSearch;
import org.apache.mahout.math.neighborhood.FastProjectionSearch;
import org.apache.mahout.math.neighborhood.LocalitySensitiveHashSearch;
import org.apache.mahout.math.neighborhood.ProjectionSearch;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;
import org.apache.mahout.clustering.ClusteringUtils;
import org.apache.mahout.clustering.streaming.cluster.StreamingKMeans;

import com.google.common.collect.Lists;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;





public class BatchMapper extends MapFunction {

	// ----------------------------------------------------------------------------------------------------------------

	private static final int NUM_ESTIMATE_POINTS = 1000;
	private StreamingKMeans clusterer;
	
	private List<Centroid> estimatePoints;
	private boolean estimateDistanceCutoff = false;
	Collector<Record> cachedCollector = null;

	private int numPoints=0;



	

	double emaxClusterSize = 0;
	double estimatedDistanceCutoff = 0;
	int ekappa = 0;

	int esearcherTechnique = 0;
	int edistanceTechnique = 0;
	int enumProjections = 0;
	int esearchSize = 0;

	public static final String maxClusterSize = "parameter.MAX_CLUSTER_SIZE";
	public static final String kappa = "parameter.KAPPA";
	public static final String estDistanceCutoff = "parameter.MAX_FACILITY_COST_INCREMENT";

	public static final String searcherTechnique ="parameter.SEARCHER_TECHNIQUE";
	public static final String distanceTechnique ="parameter.DISTANCE_TECHNIQUE";
	public static final String numProjections = "parameter.NUM_PROJECTIONS";
	public static final String searchSize = "parameter.SEARCH_SIZE";




	@Override
	public void open(Configuration parameters) {
		emaxClusterSize = Double.parseDouble(parameters.getString(maxClusterSize, "1.1"));
		estimatedDistanceCutoff=Double.parseDouble(parameters.getString(estDistanceCutoff, "0.1"));
		ekappa = parameters.getInteger(kappa, 1);

		esearcherTechnique =Integer.parseInt(parameters.getString(searcherTechnique, "0"));
		edistanceTechnique =Integer.parseInt(parameters.getString(distanceTechnique, "0"));
		enumProjections = Integer.parseInt(parameters.getString(numProjections, "0"));
		esearchSize = Integer.parseInt(parameters.getString(searchSize , "0"));


		DistanceMeasure distanceMeasure = null;
		UpdatableSearcher searcher = null;


		switch(edistanceTechnique){
		case 0:
			distanceMeasure =  new SquaredEuclideanDistanceMeasure();
			break;
		case 1:
			distanceMeasure =  new CosineDistanceMeasure();
			break;
		case 2:
			distanceMeasure =  new ChebyshevDistanceMeasure();
			break;
		case 3:
			distanceMeasure =  new EuclideanDistanceMeasure();
			break;
		case 4:
			distanceMeasure =  new ManhattanDistanceMeasure();
			break;
		case 5:
			distanceMeasure =  new MahalanobisDistanceMeasure();
			break;
		case 6:
			distanceMeasure =  new MinkowskiDistanceMeasure();
			break;
		case 7:
			distanceMeasure =  new TanimotoDistanceMeasure();
			break;
		case 8:
			distanceMeasure =  new WeightedEuclideanDistanceMeasure();
			break;
		case 9:
			distanceMeasure =  new WeightedManhattanDistanceMeasure();
			break;
		}


		switch(esearcherTechnique){
		case 0:
			searcher = new BruteSearch(distanceMeasure);
			break;
		case 1:
			searcher = new ProjectionSearch(distanceMeasure, enumProjections, esearchSize);
			break;
		case 2:
			searcher = new FastProjectionSearch(distanceMeasure, enumProjections, esearchSize);
			break;
		case 3:
			searcher = new LocalitySensitiveHashSearch(distanceMeasure, esearchSize);
			break;
		}


		if (estimatedDistanceCutoff == BatchPlan.INVALID_DISTANCE_CUTOFF) {
		      estimateDistanceCutoff = true;
		      estimatePoints = Lists.newArrayList();
		    }
		    // There is no way of estimating the distance cutoff unless we have some data.
		    clusterer = new StreamingKMeans(searcher, ekappa, estimatedDistanceCutoff);

	//	sk = new SKMeans(searcher, ekappa, emaxClusterSize, estimatedDistanceCutoff);



	}

	  private void clusterEstimatePoints() {
		    clusterer.setDistanceCutoff(ClusteringUtils.estimateDistanceCutoff(
		        estimatePoints, clusterer.getDistanceMeasure()));
		    clusterer.cluster(estimatePoints);
		    estimateDistanceCutoff = false;
		  }
	@Override
	public void map(Record record, Collector<Record> collector) {

		long startTime = System.currentTimeMillis();


		UpdatableSearcher centroids;
		if( cachedCollector == null)
			cachedCollector = collector;


		String document = record.getField(0, StringValue.class).toString();

		String[] content= document.split(",");

		double[] pointArray = new double[content.length];

		for(int i=0;i<content.length;i++)
		{
			pointArray[i]=Double.parseDouble(content[i]);
		}

		DenseVector randomDenseVector = new DenseVector(pointArray);
		Centroid newCentroid = new Centroid(numPoints, randomDenseVector);

		if (estimateDistanceCutoff) {
		      if (numPoints < NUM_ESTIMATE_POINTS) {
		        estimatePoints.add(newCentroid);
		      } else if (numPoints == NUM_ESTIMATE_POINTS) {
		        clusterEstimatePoints();
		      }
		    } else {
		      clusterer.cluster(newCentroid);
		    }
		
		numPoints++;
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("mapper: "+numPoints+" finished in "+totalTime+" ms");
	}

	public void close(){

		
		// We should cluster the points at the end if they haven't yet been clustered.
	    if (estimateDistanceCutoff) {
	      clusterEstimatePoints();
	    }

	    // Reindex the centroids before passing them to the reducer.
	    clusterer.reindexCentroids();
	    StringBuffer outball;
	  
	Iterator<Centroid> itx=	clusterer.iterator();
	
	while( itx.hasNext() ){
		Centroid next = itx.next();
		outball=new StringBuffer();

		
		for(int j=0;j<next.size()-1;j++)
		{
			outball.append(String.valueOf(next.get(j))+"#");
		}
		outball.append(next.get(next.size()-1));


		Record outPactRec=new Record();
		StringValue outString=new StringValue(outball.toString());
		IntValue one=new IntValue(1);

		outPactRec.setField(0, one);
		outPactRec.setField(1, outString);

		cachedCollector.collect(outPactRec);
	}
/*		while( ite.hasNext() ){
			Vector next = ite.next();
			outball=new StringBuffer();

			for(int j=0;j<next.size()-1;j++)
			{
				outball.append(String.valueOf(next.getQuick(j))+"#");
			}
			outball.append(next.getQuick(next.size()-1));


			Record outPactRec=new Record();
			StringValue outString=new StringValue(outball.toString());
			IntValue one=new IntValue(1);

			outPactRec.setField(0, one);
			outPactRec.setField(1, outString);

			cachedCollector.collect(outPactRec);
		}*/
	}
}