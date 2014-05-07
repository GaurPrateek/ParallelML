package pactBatchKmeans;


import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.JoinFunction;

import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public final class JoinOldAndNew extends JoinFunction implements Serializable {
	private static final long serialVersionUID = 1L;


	private final DoubleValue distance = new DoubleValue();
	@Override
	public void join(Record centroid1, Record centroid2, Collector<Record> out) throws Exception {

		CoordVector dataPoint = centroid1.getField(1, CoordVector.class);
		CoordVector clusterPoint = centroid2.getField(1, CoordVector.class);

		this.distance.setValue(dataPoint.computeEuclidianDistance(clusterPoint));


		if(Math.abs(distance.getValue()) ==0)
		{

			out.collect(centroid1);
		}

	
	}
}