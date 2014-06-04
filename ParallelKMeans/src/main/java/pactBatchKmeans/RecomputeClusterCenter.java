/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package pactBatchKmeans;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * Reduce PACT computes the new position (coordinate vector) of a cluster
 * center. This is an average computation. Hence, Combinable is annotated
 * and the combine method implemented. 
 * 
 * Output Format:
 * 0: clusterID
 * 1: clusterVector
 */

@Combinable
@ConstantFields(0)
public class RecomputeClusterCenter extends ReduceFunction implements Serializable {
	private static final long serialVersionUID = 1L;

	private final IntValue count = new IntValue();

	/**
	 * Compute the new position (coordinate vector) of a cluster center.
	 */


	@Override
	public void reduce(Iterator<Record> dataPoints, Collector<Record> out) {
		Record next = null;
		//	/	System.out.println("Reaching Reduce");
		// initialize coordinate vector sum and count
		PactVector coordinates = null;
		double[] coordinateSum = null;
		int count = 0;	

		// compute coordinate vector sum and count
		while (dataPoints.hasNext()) {
			next = dataPoints.next();

			// get the coordinates and the count from the record
			PactVector x = next.getField(1, PactVector.class);

			Vector coord=x.getValue();
			double[] thisCoords = new double[coord.size()];
			for(int i=1;i<coord.size();i++)
				thisCoords[i] = coord.get(i); 


			int thisCount = next.getField(2, IntValue.class).getValue();

			if (coordinateSum == null) {

				if (coordinates!=null) {

					//	coordinateSum = coordinates.getCoordinates();
					Vector coords=coordinates.getValue();
					coordinateSum=new double[coords.size()];
					for(int i=1;i<coords.size();i++)
						coordinateSum[i] = coords.get(i); 
				}
				else {
					coordinateSum = new double[thisCoords.length];
				}
			}

			addToCoordVector(coordinateSum, thisCoords);
			count += thisCount;
		}

		// compute new coordinate vector (position) of cluster center
		for (int i = 1; i < coordinateSum.length; i++) {
			coordinateSum[i] /= count;
		}
		Vector newcoordinates = new RandomAccessSparseVector(5);
		int centerid=next.getField(0, IntValue.class).getValue();
		StringBuffer x=new StringBuffer();
		x.append(centerid+" ");
		for(int i=1;i<coordinateSum.length;i++)
		{
			newcoordinates.set(i, coordinateSum[i]); 
			x.append(i+":"+coordinateSum[i]+" ");
		}


		coordinates=new PactVector(newcoordinates);
		StringValue coordString=new StringValue(x);

		next.setField(0, coordString);

		//next.setNull(2);

		//	System.out.println("next"+next.getField(1, StringValue.class)+next.getField(0, IntValue.class));
		// emit new position of cluster center
		out.collect(next);
	}

	/**
	 * Computes a pre-aggregated average value of a coordinate vector.
	 */
	@Override
	public void combine(Iterator<Record> dataPoints, Collector<Record> out) {

		Record next = null;
		//		System.out.println("Reaching Combine");
		// initialize coordinate vector sum and count
		PactVector coordinates = null;
		double[] coordinateSum = null;
		int count = 0;	
		double[] thisCoords = null;
		// compute coordinate vector sum and count
		while (dataPoints.hasNext()) {
			next = dataPoints.next();

			// get the coordinates and the count from the record
			PactVector x=next.getField(1, PactVector.class);
			Vector coord=x.getValue();
			thisCoords=new double[coord.size()];
			for(int i=1;i<coord.size();i++)
				thisCoords[i] = coord.get(i); 

			//	System.out.println("thiscoords "+ thisCoords.length );

			int thisCount = next.getField(2, IntValue.class).getValue();

			if (coordinateSum == null) {

				if (coordinates!=null) {


					//	coordinateSum = coordinates.getCoordinates();
					Vector coords=coordinates.getValue();
					coordinateSum=new double[coords.size()];
					for(int i=1;i<coords.size();i++)
						coordinateSum[i] = coords.get(i); 
				}
				else {

					coordinateSum = new double[thisCoords.length];
				}
			}
			addToCoordVector(coordinateSum, thisCoords);
			count += thisCount;
		}


		Vector newcoordinates = new RandomAccessSparseVector(5);

		for(int i=1;i<coordinateSum.length;i++)
			newcoordinates.set(i, coordinateSum[i]); 
		coordinates=new PactVector(newcoordinates);
		this.count.setValue(count);
		next.setField(1, coordinates);
		next.setField(2, this.count);

		// emit partial sum and partial count for average computation

		out.collect(next);
	}

	/**
	 * Adds two coordinate vectors by summing up each of their coordinates.
	 * 
	 * @param cvToAddTo
	 *        The coordinate vector to which the other vector is added.
	 *        This vector is returned.
	 * @param cvToBeAdded
	 *        The coordinate vector which is added to the other vector.
	 *        This vector is not modified.
	 */
	private void addToCoordVector(double[] cvToAddTo, double[] cvToBeAdded) {

		// check if both vectors have same length
		if (cvToAddTo.length != cvToBeAdded.length) {
			throw new IllegalArgumentException("The given coordinate vectors are not of equal length.");
		}

		// sum coordinate vectors coordinate-wise
		for (int i = 0; i < cvToAddTo.length; i++) {
			cvToAddTo[i] += cvToBeAdded[i];
		}
	}
}