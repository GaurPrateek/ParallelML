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

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;
import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
/**
 * Cross PACT computes the distance of all data points to all cluster
 * centers.
 */
@ConstantFieldsFirst({0,1})
public class ComputeDistance extends CrossFunction implements Serializable {
	private static final long serialVersionUID = 1L;

	private final DoubleValue distance = new DoubleValue();

	/**
	 * Computes the distance of one data point to one cluster center.
	 * 
	 * Output Format:
	 * 0: pointID
	 * 1: pointVector
	 * 2: clusterID
	 * 3: distance
	 */
	
	@Override
	public void cross(Record dataPointRecord, Record clusterCenterRecord, Collector<Record> out) {
		Vector dataPoint;
	
		String document = dataPointRecord.getField(0, StringValue.class).toString();


		dataPoint = new RandomAccessSparseVector(5);
		LibSvmVectorReader.readVectorSingleLabel(dataPoint, document);
	//	System.out.println("* "+document);
		int pointid=Integer.parseInt(document.split(" ")[0]);
		//PactVector dataPoint = dataPointRecord.getField(1, PactVector.class);
		Vector clusterPoint;
		String documentline = clusterCenterRecord.getField(0, StringValue.class).toString();

		System.out.println("#"+documentline);
		clusterPoint = new RandomAccessSparseVector(5);
		LibSvmVectorReader.readVectorSingleLabel(clusterPoint, documentline);
	//	System.out.println("#"+documentline);
		int clusterCenterid=Integer.parseInt(documentline.split(" ")[0]);
		

		EuclideanDistanceMeasure dist=new EuclideanDistanceMeasure();
		this.distance.setValue(dist.distance(dataPoint, clusterPoint));

		// add cluster center id and distance to the data point record 
		dataPointRecord.setField(0,new IntValue( pointid));
		dataPointRecord.setField(1,new PactVector( dataPoint));
		dataPointRecord.setField(2, new IntValue(clusterCenterid));
		dataPointRecord.setField(3, this.distance);

	//	System.out.println("record:"+ dataPointRecord.getField(0,IntValue.class)+"," 
	//	+dataPointRecord.getField(1,PactVector.class)+","+dataPointRecord.getField(2, IntValue.class)+","+dataPointRecord.getField(3, DoubleValue.class));
		out.collect(dataPointRecord);
	}
}