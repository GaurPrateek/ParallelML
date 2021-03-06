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

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;


public class KMeansIterative implements Program, ProgramDescription {


	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String dataPointInput = (args.length > 1 ? args[1] : "");
		final String clusterInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int numIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		// create DataSourceContract for cluster center input
		FileDataSource initialClusterPoints = new FileDataSource(new TextInputFormat(), clusterInput, "Centers");
		initialClusterPoints.setDegreeOfParallelism(1);

		BulkIteration iteration = new BulkIteration("K-Means Loop");
		iteration.setInput(initialClusterPoints);
		iteration.setMaximumNumberOfIterations(numIterations);

		// create DataSourceContract for data point input
		FileDataSource dataPoints = new FileDataSource(new TextInputFormat(), dataPointInput, "Data Points");


		// create CrossOperator for distance computation
		CrossOperator computeDistance = CrossOperator.builder(new ComputeDistance())
				.input1(dataPoints)
				.input2(iteration.getPartialSolution())
				.name("Compute Distances")
				.build();

		System.out.println("looping through");
		// create ReduceOperator for finding the nearest cluster centers
		ReduceOperator findNearestClusterCenters = ReduceOperator.builder(new FindNearestCenter(), IntValue.class, 0)
				.input(computeDistance)
				.name("Find Nearest Centers")
				.build();

		// create ReduceOperator for computing new cluster positions
		ReduceOperator recomputeClusterCenter = ReduceOperator.builder(new RecomputeClusterCenter(), IntValue.class, 0)
				.input(findNearestClusterCenters)
				.name("Recompute Center Positions")
				.build();
		iteration.setNextPartialSolution(recomputeClusterCenter);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink finalResult = new FileDataSink(new CsvOutputFormat(), output, iteration, "New Center Positions");
		CsvOutputFormat.configureRecordFormat(finalResult)
		.recordDelimiter('\n')
		.fieldDelimiter(' ')
		//.field(IntValue.class, 1) 
		.field(StringValue.class, 0); 
		// return the PACT plan
		Plan plan = new Plan(finalResult, "Iterative KMeans");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}


	public String getDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
	}
}