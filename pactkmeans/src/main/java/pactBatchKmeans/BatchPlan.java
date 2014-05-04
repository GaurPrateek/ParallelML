package pactBatchKmeans;



import java.io.IOException;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

/**
 * 
 * @author PRATEEK GAUR (PGAUR19@GMAIL.COM)
 *
 */

public class BatchPlan implements Program, ProgramDescription {



	public static final float INVALID_DISTANCE_CUTOFF = -1;


	public static final String finalclusters = "parameter.FINAL_CLUSTERS";
	public static final String iconvergenceDelta = "parameter.CONVERGENCE_DELTA";
	public static final String maxIterations="parameter.MAX_ITERATIONS";
	public static final String distanceTechnique ="parameter.DISTANCE_TECHNIQUE";


	public static void main(String[] args)  {

		String inputPath = "file:///Users/prateekgaur/Documents/wthesismahout/pactkmeans/src/main/java/pactproject/pactkmeans/input";

		String clusterPath = "file:///Users/prateekgaur/Documents/wthesismahout/pactkmeans/src/main/java/pactproject/pactkmeans/clusters";

		String outputPath = "file:///Users/prateekgaur/Documents/wthesismahout/pactkmeans/src/main/java/pactproject/pactkmeans/output";



		System.out.println("Reading input points from " + inputPath);
		System.out.println("Reading input clusters from " + clusterPath);

		System.out.println("Writing output to " + outputPath);


		Plan toExecute = new BatchPlan().getPlan(args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8],args[9],args[10]);
		try {
			Util.executePlan(toExecute);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			Util.deleteAllTempFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);

	}

	public String getDescription() {
		// TODO Auto-generated method stub
		return "Usage: [inputVectors] [inputClusters] [outputPath] [number_of_Clusters] [DistanceMeasure] [Maximum_Iterations] [ConvergenceDelta] [OverrightOutputifpresent]"+
		"[Canopy] [executionMethod] [numSubtasks]";
	}

	public Plan getPlan(String... args) {
		// TODO Auto-generated method stub
		String inputPath = args.length >= 1 ? args[0] : "/input";
		String clusterInputPath = args.length >= 2 ? args[1] : "/clusterInput";
		String outputPath=args.length>=3? args[2]: "/output";
		String k=args.length>=4? args[3]: "7";
		String idistanceTechnique=args.length>=5? args[4]: "0";
		String iterations=args.length>=6?args[5]: "5";
		String convergencedelta = args.length >= 7 ?(args[6]) : "0.5";
		boolean overright=args.length>=8?Boolean.parseBoolean(args[7]): true;
		String canopy=args.length>=9?args[8]: "0.9f";
		int executionMethod=args.length>=10?Integer.parseInt(args[9]): 0;
		int subtasks=args.length>=11?Integer.parseInt(args[10]): 1;

		double clusterClassificationThreshold = 0.0;

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");


		MapOperator dfMapper = MapOperator.builder(BatchMapper.class)
				.input(source)
				.name("Onepass Mapper")
				.build();


		dfMapper.setParameter(iconvergenceDelta, convergencedelta);
		dfMapper.setParameter(maxIterations, iterations);
		dfMapper.setParameter(finalclusters, k);

		dfMapper.setParameter(distanceTechnique, idistanceTechnique);




		ReduceOperator dfReducer = ReduceOperator.builder(BatchReducer.class, IntValue.class, 0)
				.input(dfMapper)
				.name("Iterative Reducer")
				.build();

		dfReducer.setParameter(finalclusters, k);
		dfReducer.setParameter(maxIterations,iterations);


		dfReducer.setParameter(distanceTechnique, idistanceTechnique);

		FileDataSink sink = new FileDataSink(CsvOutputFormat.class, outputPath, dfReducer, "Centers");
		CsvOutputFormat.configureRecordFormat(sink)
		.recordDelimiter('\n')
		.fieldDelimiter(' ')
		.field(IntValue.class, 0) 
		.field(StringValue.class, 1); 

		Plan plan = new Plan(sink, "One Pass KMeans");
		plan.setDefaultParallelism(subtasks);

		return plan;
	}
}
