package pactlocallogreg;

import java.io.IOException;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.nephele.client.JobExecutionResult;


public class EnsembleRun {

	//private static final String INPUT_FILE_TRAIN = "file:///Users/prateekgaur/Desktop/cod-rna";

	//	private static final String INPUT_FILE_TEST = "file:///Users/prateekgaur/Downloads/cod-rna.t";
	private static final String INPUT_FILE_TRAIN = "file:///Users/prateekgaur/Downloads/mnist";
	private static final String INPUT_FILE_TEST = "file:///Users/prateekgaur/Downloads/mnist.t";
	private static final String OUTPUT_TRAIN_PATH = "file:///Users/prateekgaur/Desktop/output-pactlogreg-ensemble";
	private static final String OUTPUT_TEST_PATH = "file:///Users/prateekgaur/Desktop/output-logreg-validation";

	public static void main(String[] args) throws Exception  {


		System.out.println("Reading input from " + INPUT_FILE_TRAIN);
		System.out.println("Writing output to " + OUTPUT_TRAIN_PATH);


		Plan toExecute = new EnsembleJob().getPlan( "1", INPUT_FILE_TRAIN, INPUT_FILE_TEST, OUTPUT_TRAIN_PATH, "1000","1" );


		JobExecutionResult result = LocalExecutor.execute(toExecute);

		try {
			Util.deleteAllTempFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);

	}



}
