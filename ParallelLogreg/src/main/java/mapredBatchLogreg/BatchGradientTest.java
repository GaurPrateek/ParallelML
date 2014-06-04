package mapredBatchLogreg;

import mapredLocalLogreg.EvalJob;

import org.apache.hadoop.util.ToolRunner;

import Utils.RCV1DatasetInfo;
import de.tuberlin.dima.ml.datasets.DatasetInfo;



public class BatchGradientTest {




	private static final String INPUT_FILE_TRAIN = "/Users/prateekgaur/Downloads/mnist";

	private static final String OUTPUT_TRAIN_PATH = "/Users/prateekgaur/Downloads/output-batch-logreg";
	private static final String OUTPUT_TEST_PATH = "/Users/prateekgaur/Downloads/output-batch-logreg-test";
	private static final int MAX_ITERATIONS = 3;
	private static final String INPUT_FILE_TEST = "/Users/prateekgaur/Downloads/mnist.t";

	public static void main(String[] args) throws Exception {

		int labelDimension = 0;//DATASET.getLabelIdByName(TARGET_POSITIVE);

		double initial = 1;

		String hdfsAddress = "";
		String jobtrackerAddress = "";

		BatchGradientDriver bgDriver = new BatchGradientDriver(
				INPUT_FILE_TRAIN, 
				OUTPUT_TRAIN_PATH, 
				MAX_ITERATIONS,
				initial,
				labelDimension,
				781,
				hdfsAddress,
				jobtrackerAddress);
		try {
			//	  bgDriver.train();
		}catch(Exception e){
			System.out.println("-----");
			e.printStackTrace();
		}

				ToolRunner.run(new TrainingErrorJob(
						INPUT_FILE_TRAIN,
						OUTPUT_TEST_PATH,
						labelDimension,
						781), null);
//		ToolRunner.run(new EvalJob(
//				INPUT_FILE_TEST, 
//				OUTPUT_TEST_PATH,
//				OUTPUT_TRAIN_PATH+"/Users/prateekgaur/Downloads/output-batch-logreg/iteration2",
//				labelDimension), null); 
	}


}