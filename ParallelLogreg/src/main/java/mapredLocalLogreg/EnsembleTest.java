package mapredLocalLogreg;

import org.apache.hadoop.util.ToolRunner;

import Utils.RCV1DatasetInfo;
import de.tuberlin.dima.ml.datasets.DatasetInfo;


public class EnsembleTest {

	private static final DatasetInfo DATASET = RCV1DatasetInfo.get();

	private static final int ENSEMBLE_SIZE = 1;

	// Currently we train a hardcoded single 1-vs-all classifier
	private static final String TARGET_POSITIVE = "CCAT";

	private static final String INPUT_FILE_TRAIN = "/Users/prateekgaur/Desktop/cod-rna";
	//  private static final String INPUT_FILE_TRAIN = "rcv1-v2/lyrl2004_vectors_train_5000.seq";

	private static final String INPUT_FILE_TEST = "/Users/prateekgaur/Downloads/cod-rna.t";
	//  private static final String INPUT_FILE_TEST = "rcv1-v2/lyrl2004_vectors_test_5000.seq";

	private static final String OUTPUT_TRAIN_PATH = "/Users/prateekgaur/Downloads/output-logreg-ensemble";
	private static final String OUTPUT_TEST_PATH = "/Users/prateekgaur/Downloads/output-logreg-validation";

	public static void main(String[] args) throws Exception {

		int labelDimension = DATASET.getLabelIdByName(TARGET_POSITIVE);

		ToolRunner.run(new EnsembleJob(
				INPUT_FILE_TRAIN, 
				OUTPUT_TRAIN_PATH, 
				ENSEMBLE_SIZE,
				labelDimension,
				/* (int)DATASET.getNumFeatures()*/11), null);

		ToolRunner.run(new EvalJob(
				INPUT_FILE_TEST, 
				OUTPUT_TEST_PATH,
				OUTPUT_TRAIN_PATH,
				labelDimension), null); 
	}


}
