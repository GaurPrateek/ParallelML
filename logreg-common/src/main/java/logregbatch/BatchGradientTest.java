package logregbatch;

import de.tuberlin.dima.ml.datasets.DatasetInfo;



public class BatchGradientTest {
  
  private static final DatasetInfo DATASET = RCV1DatasetInfo.get();
  
  private static final String TARGET_POSITIVE = "CCAT";

//  private static final String INPUT_FILE_TRAIN = "/Users/uce/Desktop/rcv1-v2/vectors/lyrl2004_vectors_train_5000.seq";
//  private static final String INPUT_FILE_TRAIN ="hdfs://localhost:9000/user/uce/rcv1/rcv1-v2/lyrl2004_vectors_train_5000.seq";
  private static final String INPUT_FILE_TRAIN = "/Users/prateekgaur/Downloads/sequencefiles/lyrl2004_vectors_train_2000.seq";
//  private static final String INPUT_FILE_TRAIN ="datasets/rcv1-v2/lyrl2004_vectors_train_5000.seq";
  
  private static final String OUTPUT_TRAIN_PATH = "output-aim3-batchgd";
  
  private static final int MAX_ITERATIONS = 3;

  public static void main(String[] args) throws Exception {
    
    int labelDimension = DATASET.getLabelIdByName(TARGET_POSITIVE);

    double initial = 1;
    
    String hdfsAddress = "";
    String jobtrackerAddress = "";
    
    BatchGradientDriver bgDriver = new BatchGradientDriver(
        INPUT_FILE_TRAIN, 
        OUTPUT_TRAIN_PATH, 
        MAX_ITERATIONS,
        initial,
        labelDimension,
        (int)DATASET.getNumFeatures(),
        hdfsAddress,
        jobtrackerAddress);
    
    bgDriver.train();
  }
  
}