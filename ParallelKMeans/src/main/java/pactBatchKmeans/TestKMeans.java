package pactBatchKmeans;

import datagenerator.KMeansSampleDataGenerator;




public class TestKMeans {
  
  public static void main(String[] args) throws Exception {
    // GENERATE TEST DATA
 //   String numPoints = "1000";
 //   String numClusters = "5";
  //  KMeansSampleDataGenerator.main(new String[] { numPoints, numClusters });

    // RUN JOB
    String dataDir = "file:///Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/pactBatchKmeans/";
    String output = "file:///Users/prateekgaur/ParallelML/ParallelKMeans/src/main/java/pactBatchKmeans/";
    String numSubTasks = "1";
    String numIterations = "2";
    JobRunner runner = new JobRunner();
    // <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>
    String[] jobArgs = new String[] { 
        numSubTasks,
        dataDir + "points", 
        dataDir + "centers",           
        output, 
        numIterations
    };
    runner.runLocal(new KMeansIterative().getPlan(jobArgs));
  }

}
