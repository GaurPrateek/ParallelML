ParallelML
==========

Contains the parallel Implementations of Logistic Regression and KMeans on Hadoop and Stratosphere.

Maven modules:

1) logreg-common: Common functionality regarding Machine Learning and Logistic Regression. Also contains some sequential implementations.

2) logreg-mapred: Hadoop Jobs for Logistic Regression

Ensemble + SGD (mahout) Training
Iterative Batch Gradient descent training
How to run logistic regression on Hadoop (Mahout)

How to run logistic regression on stratosphere

1) Ensemble + SGD (mahout) Training

Parameters: [numPartitions] [inputPathTrain] [inputPathTest] [outputPath] [numFeatures] [runValidation (0 or 1)]

example: bin/stratosphere run -j logreg-pact-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c de.tuberlin.dima.ml.pact.logreg.ensemble.EnsembleJob -a 1 file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr-out 3 0

2) Iterative Batch Gradient descent training

Parameters: [numSubTasks] [inputPathTrain] [inputPathTest] [outputPath] [numIteration] [runValidation (0 or 1)] [learningRate] [positiveClass] [numFeatuer]

example: bin/stratosphere run -j logreg-pact-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c de.tuberlin.dima.ml.pact.logreg.batchgd.BatchGDPlanAssembler -a 1 file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr-out 10 0 0.05 1 3
