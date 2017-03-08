# ReForeSt
=======
Abstract. Random Forests (RF) of tree classifiers are a popular ensemble method for classification. RF are usually preferred with respect to other classification techniques because of their limited hyperparameter sensitivity, high numerical robustness, native capacity of dealing with numerical and categorical features, and effectiveness in many real world classification problems. In this work we present ReForeSt, a Random Forests Apache Spark implementation which is easier to tune, faster, and less memory consuming with respect to MLlib, the de facto standard Apache Spark machine learning library. We perform an extensive comparison between ReForeSt and MLlib by taking advantage of the Google Cloud Platform. In particular, we test ReForeSt and MLlib with different library settings, on different real world datasets, and with a different number of machines and types. Results confirm that ReForeSt outperforms MLlib in all the above mentioned aspects.


### Publications

**2017 - **

Lulli, Alessandro and Oneto, Luca and Anguita, Davide
**ReForeSt: Random Forest in Apache Spark**
 (2017) (to appear).

```
```

### How to build

```
mvn clean package
```

### How to configure
It is required a configuration file.
A minimal configuration file is the following:

```
dataset /path-to-dataset/dataset

numFeatures 794
numClasses 10

jarPath /path-to-jar/reforest.jar

sparkMaster spark://<ip-master>:7077
sparkExecutorInstances 3
sparkExecutorMemory 512m
sparkCoresMax 6
```

Detailed description of available configuration variables:
variable | default value | description
------------ | ------------- | -------------
dataset | <mandatory> | path to the dataset in SVM format
numFeatures | <mandatory> | number of features in the dataset
numTrees | 3 | number of trees in the forest
maxDepth | 3 | maximum depth of each tree
numClasses | 2 | number of classes in the dataset (it is required the first class being "0", the second "1", and so on)
binNumber | 32 | number of bins to discretize input
maxNodesConcurrent | -1 | if -1 ReForeSt automatically compute the maximum number of nodes to be computed in an iteration
instrumented | false | if you want to run the Garbage Collector to get memory statistics (available in RFAllInLocalData)
skipAccuracy | false | if you want only to train the classifier (available in RFAllInLocalData)
typeDataInput | Double | type of data to use to load the input values (available in RFAllInLocalData) (Double, Float, Float16, Short)
sparkMaster | <mandatory> | IP address of the Apache Spark Master
sparkExecutorInstances | <mandatory> | number of worker machines
sparkExecutorMemory | <mandatory> | amount of memory of each worker machine
sparkCoresMax | <mandatory> | total number of cores to use


### How to run

```

```