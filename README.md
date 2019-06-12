<p style="text-align:center;"><img src="https://raw.githubusercontent.com/alessandrolulli/reforest/master/resources/img/reforest-logo.png" width="20%" height="20%"></p>

# ReForeSt
[![Build Status](https://travis-ci.org/alessandrolulli/reforest.svg?branch=master)](https://travis-ci.org/alessandrolulli/reforest)
[![Coverage Status](https://coveralls.io/repos/github/alessandrolulli/reforest/badge.svg)](https://coveralls.io/github/alessandrolulli/reforest)
[![license](https://img.shields.io/badge/license-APACHE%202.0-blue.svg)](https://img.shields.io/badge/license-APACHE%202.0-blue.svg)

 <p style="text-align: justify;">
ReForeSt is a distributed, scalable implementation of the RF learning algorithm which targets fast and memory efficient processing. ReForeSt main contributions are manifold: (i) it provides a novel approach for the RF implementation in a distributed environment targeting an in-memory efficient processing, (ii) it is faster and more memory efficient with respect to the de facto standard MLlib, (iii) the level of parallelism is self-configuring.
 </p>
 <p>
 ReForeSt and its documentation have been designed for developers and data scientists which are familiar with the <a href="https://spark.apache.org/">Spark Enviroment</a> and the <a href="https://spark.apache.org/mllib/">MLlib library</a>. Consequently please refer first to those documentation before starting with ReForeSt
 </p>
 <p>
 Additional details can be found at ReForeSt website: <a href="https://sites.google.com/view/reforest/home">https://sites.google.com/view/reforest/home</a>
 </p>

## Performance

Enreaching the comparison presented <a href="https://github.com/szilard/benchm-ml#random-forest">here</a> we report a comparison between many Random Forest Libraries, in the same setting of the link reported above, and ReForeSt

Tool    | *n*  |   Time (sec)  | RAM (GB) | AUC
-------------------------|------|---------------|----------|--------
R       | 10K  |      50       |   10     | 68.2
.       | 100K |     1200      |   35     | 71.2
.       | 1M   |     crash     |          |
Python  | 10K  |      2        |   2      | 68.4
.       | 100K |     50        |   5      | 71.4
.       | 1M   |     900       |   20     | 73.2
.       | 10M  |     crash     |          |
H2O     | 10K  |      15       |   2      | 69.8
.       | 100K |      150      |   4      | 72.5
.       | 1M   |      600      |    5     | 75.5
.       | 10M  |     4000      |   25     | 77.8
Spark   | 10K  |      50       |   10     | 69.1
.       | 100K |      270      |   30     | 71.3
.       | 1M   |  crash/2000   |          | 71.4
xgboost | 10K  |     4         |    1     | 69.9
.       | 100K |    20         |    1     | 73.2
.       | 1M   |    170        |    2     | 75.3
.       | 10M  |    3000       |    9     | 76.3
ReForeSt| 10K  |      32       |   5      | 68.7
.       | 100K |      123      |   40     | 72.1
.       | 1M   |  534          |  80      | 73.2
.       | 10M  |   2732        |   125    | 75.4

## How to build 

An already packaged ReForeSt in zip or tar.gz format can be found in the directory "resources/package".
Otherwise it is possible to build ReForeSt using Maven:
```
mvn clean package
```

## How to train a random forest with ReForeSt

```
import reforest.rf.{RFProperty, RFRunner}

// Create the ReForeSt configuration.
val property = RFParameterBuilder.apply
  .addParameter(RFParameterType.Dataset, "data/test10k-labels")
  .addParameter(RFParameterType.NumFeatures, 794)
  .addParameter(RFParameterType.NumClasses, 10)
  .addParameter(RFParameterType.NumTrees, 100)
  .addParameter(RFParameterType.Depth, 10)
  .addParameter(RFParameterType.BinNumber, 32)
  .addParameter(RFParameterType.SparkMaster, "local[4]")
  .addParameter(RFParameterType.SparkCoresMax, 4)
  .addParameter(RFParameterType.SparkPartition, 4 * 4)
  .addParameter(RFParameterType.SparkExecutorMemory, "4096m")
  .addParameter(RFParameterType.SparkExecutorInstances, 1)
  .build

val sc = CCUtil.getSparkContext(property)

// Create the Random Forest classifier.
val timeStart = System.currentTimeMillis()
val rfRunner = ReForeStTrainerBuilder.apply(property).build(sc)

// Train a Random Forest model.
val model = rfRunner.trainClassifier()
val timeEnd = System.currentTimeMillis()

// Evaluate model on test instances and compute test error
val labelAndPreds = rfRunner.getDataLoader.getTestingData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / rfRunner.getDataLoader.getTestingData.count()

println("Accuracy: "+(1 - testErr))
println("Time: " + (timeEnd - timeStart))
rfRunner.sparkStop()
```

## Authors
* <a href="http://www.lucaoneto.com">Luca Oneto</a>
* <a href="https://scholar.google.it/citations?user=ky3gHmYAAAAJ&hl=it">Francesca Cipollini</a>
* <a href="http://for.unipi.it/alessandro_lulli/">Alessandro Lulli</a>
* <a href="http://www.dibris.unige.it/anguita-davide">Davide Anguita</a>

ReForeSt has been developed at <a href="http://www.smartlab.ws">Smartlab</a> - <a href="http://www.dibris.unige.it/">DIBRIS</a> - <a href="https://unige.it/">University of Genoa</a>.
