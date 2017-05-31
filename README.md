<img src="https://raw.githubusercontent.com/alessandrolulli/reforest/master/resources/img/reforest-logo.png">

# ReForeSt
[![Build Status](https://travis-ci.org/alessandrolulli/reforest.svg?branch=master)](https://travis-ci.org/alessandrolulli/reforest)
[![license](https://img.shields.io/badge/license-APACHE%202.0-blue.svg)](https://img.shields.io/badge/license-APACHE%202.0-blue.svg)

 <p style="text-align: justify;">
ReForeSt is a distributed, scalable implementation of the RF learning algorithm which targets fast and memory efficient processing. ReForeSt main contributions are manifold: (i) it provides a novel approach for the RF implementation in a distributed environment targeting an in-memory efficient processing, (ii) it is faster and more memory efficient with respect to the de facto standard MLlib, (iii) the level of parallelism is self-configuring.
 </p>

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
val property = new RFProperty()
property.dataset = "data/sample-infimnist.libsvm"
property.featureNumber = 794

property.numTrees = 100
property.maxDepth = 5
property.numClasses = 10

// Create the Random Forest classifier.
val rfRunner = RFRunner.apply(property)

// Load and parse the data file and return the training data.
val trainingData = rfRunner.loadData(0.7)

// Train a Random Forest model.
val model = rfRunner.trainClassifier(trainingData)

// Evaluate model on test instances and compute test error
val labelAndPreds = rfRunner.getTestData().map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / rfRunner.getTestData().count()
rfRunner.printTree()
rfRunner.sparkStop()

println("Test Error = " + testErr)
```

## Quick Start
To quickly start using ReForeSt we provide a pre-built Maven project with all the settings and configuration to automatically import the project in IntelliJ.
The prebuilt-project can be found in "resources/package" in zip and tar.gz format.

## Authors
* <a href="http://for.unipi.it/alessandro_lulli/">Alessandro Lulli</a>
* <a href="http://www.lucaoneto.com">Luca Oneto</a>
* <a href="http://www.dibris.unige.it/anguita-davide">Davide Anguita</a>

The project has been developed at <a href="https://sites.google.com/site/smartlabdibrisunige/">Smartlab</a> UNIGE.
