package reforest.rf

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import reforest.TypeInfo
import reforest.data.RawDataLabeled
import reforest.data.tree.ForestManager
import reforest.rf.parameter.RFParameter
import reforest.util.CCUtilIO

import scala.collection.mutable.ListBuffer

/**
  *
  * @param sc
  * @param strategy
  * @param typeInfoBC
  * @param checkingData
  * @tparam T
  * @tparam U
  */
class RFModelSelection[T, U](@transient sc: SparkContext,
                             strategy: RFStrategy[T, U],
                             typeInfoBC: Broadcast[TypeInfo[T]],
                             checkingData: RDD[RawDataLabeled[T, U]]) extends Serializable {

  val modelMap: scala.collection.mutable.Map[(Int, Int, Int), RFModelAggregator[T, U]] = scala.collection.mutable.Map()
  val checkingDataSize = checkingData.persist(StorageLevel.MEMORY_ONLY).count()
  println("MS checking data size: " + checkingDataSize)

  var bestModel: Option[RFModel[T, U]] = Option.empty
  var bestModelAccuracy = Double.MinValue

  var modelToBeRemoved: ListBuffer[(Int, Int, Double)] = ListBuffer.empty

  val skipper = new RFSkip

  def getSkipper = skipper

  def update(forestManager: ForestManager[T, U],
             parameter: RFParameter,
             depth: Int,
             macroIteration: Int = 0) = {
    val toReturn: Array[RFModel[T, U]] = new Array[RFModel[T, U]](forestManager.getForest.length)

    var forestId = 0
    while (forestId < toReturn.length) {
      val key = (forestManager.getForest(forestId).binNumber, forestManager.getForest(forestId).featurePerNode.getFeaturePerNodeNumber, depth)

      if(!skipper.contains(forestManager.getForest(forestId).binNumber, forestManager.getForest(forestId).featurePerNode.getFeaturePerNodeNumber)) {
        val previousModel = modelMap.getOrElse(key, new RFModelAggregator[T, U](parameter.numClasses))
        previousModel.addModel(strategy.generateModel(sc, forestManager.getForest(forestId), typeInfoBC, macroIteration))

        modelMap.put(key, previousModel)
      }
      forestId += 1
    }
  }

  def testAllModels(parameter: RFParameter, testingData: RDD[RawDataLabeled[T, U]]) = {
    if (parameter.testAll && parameter.logStats) {
      val testingDataSize = testingData.persist(StorageLevel.MEMORY_ONLY).count()
      for (((binNumber, featurePerNode, depth), model) <- modelMap) {

        for (trees <- parameter.numTrees) {
          if (trees <= model.getNumTrees) {
            val m = new RFModelFixedParameter[T, U](model, if(depth >= parameter.getMaxDepth) Int.MaxValue else depth, trees)

            val labelAndPreds = testingData.map { point =>
              val prediction = m.predict(point.features, depth)
              (point.label, prediction)
            }

            val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testingDataSize
            val accuracy = 1 - testErr

            CCUtilIO.logACCURACY(parameter, m, depth, accuracy, 0, "stats-model-selection-testing.txt")
          }
        }
      }
    }
  }

  def checkBestModel(depth: Int, parameter: RFParameter) = {

    val model = modelMap.filter(t => t._1._3 == depth).values.toArray

    val modelAccuracy: ListBuffer[(Int, Int, Double)] = ListBuffer.empty

    var forestId = 0
    var bestModelAccuracyRound = 0d
    while (forestId < model.length) {

      if (!skipper.contains(model(forestId).getBinNumber, model(forestId).getFeaturePerNode)) {

        val labelAndPreds = checkingData.map { point =>
          val prediction = model(forestId).predict(point.features, depth)
          (point.label, prediction)
        }

        val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / checkingDataSize
        val accuracy = 1 - testErr

        CCUtilIO.logACCURACY(parameter, model(forestId), depth, (1 - testErr), 0, "stats-model-selection-checking.txt")

        modelAccuracy += ((model(forestId).getBinNumber, model(forestId).getFeaturePerNode, accuracy))

        if(accuracy > bestModelAccuracyRound) {
          bestModelAccuracyRound = accuracy
        }

        if (accuracy > bestModelAccuracy) {
          bestModel = Some(new RFModelFixedParameter(model(forestId), if(depth >= parameter.getMaxDepth) Int.MaxValue else depth, model(forestId).getNumTrees))
          bestModelAccuracy = accuracy
          println("MS CHECK ACCURACY " + forestId + " (feature " + model(forestId).getFeaturePerNode + ")(bin " + model(forestId).getBinNumber + ")(depth " + depth + ")(trees " + model(forestId).getNumTrees + ") = " + accuracy)
        } else {
          println("MS CHECK accuracy " + forestId + " (feature " + model(forestId).getFeaturePerNode + ")(bin " + model(forestId).getBinNumber + ")(depth " + depth + ")(trees " + model(forestId).getNumTrees + ") = " + accuracy)
        }
      }
      forestId += 1
    }

    modelToBeRemoved = ListBuffer.empty
    for (model <- modelAccuracy) {
      if (Math.abs(model._3 - bestModelAccuracyRound) > parameter.modelSelectionEpsilonRemove) {
        skipper.addConfigurationToSkip(model._1, model._2)
        modelToBeRemoved += model
        println("MS REMOVE " + model)
      }
    }

    bestModel.get
  }

  def removeModel(forestManager: ForestManager[T, U]): ForestManager[T, U] = {
    forestManager
  }
}
