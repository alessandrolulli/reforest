package reforest.rf

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data.StaticData
import reforest.data.tree.ForestManager
import reforest.rf.feature.RFFeatureManager
import reforest.rf.parameter.RFParameter
import reforest.rf.slc.SLCTreeGeneration
import reforest.util.{GCInstrumented, MemoryUtil}

import scala.reflect.ClassTag

/**
  * It executes the ReForeSt tree learning
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
trait ReForeStRunner[T, U] extends Serializable {
  /**
    * It executes the learning phase until the maximum depth passed as parameter
    *
    * @param depth              the current depth
    * @param maxDepthToCompute  the max depth to be computed
    * @param maxNodesConcurrent the maximum number of nodes to concurrently execute
    * @param forestManager      the forest manager
    * @param featureManager     the feature manager
    * @param workingData        the working data
    * @return the updated forest manager
    */
  def run(depth: Int,
          maxDepthToCompute: Int,
          maxNodesConcurrent: Int,
          forestManager: ForestManager[T, U],
          featureManager: RFFeatureManager,
          workingData: RDD[StaticData[U]],
          skip: RFSkip = new RFSkip): ForestManager[T, U]

  /**
    * Update the configuration parameter
    *
    * @param parameter the new configuration parameter
    * @return a new instance of the runner with the updated parameter
    */
  def updateParameter(parameter: RFParameter): ReForeStRunner[T, U]

  /**
    * Update the number of trees to be computed
    *
    * @param numTreesArgs the new number of trees to compute
    * @return a new instance of the runner with the updated parameter
    */
  def updateNumTrees(numTreesArgs: Int): ReForeStRunner[T, U]

  /**
    * It convert the runner to a standard runner (breadth first completly distributed)
    *
    * @return a new instance of the runner working completly distributed
    */
  def toStandard: ReForeStRunner[T, U]

  /**
    * It convert the runner to a SLC runner (depth first each node local computation)
    *
    * @return a new instance of the runner working in SLC (sub-tree local computation)
    */
  def toSLC: ReForeStRunner[T, U]

}

class ReForeStRunnerStandard[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                       propertyBC: Broadcast[RFParameter],
                                                       typeInfoBC: Broadcast[TypeInfo[T]],
                                                       typeInfoWorkingBC: Broadcast[TypeInfo[U]],
                                                       categoricalFeaturesBC: Broadcast[RFCategoryInfo],
                                                       strategyBC: Broadcast[RFStrategy[T, U]],
                                                       instrumentedBC: Broadcast[GCInstrumented],
                                                       numTrees: Int,
                                                       macroIteration: Int = 0) extends ReForeStRunner[T, U] {

  private val tree = new RFTreeGeneration[T, U](sc, propertyBC, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, numTrees, macroIteration)

  def run(depth: Int,
          maxDepthToCompute: Int,
          maxNodesConcurrent: Int,
          forestManager: ForestManager[T, U],
          featureManager: RFFeatureManager,
          workingData: RDD[StaticData[U]],
          skip: RFSkip): ForestManager[T, U] = {

    var depthToCompute = depth
    while (depthToCompute < maxDepthToCompute) {
      depthToCompute += 1

      println("DEPTH: " + depthToCompute)

      val bestCutArray = tree.findBestCut(sc, workingData, forestManager, featureManager, depthToCompute, instrumentedBC, maxNodesConcurrent, skip)

      if (!bestCutArray.isEmpty) {
        featureManager.clearFeatures

        bestCutArray.foreach(cutInfo => {
          val forestId = cutInfo._1.forestId
          val treeId = cutInfo._1.treeId
          val nodeId = cutInfo._1.nodeId
          val cut = cutInfo._2

          forestManager.getForest(forestId).updateForest(cut, forestId, treeId, nodeId, typeInfoBC.value, typeInfoWorkingBC.value, Some(featureManager))
        })
      }
    }

    forestManager
  }

  def updateParameter(parameter: RFParameter): ReForeStRunner[T, U] = new ReForeStRunnerStandard[T, U](sc, sc.broadcast(parameter), typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, instrumentedBC, numTrees, macroIteration)

  def updateNumTrees(numTreesArgs: Int): ReForeStRunner[T, U] = new ReForeStRunnerStandard[T, U](sc, propertyBC, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, instrumentedBC, numTreesArgs, macroIteration)

  def toStandard(): ReForeStRunner[T, U] = this

  def toSLC(): ReForeStRunner[T, U] = new ReForeStRunnerSLC[T, U](sc, propertyBC, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, instrumentedBC, numTrees, macroIteration)
}

class ReForeStRunnerSLC[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                  propertyBC: Broadcast[RFParameter],
                                                  typeInfoBC: Broadcast[TypeInfo[T]],
                                                  typeInfoWorkingBC: Broadcast[TypeInfo[U]],
                                                  categoricalFeaturesBC: Broadcast[RFCategoryInfo],
                                                  strategyBC: Broadcast[RFStrategy[T, U]],
                                                  instrumentedBC: Broadcast[GCInstrumented],
                                                  numTrees: Int,
                                                  macroIteration: Int) extends ReForeStRunner[T, U] {

  var treeSLC: SLCTreeGeneration[T, U] = new SLCTreeGeneration[T, U](sc, propertyBC, typeInfoBC, typeInfoWorkingBC, strategyBC.value.getSampleSize)

  def run(depth: Int,
          maxDepthToCompute: Int,
          maxNodesConcurrent: Int,
          forestManager: ForestManager[T, U],
          featureManager: RFFeatureManager,
          workingData: RDD[StaticData[U]],
          skip: RFSkip): ForestManager[T, U] = {
    treeSLC.findBestCutSLC(workingData, forestManager, featureManager, maxDepthToCompute, instrumentedBC, skip)
  }

  def updateParameter(parameter: RFParameter): ReForeStRunner[T, U] = new ReForeStRunnerSLC[T, U](sc, sc.broadcast(parameter), typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, instrumentedBC, numTrees, macroIteration)

  def updateNumTrees(numTreesArgs: Int): ReForeStRunner[T, U] = new ReForeStRunnerSLC[T, U](sc, propertyBC, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, instrumentedBC, numTreesArgs, macroIteration)

  def toStandard(): ReForeStRunner[T, U] = new ReForeStRunnerStandard[T, U](sc, propertyBC, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesBC, strategyBC, instrumentedBC, numTrees, macroIteration)

  def toSLC(): ReForeStRunner[T, U] = this
}
