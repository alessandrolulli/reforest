package reforest

import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.data.{RawDataLabeled, RawDataset, StaticData}
import reforest.data.tree.ForestManager
import reforest.rf.parameter.RFParameter
import reforest.rf.split.RFSplitterManager
import reforest.rf.{RFCategoryInfo, RFDataPrepare, RFStrategy}
import reforest.util.{GCInstrumented, GCInstrumentedEmpty, MemoryUtil}

class ReForeStLoader[T, U](@transient private val sc: SparkContext,
                           parameter: Broadcast[RFParameter],
                           strategyBC: Broadcast[RFStrategy[T, U]],
                           val typeInfoBC: Broadcast[TypeInfo[T]],
                           val typeInfoWorkingBC: Broadcast[TypeInfo[U]],
                           val categoricalFeaturesInfoBC: Broadcast[RFCategoryInfo],
                           rawDataset: RawDataset[T, U]) extends Serializable {

  val instrumented: Broadcast[GCInstrumented] = sc.broadcast(new GCInstrumentedEmpty)
  val dataPrepare = new RFDataPrepare[T, U](typeInfoBC, instrumented, strategyBC, false, 1)

  private var memoryUtil : Option[MemoryUtil] = Option.empty
  private var forestManager : Option[ForestManager[T, U]] = Option.empty
  private var workingData : Option[RDD[StaticData[U]]] = Option.empty
  private var previousWorkingData : Option[RDD[StaticData[U]]] = Option.empty
  private var splitterManager : Option[RFSplitterManager[T,U]] = Option.empty

  def testdatafreeze(): Unit = {
    rawDataset.testingData.persist(parameter.value.storageLevel)
  }

  def trainingdatafreeze(): Unit = {
    //    rawDataset.trainingData.persist(property.storageLevel)
    rawDataset.trainingData.count()
  }

  def getRawDataset = rawDataset

  def getTestingData: RDD[RawDataLabeled[T, U]] = rawDataset.testingData

  def getMemoryUtil = memoryUtil
  def getForestManager = forestManager

  /* this is ugly, please do not return 3 things */
  def getWorkingData(numTrees: Int = parameter.value.getMaxNumTrees, macroIteration: Int = 0, skipPreparation : Boolean =false) = {
    val timePreparationSTART = System.currentTimeMillis()
    if(skipPreparation) {
      forestManager = Some(new ForestManager[T, U](parameter.value.applyNumTrees(numTrees), splitterManager.get))
      previousWorkingData = workingData

      workingData = Some(dataPrepare.prepareData(rawDataset.trainingData,
        sc.broadcast(forestManager.get.splitterManager.getSplitter(macroIteration)),
        parameter.value.numFeatures,
        memoryUtil.get,
        numTrees,
        macroIteration))

//      workingData = Some(workingData.get.mapPartitionsWithIndex{case (partitionIndex, elements) =>
//        strategyBC.value.reGenerateBagging(numTrees, partitionIndex, elements)})
      val dataSize = workingData.get.persist(parameter.value.storageLevel).count()

      if(previousWorkingData.isDefined) {
        previousWorkingData.get.unpersist()
      }

      val timePreparationEND = System.currentTimeMillis()
      println("TIME PREPARATION SKIPPED INIT ("+dataSize+"): " + (timePreparationEND - timePreparationSTART))
      workingData.get
    } else {

      previousWorkingData = workingData

      val zzz = strategyBC.value.findSplits(rawDataset.trainingData, typeInfoBC, typeInfoWorkingBC, instrumented, categoricalFeaturesInfoBC)
      splitterManager = Some(zzz._1)
      forestManager = Some(new ForestManager[T, U](parameter.value.applyNumTrees(numTrees), zzz._1))
      memoryUtil = Some(zzz._2)

      val splitter = forestManager.get.splitterManager.getSplitter(macroIteration)

      // TODO the broadcast of the splitter must be unpersisted!!!
      workingData = Some(dataPrepare.prepareData(rawDataset.trainingData,
        sc.broadcast(splitter),
        parameter.value.numFeatures,
        memoryUtil.get,
        numTrees,
        macroIteration))

      val dataSize = workingData.get.persist(parameter.value.storageLevel).count()
      if(previousWorkingData.isDefined) {
        previousWorkingData.get.unpersist()
      }
      val timePreparationEND = System.currentTimeMillis()
      println("TIME PREPARATION: " + (timePreparationEND - timePreparationSTART))
      workingData.get
    }
  }

}