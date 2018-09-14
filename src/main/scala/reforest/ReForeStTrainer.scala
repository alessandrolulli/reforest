package reforest

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import reforest.data.tree.Forest
import reforest.rf._
import reforest.rf.feature.RFFeatureManager
import reforest.rf.parameter.{RFParameter, RFParameterBuilder}
import reforest.util._

import scala.reflect.ClassTag

class ReForestTrainerSingleModel[T: ClassTag, U: ClassTag] {

}

class ReForeStTrainer[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                val parameter: Broadcast[RFParameter],
                                                val instrumented: GCInstrumented,
                                                val strategy: RFStrategy[T, U],
                                                val typeInfo: TypeInfo[T],
                                                val typeInfoWorking: TypeInfo[U],
                                                val categoricalFeaturesInfo: RFCategoryInfo) extends Serializable {

  protected val instrumentedBC = sc.broadcast(instrumented)
  protected val categoricalFeaturesInfoBC = sc.broadcast(categoricalFeaturesInfo)
  protected val typeInfoBC = sc.broadcast(typeInfo)
  protected val typeInfoWorkingBC = sc.broadcast(typeInfoWorking)
  protected val strategyBC = sc.broadcast(strategy)

  protected val loader = new ReForeStLoader[T, U](sc,
    parameter,
    strategyBC,
    typeInfoBC,
    typeInfoWorkingBC,
    categoricalFeaturesInfoBC,
    strategy.generateRawDataset(sc, typeInfoBC, instrumentedBC, categoricalFeaturesInfoBC))

  def testDataFreeze(): Unit = loader.testdatafreeze()

  def getDataLoader = loader

  def sparkStop() = sc.stop()

  def printTree(oracle: Forest[T, U]) = {
    if (parameter.value.outputTree) {
      println(oracle.toString())
    }
  }

  def trainClassifier(): RFModel[T, U] = {
    val timeStart = System.currentTimeMillis()



    if (strategy.getMacroIterationNumber > 1) {
      loader.trainingdatafreeze()
    }

    //(binNumber,featurePerNode)
    val modelMap: scala.collection.mutable.Map[(Int, Int), RFModelAggregator[T, U]] = scala.collection.mutable.Map()

    val numTreesPerIteration = parameter.value.getMaxNumTrees / strategy.getMacroIterationNumber
    for (i <- 0 to strategy.getMacroIterationNumber - 1) {
      val forestManager = run(numTreesPerIteration, i, if (i == 0 || strategy.getMacroIterationNumber > 1) false else true)
      if (forestManager.getForest.length > 1) {
        println("ERROR! There are more than one forest!")
      }

      val toReturn: Array[RFModel[T, U]] = new Array[RFModel[T, U]](forestManager.getForest.length)
      var forestId = 0
      while (forestId < toReturn.length) {
        val key = (forestManager.getForest(forestId).binNumber, forestManager.getForest(forestId).featurePerNode.getFeaturePerNodeNumber)
        val previousModel = modelMap.getOrElse(key, new RFModelAggregator[T, U](parameter.value.numClasses))
        previousModel.addModel(strategy.generateModel(sc, forestManager.getForest(forestId), typeInfoBC, i))

        modelMap.put(key, previousModel)

        forestId += 1
      }
    }

    val timeEnd = System.currentTimeMillis()
    CCUtilIO.logTIME(parameter.value, (timeEnd - timeStart), 0)

    modelMap.values.last
  }

  def switchToSLC(nodeNumber: Int, depth: Int, memoryUtil: MemoryUtil) = {
    if (parameter.value.slcActive) {
      if (parameter.value.slcDepth != -1) {
        println("SLC, SWITCHING EXACTLY AT DEPTH " + parameter.value.slcDepth)
        parameter.value.slcDepth == depth
      } else {
        if (nodeNumber < parameter.value.sparkCoresMax) {
          println("SLC, NOT SWITCHING BECAUSE LOW ACTIVE NODES NUMBER")
          false
        } else {
          println("SLC, NODE NUMBER: " + nodeNumber + " DEPTH: " + depth)
          memoryUtil.switchToSLC(depth, nodeNumber)
        }
      }
    } else {
      false
    }
  }

  def run(numTrees: Int = parameter.value.getMaxNumTrees, macroIteration: Int = 0, skipPreparation: Boolean = false) = {
    val workingData = loader.getWorkingData(numTrees, macroIteration, skipPreparation)
    val memoryUtil = loader.getMemoryUtil.get
    var forestManager = loader.getForestManager.get

    var featureManager: RFFeatureManager = new RFFeatureManager(forestManager.getParameter)
    featureManager.addFeatures(forestManager)

    val maxNodesConcurrent = memoryUtil.getMaximumConcurrenNodes(parameter.value, RFSkip.empty)
    CCUtilIO.logTIME(parameter.value, parameter.value.appName, "START-TREE")

    val runner = new ReForeStRunnerStandard[T, U](sc, parameter, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesInfoBC, strategyBC, instrumentedBC, numTrees, macroIteration)

    var depth = 0
    while (depth < parameter.value.getMaxDepth && !switchToSLC(featureManager.getActiveNodesNum, depth, memoryUtil) && featureManager.getActiveNodesNum > 0) {
      runner.run(depth, depth + 1, maxNodesConcurrent, forestManager, featureManager, workingData)
      depth += 1
    }

    if (depth < parameter.value.getMaxDepth && parameter.value.slcActive) {
      forestManager = runner.toSLC.run(depth, Int.MaxValue, -1, forestManager, featureManager, workingData)
    }

    workingData.unpersist()

    forestManager
  }
}

class ReForeStTrainerModelSelectionRotation[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                                      parameter: Broadcast[RFParameter],
                                                                      instrumented: GCInstrumented,
                                                                      strategy: RFStrategy[T, U],
                                                                      typeInfo: TypeInfo[T],
                                                                      typeInfoWorking: TypeInfo[U],
                                                                      categoricalFeaturesInfo: RFCategoryInfo) extends ReForeStTrainer[T, U](sc, parameter, instrumented, strategy, typeInfo, typeInfoWorking, categoricalFeaturesInfo) {
  // this must be done differently....
  // help me, this works but i do not want to watch it... there is the logic for the model selection
  override def trainClassifier(): RFModel[T, U] = {
    val timeStart = System.currentTimeMillis()
    var timeChecking = 0l

    if (strategy.getMacroIterationNumber > 1) {
      loader.trainingdatafreeze()
    }

    var parameterToUse = parameter

    var treesDone = 0

    val timeStartChecking = System.currentTimeMillis()
    val modelSelection = new RFModelSelection[T, U](sc, strategyBC.value, typeInfoBC, loader.getRawDataset.checkingData)
    val timeEndChecking = System.currentTimeMillis()
    timeChecking += (timeEndChecking - timeStartChecking)

    var trees = 0
    var treeIndex = 0
    var end = false
    while (!end && treeIndex < parameterToUse.value.numTrees.length) {
      trees = parameterToUse.value.numTrees(treeIndex)
      treeIndex += 1

      val treesToDo = trees - treesDone
      println("DOING " + treesToDo + " TREES (ON MAXIMUM OF " + parameterToUse.value.getMaxNumTrees + ") IN " + parameterToUse.value.numTrees.length + " ITERATIONS")

      val numTreesPerIteration = treesToDo / strategy.getMacroIterationNumber

      var i = 0
      while (i < strategy.getMacroIterationNumber) {
        var runner: ReForeStRunner[T, U] = new ReForeStRunnerStandard[T, U](sc, parameterToUse, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesInfoBC, strategyBC, instrumentedBC, numTreesPerIteration, i)

        val workingData = loader.getWorkingData(numTreesPerIteration, i, if (treeIndex == 1) false else true)
        val memoryUtil = loader.getMemoryUtil.get
        var forestManager = loader.getForestManager.get

        val maxNodesConcurrent = memoryUtil.getMaximumConcurrenNodes(parameter.value, modelSelection.skipper)

        var featureManager: RFFeatureManager = new RFFeatureManager(forestManager.getParameter)
        featureManager.addFeatures(forestManager)

        var depthToCompute = 0
        var depthIndex = 0
        var depth = 0
        var depthSkip = false
        while (!depthSkip && depthIndex < parameterToUse.value.depth.length) {

          depth = parameterToUse.value.depth(depthIndex)

          if (switchToSLC(featureManager.getActiveNodesNum, depth, memoryUtil)) runner = runner.toSLC

          println("MS (binNumber " + parameterToUse.value.binNumber.mkString(",") + ")(feature " + parameterToUse.value.featureMultiplierPerNode.mkString(",") + ")(depth " + depth + ")(trees " + trees + ")")
          forestManager = runner.run(depthToCompute, depth, maxNodesConcurrent, forestManager, featureManager, workingData, modelSelection.getSkipper)
          depthToCompute = depth

          modelSelection.update(forestManager, parameterToUse.value, depth, i)

          if (i == strategy.getMacroIterationNumber - 1) {
            val previousAccuracy = modelSelection.bestModelAccuracy
            val best = modelSelection.checkBestModel(depth, parameterToUse.value)

            if (Math.abs(previousAccuracy - modelSelection.bestModelAccuracy) < parameterToUse.value.modelSelectionEpsilon) {
              if (parameterToUse.value.depth.length == 1) {
                end = true
              }
              depthSkip = true
              println("MS BEST MODEL PROVIDE ACCURACY(" + best.getDepth + "): " + modelSelection.bestModelAccuracy)
              parameterToUse.unpersist()
              parameterToUse = sc.broadcast(parameterToUse.value.applyDepth(best.getDepth))
            }
          }
          depthIndex += 1
        }

        if (!depthSkip) {
          parameterToUse.unpersist()
          parameterToUse = sc.broadcast(parameterToUse.value.applyDepth(depth))
        }

        i += 1
      }
      treesDone += treesToDo
    }

    val timeEnd = System.currentTimeMillis()
    CCUtilIO.logTIME(parameterToUse.value, (timeEnd - timeStart), timeChecking)

    modelSelection.testAllModels(parameterToUse.value, getDataLoader.getTestingData)

    modelSelection.bestModel.get
  }
}

class ReForeStTrainerModelSelection[T: ClassTag, U: ClassTag](@transient private val sc: SparkContext,
                                                              parameter: Broadcast[RFParameter],
                                                              instrumented: GCInstrumented,
                                                              strategy: RFStrategy[T, U],
                                                              typeInfo: TypeInfo[T],
                                                              typeInfoWorking: TypeInfo[U],
                                                              categoricalFeaturesInfo: RFCategoryInfo) extends ReForeStTrainer[T, U](sc, parameter, instrumented, strategy, typeInfo, typeInfoWorking, categoricalFeaturesInfo) {
  // this must be done differently....
  // help me, this works but i do not want to watch it... there is the logic for the model selection
  override def trainClassifier(): RFModel[T, U] = {
    val timeStart = System.currentTimeMillis()
    var timeChecking = 0l
    var parameterToUse = parameter

    val timeStartChecking = System.currentTimeMillis()
    val modelSelection = new RFModelSelection[T, U](sc, strategyBC.value, typeInfoBC, loader.getRawDataset.checkingData)
    val timeEndChecking = System.currentTimeMillis()
    timeChecking += (timeEndChecking - timeStartChecking)

    var treesDone = 0
    var treeIndex = 0
    var end = false
    while (!end && treeIndex < parameterToUse.value.numTrees.length) {
      val treesToDo = parameterToUse.value.numTrees(treeIndex) - treesDone
      println("DOING " + treesToDo + " TREES (ON MAXIMUM OF " + parameterToUse.value.getMaxNumTrees + ") IN " + parameterToUse.value.numTrees.length + " ITERATIONS")

      val timeStartChecking = System.currentTimeMillis()
      var runner: ReForeStRunner[T, U] = new ReForeStRunnerStandard[T, U](sc, parameterToUse, typeInfoBC, typeInfoWorkingBC, categoricalFeaturesInfoBC, strategyBC, instrumentedBC, treesToDo)
      val workingData = loader.getWorkingData(treesToDo, 0, if (treeIndex == 0) false else true)
      val memoryUtil = loader.getMemoryUtil.get
      var forestManager = loader.getForestManager.get

      val featureManager: RFFeatureManager = new RFFeatureManager(forestManager.getParameter)
      featureManager.addFeatures(forestManager)
      val timeEndChecking = System.currentTimeMillis()
      timeChecking += (timeEndChecking - timeStartChecking)

      var depthToCompute = 0
      var depthIndex = 0
      var depth = 0
      var depthSkip = false
      while (!depthSkip && depthIndex < parameterToUse.value.depth.length) {
        depth = parameterToUse.value.depth(depthIndex)

        if (switchToSLC(featureManager.getActiveNodesNum, depth, memoryUtil)) runner = runner.toSLC

        println("MS (binNumber " + parameterToUse.value.binNumber.mkString(",") + ")(feature " + parameterToUse.value.featureMultiplierPerNode.mkString(",") + ")(depth " + depth + ")(trees " + parameterToUse.value.numTrees(treeIndex) + ")")
        forestManager = runner.run(depthToCompute, depth, memoryUtil.getMaximumConcurrenNodes(parameterToUse.value, modelSelection.skipper), forestManager, featureManager, workingData, modelSelection.getSkipper)

        val timeStartChecking = System.currentTimeMillis()
        modelSelection.update(forestManager, parameterToUse.value, depth)

        val previousAccuracy = modelSelection.bestModelAccuracy
        val best = modelSelection.checkBestModel(depth, parameterToUse.value)

        if (Math.abs(previousAccuracy - modelSelection.bestModelAccuracy) < parameterToUse.value.modelSelectionEpsilon) {
          if (parameterToUse.value.depth.length == 1) {
            end = true
          }
          depthSkip = true
          parameterToUse.unpersist()
          parameterToUse = sc.broadcast(parameterToUse.value.applyDepth(best.getDepth))
        }

        depthIndex += 1
        depthToCompute = depth

        val timeEndChecking = System.currentTimeMillis()
        timeChecking += (timeEndChecking - timeStartChecking)
      }

      if (!depthSkip) {
        parameterToUse.unpersist()
        parameterToUse = sc.broadcast(parameterToUse.value.applyDepth(depth))
      }

      treesDone += treesToDo
      treeIndex += 1
    }

    val timeEnd = System.currentTimeMillis()

    CCUtilIO.logTIME(parameterToUse.value, (timeEnd - timeStart), timeChecking)

    modelSelection.testAllModels(parameterToUse.value, getDataLoader.getTestingData)

    modelSelection.bestModel.get
  }
}

class ReForeStTrainerBuilder[T: ClassTag, U: ClassTag](typeInfo: TypeInfo[T],
                                                       typeInfoWorking: TypeInfo[U],
                                                       var parameter: RFParameter = RFParameterBuilder.apply.build) {
  private var parameterBC: Option[Broadcast[RFParameter]] = Option.empty
  private var instrumented: GCInstrumented = new GCInstrumentedEmpty
  private var categoryInfo: RFCategoryInfo = new RFCategoryInfoEmpty

  private var rotationSupport: Boolean = parameter.rotation
  private var modelSelection: Boolean = parameter.modelSelection

  def addProperty(property: RFParameter): ReForeStTrainerBuilder[T, U] = {
    parameter = property
    modelSelection = property.modelSelection
    rotationSupport = property.rotation

    this
  }

  def getProperty = {
    if (!modelSelection) {
      parameter = parameter.applyDepth(parameter.getMaxDepth)
        .applyNumTrees(parameter.getMaxNumTrees)
        .applyBinNumber(parameter.getMaxBinNumber)
        .applyFeatureMultiplier(parameter.getMaxFeatureMultiplierPerNode)
    }

    if (rotationSupport) {
      parameter = parameter.applySLCActive(false)
    }

    parameter
  }

  private def getPropertyBC(sc: SparkContext) = {
    if (parameterBC.isDefined) {
      parameterBC.get
    } else {
      parameterBC = Some(sc.broadcast(getProperty))
      parameterBC.get
    }
  }

  def addGCInstrumented(instrumented_ : GCInstrumented): ReForeStTrainerBuilder[T, U] = {
    instrumented = instrumented_
    this
  }

  def addStrategyRotation(addRotationSupport: Boolean = true): ReForeStTrainerBuilder[T, U] = {
    rotationSupport = addRotationSupport
    parameter = parameter.applyRotation(addRotationSupport)
    this
  }

  def performModelSelection(addModelSelection: Boolean = true): ReForeStTrainerBuilder[T, U] = {
    modelSelection = addModelSelection
    parameter = parameter.applyModelSelection(addModelSelection)
    this
  }

  def addCategoryInfo(categoryInfo_ : RFCategoryInfo): ReForeStTrainerBuilder[T, U] = {
    categoryInfo = categoryInfo_
    this
  }

  private def getStrategy(sc: SparkContext) = {
    if (rotationSupport) {
      if (modelSelection) {
        val property = getPropertyBC(sc)
        new RFStrategyModelSelection[T, U](new RFStrategyRotation[T, U](sc, property, sc.broadcast(typeInfo)), property)
      } else {
        new RFStrategyRotation[T, U](sc, getPropertyBC(sc), sc.broadcast(typeInfo))
      }
    } else {
      if (modelSelection) {
        val property = getPropertyBC(sc)
        new RFStrategyModelSelection[T, U](new RFStrategyStandard[T, U](property), property)
      } else {
        new RFStrategyStandard[T, U](getPropertyBC(sc))
      }
    }
  }

  def build(sc: SparkContext): ReForeStTrainer[T, U] = {
    if (modelSelection) {
      if (rotationSupport) {
        new ReForeStTrainerModelSelectionRotation[T, U](sc, getPropertyBC(sc), instrumented, getStrategy(sc), typeInfo, typeInfoWorking, categoryInfo)
      } else {
        new ReForeStTrainerModelSelection[T, U](sc, getPropertyBC(sc), instrumented, getStrategy(sc), typeInfo, typeInfoWorking, categoryInfo)
      }
    } else {
      new ReForeStTrainer[T, U](sc, getPropertyBC(sc), instrumented, getStrategy(sc), typeInfo, typeInfoWorking, categoryInfo)
    }
  }
}

object ReForeStTrainerBuilder {
  def apply(): ReForeStTrainerBuilder[Double, Byte] = new ReForeStTrainerBuilder(new TypeInfoDouble(), new TypeInfoByte())

  def apply(parameter: RFParameter): ReForeStTrainerBuilder[Double, Byte] = new ReForeStTrainerBuilder(new TypeInfoDouble(), new TypeInfoByte(), parameter)

  def apply[U: ClassTag](typeInfoWorking: TypeInfo[U]): ReForeStTrainerBuilder[Double, U] = new ReForeStTrainerBuilder(new TypeInfoDouble(), typeInfoWorking)
  
  // ISSUE #1
  def apply[T: ClassTag, U: ClassTag](typeInfo: TypeInfo[T], typeInfoWorking: TypeInfo[U], parameter: RFParameter): ReForeStTrainerBuilder[T, U] = new ReForeStTrainerBuilder(typeInfo, typeInfoWorking, parameter)
}
