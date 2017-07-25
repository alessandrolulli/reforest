/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reforest.rf

import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reforest.TypeInfo
import reforest.data._
import reforest.data.tree.{Forest, ForestManager}
import reforest.rf.parameter.RFParameter
import reforest.rf.rotation.RFRotationMatrix
import reforest.rf.split._
import reforest.util.{GCInstrumented, MemoryUtil}

import scala.reflect.ClassTag

/**
  * Base class for the different random forest strategy implemented in ReForeSt
  *
  * @tparam T raw data type
  * @tparam U working data type
  */
abstract class RFStrategy[T, U]() extends Serializable {
  var sampleSize: Option[Long] = Option.empty

  /**
    * It returns the number of samples in the dataset
    *
    * @return the number of samples in the dataset
    */
  def getSampleSize: Long = {
    assert(sampleSize.isDefined)
    sampleSize.get
  }

  def getMacroIterationNumber: Int = 1

  /**
    * It generates the bagging for an element
    *
    * @param size         The number of trees
    * @param distribution The distribution to use for generating the bagging
    * @return An array with the contribution to each tree
    */
  def generateBagging(size: Int, distribution: PoissonDistribution): Array[Byte]

  /**
    * It detects the splits for each feature in the dataset for the discretization
    *
    * @param input                  The raw dataset
    * @param typeInfo               The type information for the raw data
    * @param typeInfoWorking        The type information for the working data
    * @param instrumented           The instrumentation for the GC
    * @param categoricalFeatureInfo The information about categorical features
    * @return A splitter manager which contain how to split each feature and a memory util
    */
  def findSplits(input: RDD[RawDataLabeled[T, U]],
                 typeInfo: Broadcast[TypeInfo[T]],
                 typeInfoWorking: Broadcast[TypeInfo[U]],
                 instrumented: Broadcast[GCInstrumented],
                 categoricalFeatureInfo: Broadcast[RFCategoryInfo]): (RFSplitterManager[T, U], MemoryUtil)

  /**
    * It convert the raw data to the static data to use for computing random forest
    *
    * @param numTrees       the number of trees that will be computed
    * @param macroIteration the number of the macro iteration (if not all the trees are computed at the same time)
    * @param splitter       the splitter which contain how to split each feature
    * @param partitionIndex the Spark partition index
    * @param instances      the raw dataset
    * @param instrumented   the instrumentation for the GC
    * @param memoryUtil     the memory util
    * @return
    */
  def prepareData(numTrees: Int,
                  macroIteration: Int,
                  splitter: Broadcast[RFSplitter[T, U]],
                  partitionIndex: Int,
                  instances: Iterator[RawDataLabeled[T, U]],
                  instrumented: GCInstrumented,
                  memoryUtil: MemoryUtil): Iterator[StaticData[U]]

  /**
    * Base implemntation used by the strategies to detect the splits in the raw dataset
    *
    * @param property     the configuration properties
    * @param input        the input dataset
    * @param instrumented the instrumentation for the garbage collector
    * @return
    */
  protected def findSplitSampleInput(property: RFParameter,
                                     input: RDD[RawDataLabeled[T, U]],
                                     instrumented: Broadcast[GCInstrumented]) = {
    sampleSize = Some(input.count)
    println("SAMPLE SIZE: " + sampleSize.get)

    val memoryUtil = new MemoryUtil(getSampleSize, property)

    instrumented.value.gcALL
    val requiredSamples = math.min(math.max(property.getMaxBinNumber * property.getMaxBinNumber, 10000), getSampleSize)
    val fraction = requiredSamples.toDouble / getSampleSize
    val sampledInput = input.sample(withReplacement = false, fraction)
    instrumented.value.gcALL

    (memoryUtil, sampledInput)
  }

  /**
    * It generates a specialized implementation to split the raw dataset in testing / learning part
    *
    * @param sc                        the Spark context
    * @param typeInfoBC                the type information for the raw data
    * @param instrumentedBC            the instrumentation for the garbage collector
    * @param categoricalFeaturesInfoBC the information about the categorical features
    * @return a specialized implementation for the RawDataset
    */
  def generateRawDataset(sc: SparkContext,
                         typeInfoBC: Broadcast[TypeInfo[T]],
                         instrumentedBC: Broadcast[GCInstrumented],
                         categoricalFeaturesInfoBC: Broadcast[RFCategoryInfo]): RawDataset[T, U]

  /**
    * It generates a model to predict the labels with the forest passed as argument
    *
    * @param sc             the Spark context
    * @param forest         the forest to use to predict the labels
    * @param typeInfoBC     the type information for the raw data
    * @param macroIteration the macro iteration number
    * @return a specialized RFModel
    */
  def generateModel(sc: SparkContext,
                    forest: Forest[T, U],
                    typeInfoBC: Broadcast[TypeInfo[T]],
                    macroIteration: Int = 0): RFModel[T, U]

  /**
    * Generate again the bagging, This is used during model selection when incrementally adding the trees
    *
    * @param numTrees       the number of trees that require the bagging
    * @param partitionIndex the partition identifier of the RDD currently processed
    * @param instances      the instances in the partition of the RDD
    * @return the set of instances with the updated bagging
    */
  def reGenerateBagging(numTrees: Int,
                        partitionIndex: Int,
                        instances: Iterator[StaticData[U]]): Iterator[StaticData[U]] = instances
}

/**
  * The standard strategy to compute Random Forest according to Breiman et al. "Random forests"
  *
  * @param property the ReForeSt's property
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFStrategyStandard[T: ClassTag, U: ClassTag](property: Broadcast[RFParameter]) extends RFStrategy[T, U]() {
  override def generateBagging(size: Int, distribution: PoissonDistribution) = {
    val toReturn = new Array[Byte](size)
    var i = 0
    while (i < toReturn.length) {
      toReturn(i) = distribution.sample().toByte
      i += 1
    }
    toReturn
  }

  override def findSplits(input: RDD[RawDataLabeled[T, U]],
                          typeInfo: Broadcast[TypeInfo[T]],
                          typeInfoWorking: Broadcast[TypeInfo[U]],
                          instrumented: Broadcast[GCInstrumented],
                          categoricalFeatureInfo: Broadcast[RFCategoryInfo]): (RFSplitterManager[T, U], MemoryUtil) = {
    val (memoryUtil, sampledInput) = findSplitSampleInput(property.value, input, instrumented)

    (new RFSplitterManagerSingle[T, U](property.value.strategySplit.findSplitsSimple(sampledInput, property.value.getMaxBinNumber, property.value.numFeatures, memoryUtil.maximumConcurrentNumberOfFeature, typeInfo, typeInfoWorking, instrumented, categoricalFeatureInfo)), memoryUtil)
  }

  override def prepareData(numTrees: Int,
                           macroIteration: Int,
                           splitter: Broadcast[RFSplitter[T, U]],
                           partitionIndex: Int,
                           instances: Iterator[RawDataLabeled[T, U]],
                           instrumented: GCInstrumented,
                           memoryUtil: MemoryUtil): Iterator[StaticData[U]] = {
    val poisson = new PoissonDistribution(property.value.poissonMean)
    poisson.reseedRandomGenerator(0 + partitionIndex + 1)

    instances.map { instance =>
      val sampleArray = generateBagging(numTrees, poisson)
      instance.features match {
        case v: RawDataSparse[T, U] => {
          if (((property.value.permitSparseWorkingData && (v.indices.length + v.indices.length * 4) < v.size) || property.value.numFeatures > memoryUtil.maximumConcurrentNumberOfFeature) && (v.indices.length + v.indices.length * 4) < v.size) {
            new StaticDataClassic[U](instance.label.toByte, v.toWorkingDataSparse(splitter.value), sampleArray)
          } else {
            new StaticDataClassic[U](instance.label.toByte, v.toWorkingDataDense(splitter.value), sampleArray)
          }
        }
        case v: RawDataDense[T, U] => {
          new StaticDataClassic[U](instance.label.toByte, v.toWorkingDataDense(splitter.value), sampleArray)
        }
        case _ => throw new ClassCastException
      }
    }
  }

  override def reGenerateBagging(numTrees: Int,
                                 partitionIndex: Int,
                                 instances: Iterator[StaticData[U]]): Iterator[StaticData[U]] = {
    val poisson = new PoissonDistribution(property.value.poissonMean)
    poisson.reseedRandomGenerator(0 + partitionIndex + 1)

    instances.map { instance => instance.applyBagging(generateBagging(numTrees, poisson)) }
  }

  override def generateRawDataset(sc: SparkContext,
                                  typeInfoBC: Broadcast[TypeInfo[T]],
                                  instrumentedBC: Broadcast[GCInstrumented],
                                  categoricalFeaturesInfoBC: Broadcast[RFCategoryInfo]): RawDataset[T, U] = {
    val b = new RawDatasetBuilder[T, U]
    b.numFeatures = property.value.numFeatures
    b.property = Some(property)
    b.minPartition = property.value.sparkPartition
    b.splitSizeTrainingData = 0.7
    b.typeInfoBC = Some(typeInfoBC)
    b.instrumented = Some(instrumentedBC)
    b.categoricalInfoBC = Some(categoricalFeaturesInfoBC)
    b.build(sc, property.value.dataset)
  }

  override def generateModel(sc: SparkContext, forest: Forest[T, U], typeInfoBC: Broadcast[TypeInfo[T]],
                             macroIteration: Int = 0) = {
    new RFModelStandard[T, U](sc.broadcast(forest), typeInfoBC, property.value.numClasses)
  }
}

/**
  * The strategy to compute random rotation forest according to Blaser et al. "Random rotation ensembles"
  *
  * @param sc         the Spark context
  * @param property   the ReForeSt's property
  * @param typeInfoBC the type information for the raw data
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFStrategyRotation[T: ClassTag, U: ClassTag](@transient sc: SparkContext, property: Broadcast[RFParameter], typeInfoBC: Broadcast[TypeInfo[T]]) extends RFStrategy[T, U]() {

  private val rotationMatrixArrayBC = sc.broadcast(generateMatrices(property.value.numRotation))

  private def generateMatrices(amount: Int) = {
    sc.parallelize(Array.tabulate(amount)(i => i)).map(i => new RFRotationMatrix[T, U](property.value.numFeatures, typeInfoBC.value, i)).collect()
  }

  override def generateBagging(size: Int, distribution: PoissonDistribution) = {
    Array.tabulate(size)(_ => 1.toByte)
  }

  override def findSplits(input: RDD[RawDataLabeled[T, U]],
                          typeInfo: Broadcast[TypeInfo[T]],
                          typeInfoWorking: Broadcast[TypeInfo[U]],
                          instrumented: Broadcast[GCInstrumented],
                          categoricalFeatureInfo: Broadcast[RFCategoryInfo]): (RFSplitterManager[T, U], MemoryUtil) = {
    val (memoryUtil, sampledInput) = findSplitSampleInput(property.value, input, instrumented)

    val splitterArray = new Array[RFSplitter[T, U]](rotationMatrixArrayBC.value.length)
    var count = 0

    while (count < splitterArray.length) {
      splitterArray(count) = property.value.strategySplit.findSplitsSimple(sampledInput.map(t => rotationMatrixArrayBC.value(count).rotate(t)), property.value.getMaxBinNumber, property.value.numFeatures, memoryUtil.maximumConcurrentNumberOfFeature, typeInfo, typeInfoWorking, instrumented, categoricalFeatureInfo)
      count += 1
    }

    (new RFSplitterManagerCollection[T, U](splitterArray, property.value.getMaxBinNumber, property.value.getMaxNumTrees, splitterArray.length, categoricalFeatureInfo.value), memoryUtil)
  }

  override def prepareData(numTrees: Int,
                           macroIteration: Int,
                           splitter: Broadcast[RFSplitter[T, U]],
                           partitionIndex: Int,
                           instances: Iterator[RawDataLabeled[T, U]],
                           instrumented: GCInstrumented,
                           memoryUtil: MemoryUtil): Iterator[StaticData[U]] = {
    instances.map { instance =>
      instance.features match {
        case v: RawData[T, U] => {
          new StaticDataRotationSingle[U](instance.label.toByte, rotationMatrixArrayBC.value(macroIteration).rotateRawData(v).toWorkingDataDense(splitter.value))
        }
        case _ => throw new ClassCastException
      }
    }
  }

  override def generateRawDataset(sc: SparkContext,
                                  typeInfoBC: Broadcast[TypeInfo[T]],
                                  instrumentedBC: Broadcast[GCInstrumented],
                                  categoricalFeaturesInfoBC: Broadcast[RFCategoryInfo]): RawDataset[T, U] = {
    val b = new RawDatasetBuilderRotation[T, U]
    b.numFeatures = property.value.numFeatures
    b.property = Some(property)
    b.minPartition = property.value.sparkPartition
    b.splitSizeTrainingData = 0.7
    b.typeInfoBC = Some(typeInfoBC)
    b.instrumented = Some(instrumentedBC)
    b.categoricalInfoBC = Some(categoricalFeaturesInfoBC)
    val toReturn = b.build(sc, property.value.dataset)

    toReturn
  }

  override def generateModel(sc: SparkContext,
                             forest: Forest[T, U],
                             typeInfoBC: Broadcast[TypeInfo[T]],
                             macroIteration: Int = 0) = {
    new RFModelRotate[T, U](sc.broadcast(forest), typeInfoBC, sc.broadcast(rotationMatrixArrayBC.value(macroIteration)), property.value.numClasses)
  }

  override def getMacroIterationNumber = rotationMatrixArrayBC.value.length
}

/**
  * The strategy implementation for the model selection
  * @param innerStrategy the underlying strategy
  * @param property the configuration properties
  * @tparam T raw data type
  * @tparam U working data type
  */
class RFStrategyModelSelection[T: ClassTag, U: ClassTag](innerStrategy: RFStrategy[T, U], property: Broadcast[RFParameter]) extends RFStrategy[T, U]() {

  override def getSampleSize: Long = innerStrategy.getSampleSize

  override def getMacroIterationNumber: Int = innerStrategy.getMacroIterationNumber

  override def generateBagging(size: Int, distribution: PoissonDistribution): Array[Byte] = innerStrategy.generateBagging(size, distribution)

  override def findSplits(input: RDD[RawDataLabeled[T, U]],
                          typeInfo: Broadcast[TypeInfo[T]],
                          typeInfoWorking: Broadcast[TypeInfo[U]],
                          instrumented: Broadcast[GCInstrumented],
                          categoricalFeatureInfo: Broadcast[RFCategoryInfo]): (RFSplitterManager[T, U], MemoryUtil) =
    innerStrategy.findSplits(input, typeInfo, typeInfoWorking, instrumented, categoricalFeatureInfo)

  override def prepareData(numTrees: Int,
                           macroIteration: Int,
                           splitter: Broadcast[RFSplitter[T, U]],
                           partitionIndex: Int,
                           instances: Iterator[RawDataLabeled[T, U]],
                           instrumented: GCInstrumented,
                           memoryUtil: MemoryUtil): Iterator[StaticData[U]] =
    innerStrategy.prepareData(numTrees, macroIteration, splitter, partitionIndex, instances, instrumented, memoryUtil)

  override def generateModel(sc: SparkContext,
                             forest: Forest[T, U],
                             typeInfoBC: Broadcast[TypeInfo[T]],
                             macroIteration: Int = 0): RFModel[T, U] =
    innerStrategy.generateModel(sc, forest, typeInfoBC, macroIteration)

  override def reGenerateBagging(numTrees: Int,
                                 partitionIndex: Int,
                                 instances: Iterator[StaticData[U]]): Iterator[StaticData[U]] =
    innerStrategy.reGenerateBagging(numTrees, partitionIndex, instances)

  override def generateRawDataset(sc: SparkContext,
                                  typeInfoBC: Broadcast[TypeInfo[T]],
                                  instrumentedBC: Broadcast[GCInstrumented],
                                  categoricalFeaturesInfoBC: Broadcast[RFCategoryInfo]): RawDataset[T, U] = {

    val innerRawDataset = innerStrategy.generateRawDataset(sc, typeInfoBC, instrumentedBC, categoricalFeaturesInfoBC)

    val toReturn = new RawDatasetBuilderModelSelection[T, U](innerRawDataset)

    toReturn.build(sc, property.value.dataset)
  }
}