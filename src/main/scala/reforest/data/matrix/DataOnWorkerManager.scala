package reforest.data.matrix

import reforest.data.tree.{ForestManager, NodeId}
import reforest.rf.RFSkip
import reforest.rf.feature.RFFeatureManager
import reforest.util.BiMap

/**
  * An utility class to handle the matrices in each machine.
  * For each combination of binNumber and featurePerNodeultiplier a matrix is initialized.
  */
object DataOnWorkerManager {
  var matrixArray: Option[Array[DataOnWorker]] = Option.empty
  var taken = false
  private var iterationInit = (-1, -1)

  /**
    * Retrieve all the matrices stored in the machine.
    *
    * @return an array with all the matrices stored in the machine
    */
  def getMatrix: Option[Array[(DataOnWorker, Int)]] = {
    var toReturn: Option[Array[(DataOnWorker, Int)]] = Option.empty
    this.synchronized(
      if (matrixArray.isDefined && !taken) {
        toReturn = Some(matrixArray.get.zipWithIndex.filter(t => t._1 != null))
        taken = true
      }
    )
    toReturn
  }

  /**
    * Iteratively calls the utility to free the memory of each matrix
    */
  def releaseMatrix() = {
    this.synchronized(
      if (matrixArray.isDefined) {
        var forestId = 0
        while (forestId < matrixArray.get.length) {
          if(matrixArray.get(forestId)!=null) {
            matrixArray.get(forestId).releaseMatrix
          }
          forestId += 1
        }
      }
    )
  }

  /**
    * It retrieves the matrix with the given index
    *
    * @param forestId the forest identifier
    * @return the matrix relative to the forest identifier
    */
  def getForestMatrix(forestId: Int) = {
    matrixArray.get(forestId)
  }

  /**
    * Initialize all the matrices in the machine. This must be called one time per iteration.
    *
    * @param forestManager   the forest manager
    * @param featureManager  the features manager
    * @param depth           the currently processed depth
    * @param iteration       the iteration of the sub-iteration functionality
    * @param iterationNumber the number of iterations of the sub-iteration functionality
    * @param numClasses      the number of classes in the dataset
    * @param idToId          the mapping between node ids and index in the relative matrix
    * @tparam T raw data type
    * @tparam U working data type
    */
  def init[T, U](forestManager: ForestManager[T, U],
                 featureManager: RFFeatureManager,
                 depth: Int,
                 iteration: Int,
                 iterationNumber: Int,
                 numClasses: Int,
                 idToId: Array[BiMap[NodeId, Int]],
                 skip: RFSkip): Unit = {
    this.synchronized(
      if (iterationInit != (depth, iteration)) {
        iterationInit = (depth, iteration)
        taken = false

        matrixArray = Some(new Array[DataOnWorker](forestManager.getForest.length))
        var forestId = 0
        while (forestId < forestManager.getForest.length) {
          if (!skip.contains(forestManager.getForest(forestId))) {
            matrixArray.get(forestId) = new DataOnWorker(forestId, numClasses, featureManager)
          }
          forestId += 1
        }
      }
    )

    var forestId = 0
    while (forestId < matrixArray.get.length) {
      if (matrixArray.get(forestId) != null) {
        matrixArray.get(forestId).init(forestManager.getForest(forestId), idToId(forestId), iteration, iterationNumber, depth)
      }
      forestId += 1
    }
  }
}
