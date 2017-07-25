package reforest.rf.feature

import reforest.data.tree.{ForestManager, NodeId}
import reforest.rf.parameter.RFParameter
import reforest.util.BiMap

/**
  * The manager to retrieve the informations of all the features for all the forests that are generated.
  * It stores also the features that are used in each node of the forests
  *
  * @param parameter the configuration parameters
  */
class RFFeatureManager(parameter: RFParameter) extends Serializable {

  private val featureArrayMap: Array[scala.collection.mutable.Map[NodeId, Array[Int]]] = Array.tabulate(parameter.numForest)(_ => scala.collection.mutable.Map[NodeId, Array[Int]]())

  /**
    * Clear the features used in each node of the forests
    */
  def clearFeatures() = featureArrayMap.foreach(m => m.clear())

  /**
    * Insert the selected features to be used for the given node
    *
    * @param nodeId       the node identifier
    * @param featureArray the set of features to be used in the node
    */
  def addFeatures(nodeId: NodeId, featureArray: Array[Int]): Unit = {
    featureArrayMap(nodeId.forestId).put(nodeId, featureArray)
  }

  /**
    * Generate the features to be used in all the active nodes of the forests
    *
    * @param forestManager the forest manager to retrieve the active nodes
    * @tparam T raw data type
    * @tparam U working data type
    */
  def addFeatures[T, U](forestManager: ForestManager[T, U]) = {
    var forestId = 0
    while (forestId < forestManager.getForest.length) {
      var treeId = 0
      while (treeId < parameter.getMaxNumTrees) {
        featureArrayMap(forestId).put(NodeId(forestId, treeId, 0), parameter.strategyFeature.getFeaturePerNode)
        treeId += 1
      }
      forestId += 1
    }
  }

  /**
    * Retrieve the active nodes (i.e. all the nodes for which it has been generated the set of features)
    *
    * @return the list of active node identifier
    */
  def getActiveNodes: Array[NodeId] = {
    featureArrayMap.flatMap(m => m.keys)
  }

  /**
    * The number of active nodes
    *
    * @return the number of active nodes
    */
  def getActiveNodesNum: Int = {
    featureArrayMap.map(m => m.size).sum
  }

  /**
    * Retrieve all the features that must be used for the active nodes for the given forest
    *
    * @param forestId the forest identifier
    * @return the features to be used for the active nodes
    */
  def getFeatureForForest(forestId: Int): scala.collection.mutable.Map[NodeId, Array[Int]] = {
    featureArrayMap(forestId)
  }

  /**
    * Retrieve the features for the given node
    *
    * @param nodeId the node identifier
    * @return the set of features for the given node
    */
  def getFeature(nodeId: NodeId): Option[Array[Int]] = {
    featureArrayMap(nodeId.forestId).get(nodeId)
  }

  /**
    * It generates the mapping between active nodes identifier and the identifier in the relative matrix
    *
    * @param iteration       the currently sub-iteration number
    * @param iterationNumber the number of iterations of the sub-iteration functionality
    * @return the mapping between active nodes identifier and the identifier in the relative matrix
    */
  def generateIdToId(iteration: Int, iterationNumber: Int): Array[BiMap[NodeId, Int]] = {
    featureArrayMap.map(m => new BiMap(m.keys.filter(n => n.treeId % iterationNumber == iteration).zipWithIndex.toMap))
  }

}
