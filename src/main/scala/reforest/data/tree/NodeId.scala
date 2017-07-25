package reforest.data.tree

/**
  * It represents how a node can be recognized in all the forests
  * @param forestId the forest identifier of the node
  * @param treeId the tree identifier in the forest of the node
  * @param nodeId the node identifier in the tree of the node
  */
case class NodeId(val forestId : Int, val treeId : Int, val nodeId : Int) {
}
