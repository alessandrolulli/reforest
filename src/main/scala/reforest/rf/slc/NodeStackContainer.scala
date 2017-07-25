package reforest.rf.slc

/**
  * Contains the information stored in the stack of nodes to be computed in the SLC, namely the node identifier to be computed
  * and the list of indices of data amenable for the node in the dataset
  * @param nodeId the node identifier
  * @param mapping the list of indices in the dataset of the data amenable for the node
  */
case class NodeStackContainer(val nodeId : Int, val mapping : Option[ElementMapping])