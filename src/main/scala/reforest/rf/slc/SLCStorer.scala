package reforest.rf.slc

import reforest.data.tree.NodeId

/**
  * It is used to store locally in the machine the indices of the dataset amenable for each node.
  * It is currently used during SLC computation when using the model selection to reduce the computational time
  * between different iterations of the model selection.
  *
  * It should be used also when doing the fully distributed computation to save the indicies of the working data
  * stored in the machine.
  */
object SLCStorer {

  private var m: scala.collection.mutable.Map[NodeId, NodeProcessingList[NodeStackContainer]] = scala.collection.mutable.Map()

  /**
    * Store the indices relative to a node
    *
    * @param nodeKey the node identifier
    * @param value   the list of indices
    */
  def store(nodeKey: NodeId, value: NodeProcessingList[NodeStackContainer]): Unit = {
    this.synchronized(
      m.put(nodeKey, value)
    )
  }

  /**
    * Free all the memory used to store the indices.
    */
  def clear() = {
    this.synchronized({
      if (m.nonEmpty) {
        m = scala.collection.mutable.Map()
      }
    })
  }

  /**
    * Retrieve the indicies relative to a node.
    *
    * @param nodeKey the node identifier
    * @return the list of indices relative to the node
    */
  def get(nodeKey: NodeId): Option[NodeProcessingList[NodeStackContainer]] = {
    var toReturn: Option[NodeProcessingList[NodeStackContainer]] = Option.empty
    this.synchronized({
      toReturn = m.get(nodeKey)
      m.remove(nodeKey)
    })
    toReturn
  }

}
