package reforest.rf.slc

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import reforest.data.StaticData
import reforest.data.tree.NodeId
import reforest.rf.feature.RFFeatureManager

/**
  * It manages the dataset when performing SLC. The actions are:
  * *) free the memory used by the working data in the machines
  * *) collect the entire dataset in each machine to perform SLC
  * *) keep the dataset update on the machines when the number of trees change in the model selection
  * @param sc the Spark context
  * @tparam U working data type
  */
class SLCDataset[U](@transient sc : SparkContext) extends Serializable {

  private var numTreesInit : Int = -1
  private var dataset : Option[Broadcast[Array[StaticData[U]]]] = Option.empty
  private var bucket : Option[Array[NodeId]] = Option.empty
  private var clear : Boolean = false

  /**
    * Initialize the SLCDataset (this must be do in the constructor not in the init...)
    * @param distributdDataset the working data stored in the RDD
    * @param featureManager the feature manager
    * @param numTrees the number of trees to compute
    */
  def init(distributdDataset : RDD[StaticData[U]], featureManager : RFFeatureManager, numTrees : Int) : Unit =  {
    println("SLC INIT DATASET "+numTrees+" "+numTreesInit)
    if(numTrees != numTreesInit) {
      if(dataset.isDefined) {
        dataset.get.destroy()
      }

      numTreesInit = numTrees
      dataset = Some(sc.broadcast(distributdDataset.collect()))
      bucket = Some(featureManager.getActiveNodes)
      clear = true

//      distributdDataset.unpersist()
    } else {
      clear = false
    }
  }

  /**
    * Check if the local dataset has been cleared
    * @return true if the local dataset has been cleared
    */
  def getClear: Boolean = clear

  /**
    * Retrieve the local dataset
    * @return the local dataset
    */
  def getLocalDataset: Broadcast[Array[StaticData[U]]] = {
    if(dataset.isEmpty) {
      throw new RuntimeException("The local dataset has not been initialized. Call the init method before.")
    } else {
      dataset.get
    }
  }

  /**
    * Retrieve the set of root nodes whose trees are still active
    * @return
    */
  def getActiveRoot: Array[NodeId] = {
    if(bucket.isEmpty) {
      throw new RuntimeException("The active roots has not been initialized. Call the init method before.")
    } else {
      bucket.get
    }
  }
}
