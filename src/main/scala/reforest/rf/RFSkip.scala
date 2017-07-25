package reforest.rf

import reforest.data.tree.Forest

class RFSkip extends Serializable {
  var toSkip : scala.collection.mutable.Set[(Int, Int)] = scala.collection.mutable.Set()

  def addConfigurationToSkip(binNumber : Int, featureMultiplier : Int) = {
    toSkip += ((binNumber, featureMultiplier))
  }

  def contains(binNumber : Int, featureMultiplier : Int) = {
    toSkip.contains((binNumber, featureMultiplier))
  }

  def contains[T, U](forest : Forest[T, U]) = {
    toSkip.contains((forest.binNumber, forest.featurePerNode.getFeaturePerNodeNumber))
  }

  def size = toSkip.size
}

object RFSkip {
  val empty = new RFSkip
}

