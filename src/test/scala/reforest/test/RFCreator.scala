package reforest.test

import reforest.rf.{RFCategoryInfoEmpty}
import reforest.{TypeInfo, TypeInfoDouble, TypeInfoInt}
import reforest.rf.split.{RFSplitter, RFSplitterManager, RFSplitterManagerSingle, RFSplitterSimpleRandom}

object RFCreator {
  val getCategoricalInfo = new RFCategoryInfoEmpty

  def getSplitterRandom[T, U](min: T, max: T, typeInfo: TypeInfo[T], typeInfoWorking: TypeInfo[U], numberBin: Int) = new RFSplitterSimpleRandom[T, U](min, max, typeInfo, typeInfoWorking, numberBin, getCategoricalInfo)

  def getSplitterRandomDefault(min: Double, max: Double, numberBin: Int): RFSplitter[Double, Int] = {
    val a = getSplitterRandom(min, max, new TypeInfoDouble(false, 0), new TypeInfoInt(false, 0), numberBin)
    a
  }

  def getSplitterManager(min: Double,
                         max: Double,
                         numberBin: Int): RFSplitterManager[Double, Int] = {
    new RFSplitterManagerSingle[Double, Int](getSplitterRandomDefault(min, max, numberBin))
  }
}
