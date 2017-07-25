package reforest.data.tree

import org.junit.{Assert, Test}
import reforest.data.RawData
import reforest.rf.parameter.{RFParameterBuilder, RFParameterType}
import reforest.test._

class ForestManagerTest {

  val parameter = new RFParameterBuilder()
    .addParameter(RFParameterType.NumTrees, Array(50, 100))
    .addParameter(RFParameterType.Depth, Array(5, 10))
    .addParameter(RFParameterType.BinNumber, Array(16, 32))
    .build

  val splitterManager = RFCreator.getSplitterManager(0, 100, 32)

  @Test
  def construct() = {

    val forestManager = new ForestManager[Double, Int](parameter, splitterManager)

    Assert.assertEquals(2, forestManager.getForest.length)

    Assert.assertEquals(100, forestManager.getForest(0).numTrees)
    Assert.assertEquals(10, forestManager.getForest(0).maxDepth)
    Assert.assertEquals(16, forestManager.getForest(0).binNumber)

    Assert.assertEquals(100, forestManager.getForest(1).numTrees)
    Assert.assertEquals(10, forestManager.getForest(1).maxDepth)
    Assert.assertEquals(32, forestManager.getForest(1).binNumber)

    Assert.assertEquals(100, forestManager.getNumTrees)

  }
}
