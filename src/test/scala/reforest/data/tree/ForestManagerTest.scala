package reforest.data.tree

import org.junit.{Assert, Test}
import reforest.data.RawData
import reforest.rf.parameter.{RFParameterBuilder, RFParameterType}
import reforest.test._

class ForestManagerTest {

  val splitterManager = RFCreator.getSplitterManager(0, 100, 32)

  @Test
  def construct() = {

    val forestManager = new ForestManager[Double, Int](RFCreator.parameterBuilder.build, splitterManager)

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
