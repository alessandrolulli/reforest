package reforest.rf.parameter

import org.junit.{Assert, Test}
import reforest.test.RFCreator

class RFParameterTest {

  @Test
  def builderInit = {
    val b1 = RFCreator.parameterBuilder
      .addParameter(RFParameterType.NumTrees, 100)
    val parameter1 = b1.build

    Assert.assertEquals(1, parameter1.numTrees.length)
    Assert.assertEquals(100, parameter1.getMaxNumTrees)

    val parameter2 = parameter1.applyNumTrees(101)

    Assert.assertEquals(1, parameter1.numTrees.length)
    Assert.assertEquals(1, parameter2.numTrees.length)
    Assert.assertEquals(100, parameter1.getMaxNumTrees)
    Assert.assertEquals(101, parameter2.getMaxNumTrees)
  }
}
