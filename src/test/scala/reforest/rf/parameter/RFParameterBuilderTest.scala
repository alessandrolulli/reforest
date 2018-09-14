package reforest.rf.parameter

import org.junit.{Assert, Test}
import reforest.test.RFCreator

class RFParameterBuilderTest {

  @Test
  def builderInit = {
    val b1 = RFCreator.parameterBuilder
    val b2 = RFCreator.parameterBuilder

    val parameter1 = b1.build
    val parameter2 = b2.build

    Assert.assertNotEquals(parameter1.UUID, parameter2.UUID)
  }

  @Test
  def builderInitFromParameter = {
    val b1 = RFCreator.parameterBuilder
    val parameter1 = b1.build

    val b2 = RFParameterBuilder.apply(parameter1)
    val parameter2 = b2.build

    Assert.assertEquals(parameter1.UUID, parameter2.UUID)
  }

  @Test
  def builderAddParameter = {
    val b1 = RFParameterBuilder.apply
      .addParameter(RFParameterType.Dataset, "this is required")
      .addParameter(RFParameterType.Instrumented, true)
      .addParameter(RFParameterType.SparkCompressionCodec, "snappy")
      .addParameter(RFParameterType.MaxNodesConcurrent, 5)
      .addParameter(RFParameterType.PoissonMean, 5.3)

    val parameter1 = b1.build

    Assert.assertEquals(true, parameter1.Instrumented)
    Assert.assertEquals("snappy", parameter1.sparkCompressionCodec)
    Assert.assertEquals(5, parameter1.maxNodesConcurrent)
    Assert.assertEquals(5.3, parameter1.poissonMean, 0.000001)
  }
}
