package reforest.rf.feature

import org.junit.Assert.assertEquals
import reforest.rf.{RFCategoryInfo, RFCategoryInfoEmpty}
import reforest.rf.split.RFSplitter
import test.RFResourceFactory
import org.junit.{Assert, Test}

class RFFeatureSizerTest {
  private val numberBin = 32
  private val numClasses = 10
  private val splitter = RFResourceFactory.getSplitterRandomDefault(-23.5, 12.7, numberBin)
  private val sizer = splitter.generateRFSizer(numClasses)

  @Test
  def getSize(): Unit = {
    assertEquals((numberBin + 1) * numClasses, sizer.getSize(1))
  }

  @Test
  def shrinker(): Unit = {
    val sizer = new RFFeatureSizerSimpleModelSelection(32, 2, new RFCategoryInfoEmpty, 16)
    assertEquals(0, sizer.getShrinkedValue(1, 0))
    assertEquals(1, sizer.getShrinkedValue(1, -1))
    assertEquals(16, sizer.getShrinkedValue(1, 32))
    assertEquals(16, sizer.getShrinkedValue(1, 33))
    assertEquals(16, sizer.getShrinkedValue(1, 100))
  }

  @Test
  def shrinkerSpecialized(): Unit = {
    val splitNumberMap = Map(0 -> 4, 1 -> 8, 2 -> 7)
    val sizer = new RFFeatureSizerSpecializedModelSelection(splitNumberMap, 2, new RFCategoryInfoEmpty, 8, 32)

    assertEquals(5, sizer.getShrinkedValue(0, 5))
    assertEquals(4, sizer.getShrinkedValue(0, 4))
    assertEquals(1, sizer.getShrinkedValue(0, 1))
    assertEquals(0, sizer.getShrinkedValue(0, 0))
    assertEquals(0, sizer.getShrinkedValue(1, 0))
    assertEquals(1, sizer.getShrinkedValue(1, -1))
    assertEquals(8, sizer.getShrinkedValue(1, 32))
    assertEquals(8, sizer.getShrinkedValue(1, 33))
    assertEquals(8, sizer.getShrinkedValue(1, 100))
    assertEquals(8, sizer.getShrinkedValue(2, 8))
    assertEquals(7, sizer.getShrinkedValue(2, 7))
    assertEquals(6, sizer.getShrinkedValue(2, 6))
    assertEquals(2, sizer.getShrinkedValue(2, 2))
    assertEquals(1, sizer.getShrinkedValue(2, 1))
  }

  @Test
  def deShrinker(): Unit = {
    val sizer = new RFFeatureSizerSimpleModelSelection(32, 2, new RFCategoryInfoEmpty, 16)

    assertEquals(0, sizer.getDeShrinkedValue(0, 0))
    assertEquals(0, sizer.getDeShrinkedValue(1, 0))
    assertEquals(32, sizer.getDeShrinkedValue(1, 32))
    assertEquals(32, sizer.getDeShrinkedValue(1, 33))
    assertEquals(32, sizer.getDeShrinkedValue(1, 100))
  }

  @Test
  def deShrinkerSpecialized(): Unit = {
    val splitNumberMap = Map(0 -> 4, 1 -> 8, 2 -> 7)
    val sizer = new RFFeatureSizerSpecializedModelSelection(splitNumberMap, 2, new RFCategoryInfoEmpty, 8, 32)

    assertEquals(5, sizer.getDeShrinkedValue(0, 5))
    assertEquals(4, sizer.getDeShrinkedValue(0, 4))
    assertEquals(1, sizer.getDeShrinkedValue(0, 1))
    assertEquals(0, sizer.getDeShrinkedValue(0, 0))
    assertEquals(0, sizer.getDeShrinkedValue(1, 0))
    assertEquals(9, sizer.getDeShrinkedValue(1, 32))
    assertEquals(9, sizer.getDeShrinkedValue(1, 33))
    assertEquals(9, sizer.getDeShrinkedValue(1, 100))
    assertEquals(8, sizer.getDeShrinkedValue(2, 8))
    assertEquals(7, sizer.getDeShrinkedValue(2, 7))
    assertEquals(6, sizer.getDeShrinkedValue(2, 6))
    assertEquals(2, sizer.getDeShrinkedValue(2, 2))
    assertEquals(1, sizer.getDeShrinkedValue(2, 1))
  }
}
