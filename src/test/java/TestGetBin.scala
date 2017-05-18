//import org.scalatest.{FlatSpec, Matchers}
//import randomForest.test.freshOK.{RFAllInLocalData}
//
//import scala.collection.Map
//
///**
//  * Created by lulli on 02/02/2017.
//  */
//class TestGetBin extends FlatSpec with Matchers{
//
//  val split : Map[Int,Array[Double]] = Map(0 -> Array(-10.5, -5d,-1d,1.5,7d))
//
//  def checkDataInCorrectBin() = {
//    assert(RFAllInLocalData.getBin(0, 0, split) == 0)
//    assert(RFAllInLocalData.getBin(0, -20.5, split) == 1)
//    assert(RFAllInLocalData.getBin(0, -10.5, split) == 1)
//    assert(RFAllInLocalData.getBin(0, -8.5, split) == 2)
//    assert(RFAllInLocalData.getBin(0, -5, split) == 2)
//    assert(RFAllInLocalData.getBin(0, -1, split) == 3)
//    assert(RFAllInLocalData.getBin(0, 1.5, split) == 4)
//    assert(RFAllInLocalData.getBin(0, 7, split) == 5)
//    assert(RFAllInLocalData.getBin(0, 7.5, split) == 5)
//  }
//}
