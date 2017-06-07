package test

import org.apache.spark.broadcast.Broadcast
import reforest.{TypeInfo, TypeInfoDouble, TypeInfoInt}

import scala.reflect.ClassTag

/**
  * Created by lulli on 6/7/17.
  */
class BroadcastSimple[T: ClassTag](v: T) extends Broadcast[T](0) {
  override def value: T = v

  override def getValue(): T = v

  override def doDestroy(blocking: Boolean) = {}

  override def doUnpersist(blocking: Boolean) = {}
}

object BroadcastSimple {
  val typeInfoInt = new BroadcastSimple[TypeInfoInt](new TypeInfoInt(false, -100))
  val typeInfoDouble = new BroadcastSimple[TypeInfoDouble](new TypeInfoDouble(false, -100))
}
