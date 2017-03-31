/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reforest.util

import scala.collection.Map

/**
  * Code from: http://stackoverflow.com/questions/9850786/is-there-such-a-thing-as-bidirectional-maps-in-scala
  */
object BiMap {

  private[BiMap] trait MethodDistinctor

  implicit object MethodDistinctor extends MethodDistinctor

}

case class BiMap[X, Y](map: Map[X, Y]) extends Serializable{
  def this(tuples: (X, Y)*) = this(tuples.toMap)

  private val reverseMap = map map (_.swap)
  require(map.size == reverseMap.size, "no 1 to 1 relation")

  def apply(x: X): Y = map(x)

  def apply(y: Y)(implicit d: BiMap.MethodDistinctor): X = reverseMap(y)

  val domain = map.keys
  val codomain = reverseMap.keys
}