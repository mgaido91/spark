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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods

/**
 * A builder of [[ArrayBasedMapData]], which fails if a null map key is detected, and removes
 * duplicated map keys w.r.t. the last wins policy.
 */
class ArrayBasedMapBuilder(keyType: DataType, valueType: DataType) extends Serializable {
  assert(!keyType.existsRecursively(_.isInstanceOf[MapType]), "key of map cannot be/contain map")
  assert(keyType != NullType, "map key cannot be null type.")

  private lazy val keyToIndex = keyType match {
    // Binary type data is `byte[]`, which can't use `==` to check equality.
    case _: AtomicType | _: CalendarIntervalType if !keyType.isInstanceOf[BinaryType] =>
      new java.util.HashMap[Any, Int]()
    case _ =>
      // for complex types, use interpreted ordering to be able to compare unsafe data with safe
      // data, e.g. UnsafeRow vs GenericInternalRow.
      new java.util.TreeMap[Any, Int](TypeUtils.getInterpretedOrdering(keyType))
  }

  // TODO: specialize it
  private lazy val keys = mutable.ArrayBuffer.empty[Any]
  private lazy val values = mutable.ArrayBuffer.empty[Any]

  private lazy val keyGetter = InternalRow.getAccessor(keyType)
  private lazy val valueGetter = InternalRow.getAccessor(valueType)

  def put(key: Any, value: Any, withSizeCheck: Boolean = false): Unit = {
    if (key == null) {
      throw new RuntimeException("Cannot use null as map key.")
    }

    val index = keyToIndex.getOrDefault(key, -1)
    if (index == -1) {
      if (withSizeCheck && size >= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
        throw new RuntimeException(s"Unsuccessful attempt to concat maps with $size elements " +
          s"due to exceeding the map size limit ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
      }
      keyToIndex.put(key, values.length)
      keys.append(key)
      values.append(value)
    } else {
      // Overwrite the previous value, as the policy is last wins.
      values(index) = value
    }
  }

  // write a 2-field row, the first field is key and the second field is value.
  def put(entry: InternalRow): Unit = {
    if (entry.isNullAt(0)) {
      throw new RuntimeException("Cannot use null as map key.")
    }
    put(keyGetter(entry, 0), valueGetter(entry, 1))
  }

  def putAll(keyArray: ArrayData, valueArray: ArrayData): Unit = {
    if (keyArray.numElements() != valueArray.numElements()) {
      throw new RuntimeException(
        "The key array and value array of MapData must have the same length.")
    }
    val sizeCheckRequired =
      size + keyArray.numElements() > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    var i = 0
    while (i < keyArray.numElements()) {
      put(keyGetter(keyArray, i), valueGetter(valueArray, i), withSizeCheck = sizeCheckRequired)
      i += 1
    }
  }

  private def reset(): Unit = {
    keyToIndex.clear()
    keys.clear()
    values.clear()
  }

  /**
   * Builds the result [[ArrayBasedMapData]] and reset this builder to free up the resources. The
   * builder becomes fresh afterward and is ready to take input and build another map.
   */
  def build(): ArrayBasedMapData = {
    val map = new ArrayBasedMapData(
      new GenericArrayData(keys.toArray), new GenericArrayData(values.toArray))
    reset()
    map
  }

  /**
   * Builds a [[ArrayBasedMapData]] from the given key and value array and reset this builder. The
   * builder becomes fresh afterward and is ready to take input and build another map.
   */
  def from(keyArray: ArrayData, valueArray: ArrayData): ArrayBasedMapData = {
    assert(keyToIndex.isEmpty, "'from' can only be called with a fresh ArrayBasedMapBuilder.")
    putAll(keyArray, valueArray)
    if (keyToIndex.size == keyArray.numElements()) {
      // If there is no duplicated map keys, creates the MapData with the input key and value array,
      // as they might already in unsafe format and are more efficient.
      reset()
      new ArrayBasedMapData(keyArray, valueArray)
    } else {
      build()
    }
  }

  /**
   * Returns the current size of the map which is going to be produced by the current builder.
   */
  def size: Int = keys.size
}
