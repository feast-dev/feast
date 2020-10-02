/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.utils

import java.sql

import com.google.protobuf.{Message, Timestamp}
import feast.proto.types.ValueProto
import org.apache.spark.sql.types.{
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  TimestampType
}

object TypeConversion {
  def sqlTypeToProtoValue(value: Any, `type`: DataType): Message = {
    (`type` match {
      case IntegerType => ValueProto.Value.newBuilder().setInt32Val(value.asInstanceOf[Int])
      case LongType    => ValueProto.Value.newBuilder().setInt64Val(value.asInstanceOf[Long])
      case StringType  => ValueProto.Value.newBuilder().setStringVal(value.asInstanceOf[String])
      case DoubleType  => ValueProto.Value.newBuilder().setDoubleVal(value.asInstanceOf[Double])
      case FloatType   => ValueProto.Value.newBuilder().setFloatVal(value.asInstanceOf[Float])
      case TimestampType =>
        Timestamp
          .newBuilder()
          .setSeconds(value.asInstanceOf[java.sql.Timestamp].getTime / 1000)
    }).build
  }

  class AsScala[A](op: => A) {
    def asScala: A = op
  }

  implicit def protoValueAsScala(v: ValueProto.Value): AsScala[Any] = new AsScala[Any](
    v.getValCase match {
      case ValueProto.Value.ValCase.INT32_VAL => v.getInt32Val
      case ValueProto.Value.ValCase.FLOAT_VAL => v.getFloatVal
      case ValueProto.Value.ValCase.VAL_NOT_SET =>
        throw new RuntimeException(s"$v not a ValueProto")
    }
  )

  implicit def timestampAsScala(t: Timestamp): AsScala[java.sql.Timestamp] =
    new AsScala[java.sql.Timestamp](
      new sql.Timestamp(t.getSeconds * 1000)
    )

}
