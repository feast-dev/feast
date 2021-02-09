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

import com.google.protobuf.{ByteString, Message, Timestamp}
import feast.proto.types.ValueProto
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable

object TypeConversion {
  def sqlTypeToProtoValue(value: Any, `type`: DataType): Message = {
    (`type` match {
      case IntegerType => ValueProto.Value.newBuilder().setInt32Val(value.asInstanceOf[Int])
      case LongType    => ValueProto.Value.newBuilder().setInt64Val(value.asInstanceOf[Long])
      case StringType  => ValueProto.Value.newBuilder().setStringVal(value.asInstanceOf[String])
      case DoubleType  => ValueProto.Value.newBuilder().setDoubleVal(value.asInstanceOf[Double])
      case FloatType   => ValueProto.Value.newBuilder().setFloatVal(value.asInstanceOf[Float])
      case BooleanType => ValueProto.Value.newBuilder().setBoolVal(value.asInstanceOf[Boolean])
      case BinaryType =>
        ValueProto.Value
          .newBuilder()
          .setBytesVal(ByteString.copyFrom(value.asInstanceOf[Array[Byte]]))
      case ArrayType(t: IntegerType, _) =>
        ValueProto.Value
          .newBuilder()
          .setInt32ListVal(
            ValueProto.Int32List.newBuilder
              .addAllVal(value.asInstanceOf[mutable.WrappedArray[Integer]].asJava)
          )
      case ArrayType(t: LongType, _) =>
        ValueProto.Value
          .newBuilder()
          .setInt64ListVal(
            ValueProto.Int64List.newBuilder
              .addAllVal(
                value.asInstanceOf[mutable.WrappedArray[Long]].map(java.lang.Long.valueOf).asJava
              )
          )
      case ArrayType(t: BooleanType, _) =>
        ValueProto.Value
          .newBuilder()
          .setBoolListVal(
            ValueProto.BoolList.newBuilder.addAllVal(
              value
                .asInstanceOf[mutable.WrappedArray[Boolean]]
                .map(java.lang.Boolean.valueOf)
                .asJava
            )
          )
      case ArrayType(t: FloatType, _) =>
        ValueProto.Value
          .newBuilder()
          .setFloatListVal(
            ValueProto.FloatList.newBuilder
              .addAllVal(
                value.asInstanceOf[mutable.WrappedArray[Float]].map(java.lang.Float.valueOf).asJava
              )
          )
      case ArrayType(t: DoubleType, _) =>
        ValueProto.Value
          .newBuilder()
          .setDoubleListVal(
            ValueProto.DoubleList.newBuilder.addAllVal(
              value.asInstanceOf[mutable.WrappedArray[Double]].map(java.lang.Double.valueOf).asJava
            )
          )
      case ArrayType(t: StringType, _) =>
        ValueProto.Value
          .newBuilder()
          .setStringListVal(
            ValueProto.StringList.newBuilder
              .addAllVal(value.asInstanceOf[mutable.WrappedArray[String]].asJava)
          )
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
      case ValueProto.Value.ValCase.INT32_VAL  => v.getInt32Val
      case ValueProto.Value.ValCase.INT64_VAL  => v.getInt64Val
      case ValueProto.Value.ValCase.FLOAT_VAL  => v.getFloatVal
      case ValueProto.Value.ValCase.BOOL_VAL   => v.getBoolVal
      case ValueProto.Value.ValCase.DOUBLE_VAL => v.getDoubleVal
      case ValueProto.Value.ValCase.STRING_VAL => v.getStringVal
      case ValueProto.Value.ValCase.BYTES_VAL  => v.getBytesVal
      case ValueProto.Value.ValCase.INT32_LIST_VAL =>
        v.getInt32ListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.INT64_LIST_VAL =>
        v.getInt64ListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.FLOAT_LIST_VAL =>
        v.getFloatListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.DOUBLE_LIST_VAL =>
        v.getDoubleListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.BOOL_LIST_VAL =>
        v.getBoolListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.STRING_LIST_VAL =>
        v.getStringListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.BYTES_LIST_VAL =>
        v.getBytesListVal.getValList.iterator().asScala.toList
      case ValueProto.Value.ValCase.VAL_NOT_SET =>
        throw new RuntimeException(s"$v not a ValueProto")
    }
  )

  implicit def timestampAsScala(t: Timestamp): AsScala[java.sql.Timestamp] =
    new AsScala[java.sql.Timestamp](
      new sql.Timestamp(t.getSeconds * 1000)
    )

}
