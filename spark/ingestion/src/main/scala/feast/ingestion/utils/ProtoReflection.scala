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

import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.{AbstractMessage, ByteString, DynamicMessage}
import feast.ingestion.registry.proto.ProtoRegistry
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import collection.convert.ImplicitConversions._

object ProtoReflection {
  def structFieldFor(fd: FieldDescriptor): Option[StructField] = {
    val dataType = fd.getJavaType match {
      case INT         => Some(IntegerType)
      case LONG        => Some(LongType)
      case FLOAT       => Some(FloatType)
      case DOUBLE      => Some(DoubleType)
      case BOOLEAN     => Some(BooleanType)
      case STRING      => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM        => Some(StringType)
      case MESSAGE =>
        fd.getMessageType.getFullName match {
          case "google.protobuf.Timestamp" => Some(TimestampType)
          case name if name.endsWith(".MapEntry") =>
            Some(
              MapType(
                structFieldFor(fd.getMessageType.getFields.head).get.dataType,
                structFieldFor(fd.getMessageType.getFields.last).get.dataType
              )
            )
          case _ =>
            Option(fd.getMessageType.getFields.flatMap(structFieldFor))
              .filter(_.nonEmpty)
              .map(StructType.apply)
        }
    }

    dataType.map(dt =>
      StructField(
        fd.getName,
        if (fd.isRepeated && !dt.isInstanceOf[MapType]) ArrayType(dt, containsNull = false) else dt,
        nullable = !fd.isRequired && !fd.isRepeated
      )
    )
  }

  def inferSchema(protoDescriptor: Descriptor): StructType =
    StructType(
      protoDescriptor.getFields.flatMap(ProtoReflection.structFieldFor)
    )

  private def toRowData(fd: FieldDescriptor, obj: AnyRef): AnyRef = {
    fd.getJavaType match {
      case BYTE_STRING => obj.asInstanceOf[ByteString].toByteArray
      case ENUM        => obj.asInstanceOf[EnumValueDescriptor].getName
      case MESSAGE =>
        fd.getMessageType.getFullName match {
          case "google.protobuf.Timestamp" =>
            val seconds = obj
              .asInstanceOf[DynamicMessage]
              .getField(fd.getMessageType.findFieldByName("seconds"))
              .asInstanceOf[Long]

            new sql.Timestamp(seconds * 1000)
          case _ => messageToRow(fd.getMessageType, obj.asInstanceOf[AbstractMessage])
        }

      case _ => obj
    }
  }

  private def defaultValue(fd: FieldDescriptor): AnyRef = {
    fd.getJavaType match {
      case ENUM    => null
      case MESSAGE => null
      case _       => fd.getDefaultValue
    }
  }

  private def messageToRow(protoDescriptor: Descriptor, message: AbstractMessage): Row = {
    val fields = message.getAllFields

    Row(protoDescriptor.getFields.map { fd =>
      if (fields.containsKey(fd)) {
        val obj = fields.get(fd)
        if (fd.getJavaType.equals(MESSAGE) && fd.getMessageType.getFullName.endsWith(".MapEntry")) {
          obj
            .asInstanceOf[java.util.List[Object]]
            .map(toRowData(fd, _))
            .map { r =>
              (r.asInstanceOf[Row].get(0), r.asInstanceOf[Row].get(1))
            }
            .toMap
        } else if (fd.isRepeated) {
          obj.asInstanceOf[java.util.List[Object]].map(toRowData(fd, _))
        } else {
          toRowData(fd, obj)
        }
      } else if (fd.isRepeated) {
        Seq()
      } else defaultValue(fd)
    }: _*)
  }

  def createMessageParser(protoRegistry: ProtoRegistry, className: String)(
      bytes: Array[Byte]
  ): Row = {
    val protoDescriptor = protoRegistry.getProtoDescriptor(className)

    messageToRow(protoDescriptor, DynamicMessage.parseFrom(protoDescriptor, bytes))
  }
}
