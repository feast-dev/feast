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
package feast.ingestion.validation

import feast.ingestion.FeatureTable
import feast.proto.types.ValueProto.ValueType
import org.apache.spark.sql.types._

object TypeCheck {
  def typesMatch(defType: ValueType.Enum, columnType: DataType): Boolean =
    (defType, columnType) match {
      case (ValueType.Enum.BOOL, BooleanType)                        => true
      case (ValueType.Enum.INT32, IntegerType)                       => true
      case (ValueType.Enum.INT64, LongType)                          => true
      case (ValueType.Enum.FLOAT, FloatType)                         => true
      case (ValueType.Enum.DOUBLE, DoubleType)                       => true
      case (ValueType.Enum.STRING, StringType)                       => true
      case (ValueType.Enum.BYTES, BinaryType)                        => true
      case (ValueType.Enum.BOOL_LIST, ArrayType(_: BooleanType, _))  => true
      case (ValueType.Enum.INT32_LIST, ArrayType(_: IntegerType, _)) => true
      case (ValueType.Enum.INT64_LIST, ArrayType(_: LongType, _))    => true
      case (ValueType.Enum.FLOAT_LIST, ArrayType(_: FloatType, _))   => true
      case (ValueType.Enum.DOUBLE_LIST, ArrayType(_: DoubleType, _)) => true
      case (ValueType.Enum.STRING_LIST, ArrayType(_: StringType, _)) => true
      case (ValueType.Enum.BYTES_LIST, ArrayType(_: BinaryType, _))  => true
      case _                                                         => false
    }

  /**
    * Verify whether types declared in FeatureTable match correspondent columns
    *
    * @param schema       Spark's dataframe columns
    * @param featureTable definition of expected schema
    * @return error details if some column doesn't match
    */
  def allTypesMatch(schema: StructType, featureTable: FeatureTable): Option[String] = {
    val typeByField =
      (featureTable.entities ++ featureTable.features).map(f => f.name -> f.`type`).toMap

    schema.fields
      .map(f =>
        if (typeByField.contains(f.name) && !typesMatch(typeByField(f.name), f.dataType)) {
          Some(
            s"Feature ${f.name} has different type ${f.dataType} instead of expected ${typeByField(f.name)}"
          )
        } else None
      )
      .foldLeft[Option[String]](None) {
        case (Some(error), _) => Some(error)
        case (_, Some(error)) => Some(error)
        case _                => None
      }
  }
}
