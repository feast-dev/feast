package feast.ingestion.utils

import com.google.protobuf.{Message, Timestamp}
import feast.proto.types.ValueProto
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType}

object TypeConversion {
  def sqlTypeToProtoValue(value: Any, `type`: DataType): Message = {
    (`type` match {
      case IntegerType => ValueProto.Value.newBuilder().setInt32Val(value.asInstanceOf[Int])
      case LongType => ValueProto.Value.newBuilder().setInt64Val(value.asInstanceOf[Long])
      case StringType => ValueProto.Value.newBuilder().setStringVal(value.asInstanceOf[String])
      case DoubleType => ValueProto.Value.newBuilder().setDoubleVal(value.asInstanceOf[Double])
      case FloatType => ValueProto.Value.newBuilder().setFloatVal(value.asInstanceOf[Float])
      case TimestampType => Timestamp.newBuilder()
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
      case ValueProto.Value.ValCase.VAL_NOT_SET => throw new RuntimeException(s"$v not a ValueProto")
    }
  )

}
