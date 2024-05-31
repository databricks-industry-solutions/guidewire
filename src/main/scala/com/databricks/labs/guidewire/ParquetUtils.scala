package com.databricks.labs.guidewire

import io.delta.standalone.types._
import org.apache.avro.LogicalTypes.{Date, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

object ParquetUtils {

  def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String] = Set.empty[String]): SchemaType = {
    avroSchema.getType match {
      case STRING => SchemaType(new StringType(), nullable = false)
      case BOOLEAN => SchemaType(new BooleanType(), nullable = false)
      case BYTES => SchemaType(new BinaryType(), nullable = false)
      case FIXED => SchemaType(new BinaryType(), nullable = false)
      case DOUBLE => SchemaType(new DoubleType(), nullable = false)
      case FLOAT => SchemaType(new FloatType(), nullable = false)
      case ENUM => SchemaType(new StringType(), nullable = false)
      case NULL => SchemaType(new NullType(), nullable = true)
      case LONG => avroSchema.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => SchemaType(new TimestampType(), nullable = false)
        case _ => SchemaType(new LongType(), nullable = false)
      }
      case INT => avroSchema.getLogicalType match {
        case _: Date => SchemaType(new DateType(), nullable = false)
        case _ => SchemaType(new IntegerType(), nullable = false)
      }
      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IllegalArgumentException(
            s"""
               |Found recursive reference in Avro schema, which can not be processed by Spark:
               |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          new StructField(f.name, schemaType.dataType, schemaType.nullable)
        }
        SchemaType(new StructType(fields.toArray), nullable = false)
      case ARRAY =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(new ArrayType(schemaType.dataType, schemaType.nullable), nullable = false)
      case MAP =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(new MapType(new StringType(), schemaType.dataType, schemaType.nullable), nullable = false)
      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlTypeHelper(remainingUnionTypes.head, existingRecordNames).copy(nullable = true)
          } else {
            toSqlTypeHelper(Schema.createUnion(remainingUnionTypes.asJava), existingRecordNames).copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(_) => toSqlTypeHelper(avroSchema.getTypes.get(0), existingRecordNames)
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) => SchemaType(new LongType(), nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) => SchemaType(new DoubleType(), nullable = false)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlTypeHelper(s, existingRecordNames)
                // All fields are nullable because only one of them is set at a time
                new StructField(s"member$i", schemaType.dataType, true)
            }
            SchemaType(new StructType(fields.toArray), nullable = false)
        }
      case other => throw new IllegalArgumentException(s"Unsupported type $other")
    }
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)

}
