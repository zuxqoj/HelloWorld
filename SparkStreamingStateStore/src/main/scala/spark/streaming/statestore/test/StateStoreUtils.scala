package spark.streaming.statestore.test

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object StateStoreUtils {
    val strProj = UnsafeProjection.create(Array[DataType](StringType))
  val intProj = UnsafeProjection.create(Array[DataType](IntegerType))

  def stringToRow(s: String): UnsafeRow = {
    strProj.apply(new GenericInternalRow(Array[Any](UTF8String.fromString(s)))).copy()
  }

  def intToRow(i: Int): UnsafeRow = {
    intProj.apply(new GenericInternalRow(Array[Any](i))).copy()
  }

  def rowToString(row: UnsafeRow): String = {
    row.getUTF8String(0).toString
  }

  def rowToInt(row: UnsafeRow): Int = {
    row.getInt(0)
  }

  def rowsToIntInt(row: (UnsafeRow, UnsafeRow)): (Int, Int) = {
    (rowToInt(row._1), rowToInt(row._2))
  }


  def rowsToStringInt(row: (UnsafeRow, UnsafeRow)): (String, Int) = {
    (rowToString(row._1), rowToInt(row._2))
  }

  def rowsToSet(iterator: Iterator[(UnsafeRow, UnsafeRow)]): Set[(String, Int)] = {
    iterator.map(rowsToStringInt).toSet
  }

}