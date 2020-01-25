package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.AtomicType
import scala.collection.mutable

class AscStruct(val fieldIndex: Int, dataType: AtomicType) extends Ordering[InternalRow] {
  override def compare(x: InternalRow, y: InternalRow): Int = {
    val lhs = x.get(fieldIndex, dataType)
    val rhs = y.get(fieldIndex, dataType)
    type InternalType = dataType.InternalType
    val ordering = dataType.ordering
    ordering.compare(lhs.asInstanceOf[InternalType], rhs.asInstanceOf[InternalType])
  }
}

object AscStruct {
  def apply(fieldIndex: Int, dataType: AtomicType): AscStruct = new AscStruct(fieldIndex, dataType)
}

class DescStruct(val fieldIndex: Int, dataType: AtomicType) extends Ordering[InternalRow] {
  val lessStruct = new AscStruct(fieldIndex, dataType)
  override def compare(x: InternalRow, y: InternalRow): Int = {
    - lessStruct.compare(x, y)
  }
}

object DescStruct {
  def apply(fieldIndex: Int, dataType: AtomicType) = new DescStruct(fieldIndex, dataType)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements with a limit on the number of elements.")
private case class CollectListLimit(child: Expression,
    limit: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[mutable.ArrayBuffer[Any]] {
  def this(child: Expression, limit: Int) = this(child, limit, 0, 0)

  override lazy val deterministic: Boolean = false

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_list_limit"

  override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def update(buffer: mutable.ArrayBuffer[Any], input: InternalRow): mutable.ArrayBuffer[Any] = {
    if (buffer.size < limit) {
      val value = child.eval(input)
      if (value != null) {
        val item = InternalRow.copyValue(value)
        buffer += item
      }
    }
    buffer
  }

  override def merge(buffer: mutable.ArrayBuffer[Any], other: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
    val need = limit - buffer.size
    if (need > 0) {
      buffer ++= other.take(need)
    }
    buffer
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a top (by specified ordering) list of non-unique elements " +
    "with a limit on the number of elements.")
private case class CollectTop(child: Expression,
    ordering: Ordering[InternalRow],
    limit: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[mutable.PriorityQueue[Any]] {
  implicit val ord = ordering

  def this(child: Expression,
      ordering: Ordering[InternalRow],
      limit: Int) = this(child, ordering, limit, 0, 0)

  override lazy val deterministic: Boolean = true

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_top"

  override def createAggregationBuffer(): mutable.PriorityQueue[Any] =
    mutable.PriorityQueue.empty[InternalRow].asInstanceOf[mutable.PriorityQueue[Any]]

  override def update(buffer: mutable.PriorityQueue[Any],
      input: InternalRow): mutable.PriorityQueue[Any] = {
    val value = child.eval(input)
    if (value != null) {
      val item = value.asInstanceOf[InternalRow]
      if (buffer.size < limit) {
        buffer += item.copy()
      }
      else if (ordering.lt(item, buffer.head.asInstanceOf[InternalRow])) {
        buffer += item.copy()
        buffer.dequeue()
      }
    }
    buffer
  }

  override def merge(buffer: mutable.PriorityQueue[Any],
      other: mutable.PriorityQueue[Any]): mutable.PriorityQueue[Any] = {
    other.map(x => {
      val item = x.asInstanceOf[InternalRow]
      if (buffer.size < limit) {
        buffer += item
      } else if (ordering.lt(item, buffer.head.asInstanceOf[InternalRow])){
        buffer += item
        buffer.dequeue()
      }
    })
    buffer
  }
}

object CollectLimit {
  def collect_list_limit(e: Column, limit: Int): Column = withAggregateFunction { CollectListLimit(e.expr, limit) }
  def collect_top(e: Column, ordering: Ordering[InternalRow], limit: Int): Column =
    withAggregateFunction { CollectTop(e.expr, ordering, limit) }
  private def withAggregateFunction(func: AggregateFunction,
      isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }
}