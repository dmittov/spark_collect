package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate}
import scala.collection.mutable

@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements with a limit on the number of elements.")
private case class CollectTruncate(child: Expression,
    limit: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[mutable.ArrayBuffer[Any]] {
  def this(child: Expression, limit: Int) = this(child, limit, 0, 0)

  override lazy val deterministic: Boolean = false

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_truncate"

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
