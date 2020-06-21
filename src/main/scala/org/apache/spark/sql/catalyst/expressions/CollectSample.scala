package org.apache.spark.sql.catalyst.expressions

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable

case class WeightedItem[T](
    body: T,
    weight: Double
)

case class Reservoir[T](
    buffer: mutable.ArrayBuffer[WeightedItem[T]],
    var weight: Double,
    limit: Int) {

  def isFull: Boolean = buffer.length == limit

  def add(item: WeightedItem[T]): Reservoir[T] = {
    if (isFull) {
      // FIXME: replace stub with implementation
      replace(item, 0)
    } else append(item)
  }

  def merge(other: Reservoir[T]): Reservoir[T] = {
    // FIXME: replace stub with implementation
    this.copy(weight = weight + other.weight)
  }

  private[this] def append(item: WeightedItem[T]): Reservoir[T] = {
    copy(buffer = buffer :+ item, weight = weight + item.weight)
  }

  private[this] def replace(item: WeightedItem[T], position: Int): Reservoir[T] = {
    this.buffer(position) = item
    copy(weight = weight + item.weight)
  }

}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of <size> non-unique elements " +
    "out of origin list with equal probabilities.")
private case class CollectSample(
    child: Expression,
    limit: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[Reservoir[Any]] {

  override lazy val deterministic: Boolean = false

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_sample"

  override def createAggregationBuffer(): Reservoir[Any] =
    Reservoir(mutable.ArrayBuffer.empty, 0.0, limit)

  override def update(reservoir: Reservoir[Any], input: InternalRow): Reservoir[Any] = {
    val value = child.eval(input)
    if (value != null) {
      val item = WeightedItem(body = InternalRow.copyValue(value), weight = 1.0)
      reservoir.add(item)
    } else reservoir
  }

  override def merge(
      leftReservoir: Reservoir[Any],
      rightReservoir: Reservoir[Any]): Reservoir[Any] = {
    if (leftReservoir.isFull && rightReservoir.isFull) leftReservoir.merge(rightReservoir) else {
      val (reservoir, incompleteReservoir) = (leftReservoir.isFull, rightReservoir.isFull) match {
        case (true, false) => (leftReservoir, rightReservoir)
        case (false, true) => (rightReservoir, leftReservoir)
        case _ => {
          if (leftReservoir.buffer.length < rightReservoir.buffer.length) (leftReservoir, rightReservoir)
          else (rightReservoir, leftReservoir)
        }
      }
      incompleteReservoir.buffer.foldLeft(reservoir) { (reservoir, item) => reservoir.add(item) }
    }
  }

  override def eval(reservoir: Reservoir[Any]): Any = new GenericArrayData(reservoir.buffer.map(_.body))

  override def serialize(instance: Reservoir[Any]): Array[Byte]  = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(stream)
    objectOutputStream.writeObject(instance)
    // FIXME: switch to scala.util.Using when scala 2.13
    objectOutputStream.close()
    stream.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): Reservoir[Any] = {
    // FIXME: switch to scala.util.Using when scala 2.13
    val objectInputStream = new ObjectInputStream(new ByteArrayInputStream(storageFormat))
    val value = objectInputStream.readObject
    objectInputStream.close()
    value.asInstanceOf[Reservoir[Any]]
  }

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

}
