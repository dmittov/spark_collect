package org.apache.spark.sql.catalyst.expressions

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable
import scala.util.Random

case class WeightedItem[T](
   body: T,
   weight: Double,
   priority: Double
)

object WeightedItem {
 implicit def orderingByPriority[A <: WeightedItem[_]]: Ordering[A] =
   Ordering.by(item => item.priority)
}

/**
 * A-ExpJ Reservoir implementation with merge option
 * @param buffer Reservoir PriorityQueue buffer dequeue extracts Max, therefore priority it's = to -log(weightKey)
 *               of the original algorithm
 * @param weightsToSkip
 * @param passedWeight
 * @param limit Reservoir size
 * @tparam T
 */
case class Reservoir[T](
    buffer: mutable.PriorityQueue[WeightedItem[T]],
    weightsToSkip: Double,
    passedWeight: Double,
    limit: Int) {

  def isFull: Boolean = buffer.length == limit

  def add(item: T, weight: Double): Reservoir[T] = {
    if (isFull) {
      if (weight >= weightsToSkip) {
        replace(item, weight)
      } else discard(weight)
    } else append(item, weight)
  }

  def merge(other: Reservoir[T]): Reservoir[T] = {
    // FIXME: replace stub with implementation
    copy(
      passedWeight = passedWeight + other.passedWeight,
      weightsToSkip = weightsToSkip + other.weightsToSkip
    )
  }

  private[this] def discard(weight: Double): Reservoir[T] = {
    copy(passedWeight = passedWeight + weight, weightsToSkip = weightsToSkip - weight)
  }

  private[this] def append(item: T, weight: Double): Reservoir[T] = {
    val negPriority = math.log(Random.nextDouble()) / weight
    buffer += WeightedItem(item, weight, -negPriority)
    val weightsToSkip = if (buffer.length == limit) math.log(Random.nextDouble) / (-buffer.head.priority) else 0.0
    copy(passedWeight = passedWeight + weight, weightsToSkip = weightsToSkip)
  }

  private[this] def replace(item: T, weight: Double): Reservoir[T] = {
    val offset = math.exp(-buffer.head.priority * weight)
    val negPriority = math.log((1 - offset) * Random.nextDouble + offset) / weight
    buffer.dequeue()
    buffer += WeightedItem(item, weight, -negPriority)
    copy(
      passedWeight = passedWeight + weight,
      weightsToSkip = math.log(Random.nextDouble) / (-buffer.head.priority)
    )
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
    Reservoir(mutable.PriorityQueue.empty, 0.0, 0.0, limit)

  override def update(reservoir: Reservoir[Any], input: InternalRow): Reservoir[Any] = {
    val value = child.eval(input)
    if (value != null) {
      val item = InternalRow.copyValue(value)
      reservoir.add(item, 1.0)
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
      val itemWeight = reservoir.passedWeight / reservoir.buffer.length
      reservoir.buffer.foldLeft(incompleteReservoir) {
        (reservoir, item) => reservoir.add(item.body, itemWeight)
      }
      val resultReservoir = incompleteReservoir.copy(
        weightsToSkip = incompleteReservoir.weightsToSkip + reservoir.weightsToSkip
      )
      resultReservoir
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
