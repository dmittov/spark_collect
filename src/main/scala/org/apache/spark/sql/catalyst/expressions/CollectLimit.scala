package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.aggregate._

object CollectLimit {
  def collect_truncate(e: Column, limit: Int): Column = withAggregateFunction { CollectTruncate(e.expr, limit) }
  def collect_top(e: Column, ordering: Ordering[InternalRow], limit: Int): Column =
    withAggregateFunction { CollectTop(e.expr, ordering, limit) }
  def collect_sample(e: Column, limit: Int): Column = withAggregateFunction { CollectSample(e.expr, limit) }
  private def withAggregateFunction(func: AggregateFunction,
      isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }
}