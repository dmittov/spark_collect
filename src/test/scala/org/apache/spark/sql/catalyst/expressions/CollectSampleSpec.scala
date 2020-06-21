package org.apache.spark.sql.catalyst.expressions

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{explode, struct, udf}
import org.scalatest.{FunSpec, Matchers}

class CollectSampleSpec extends FunSpec with Matchers with DataFrameSuiteBase
  with RDDComparisons with SharedSparkContext {

  import spark.implicits._

  describe("collect_sample tests") {
    it("check result cardinality") {
      val df = sc.parallelize(DataStubs.Primarchs).toDF("loyal", "id", "name")
      val size = udf { x: Seq[Row] => x.size }
      val limited = df.
        groupBy($"loyal").
        agg(
          CollectLimit.collect_sample(struct($"id", $"name"), 3).as("sample")
        )
      limited.show()
      val correct = sc.parallelize(List(
        Row("yes", 3),
        Row("no", 3)
      ))
      compareRDD(limited.select($"loyal", size($"sample").as("sz")).rdd, correct) should be(None)
    }

    it("check simple datatype") {
      val df = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)).toDF("num")
      val size = udf { x: Seq[Row] => x.size }
      val limited = df.agg(CollectLimit.collect_sample($"num", 4).as("lst"))
      val correct = sc.parallelize(List(Row(4)))
      compareRDD(limited.select(size($"lst").as("cnt")).rdd, correct) should be(None)
    }

    it("incomplete buckets") {
      val df = sc.parallelize(DataStubs.Primarchs).toDF("loyal", "id", "name")
      val limited = df.
        groupBy($"id").
        agg(
          CollectLimit.collect_sample($"name", 2).as("names")
        )
      val result = limited.select($"id", explode($"names").as("name")).rdd
      val correct = df.select($"id", $"name").rdd
      compareRDD(result, correct) should be(None)
    }

    it("null test") {
      val df = sc.parallelize(
        List(Some(1), None, Some(5), None, Some(2), None, Some(3), None)
      ).toDF("num")
      val limited = df.agg(CollectLimit.collect_sample($"num", 2).as("nums"))
      val correct = sc.parallelize(List(Row(List(1, 5))))
      compareRDD(limited.rdd, correct) should be(None)
    }
  }
}
