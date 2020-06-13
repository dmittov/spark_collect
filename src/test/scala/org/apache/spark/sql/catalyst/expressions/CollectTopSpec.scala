package org.apache.spark.sql.catalyst.expressions

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{explode, struct}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.scalatest.{FunSpec, Matchers}


class CollectTopSpec extends FunSpec with Matchers with DataFrameSuiteBase
  with RDDComparisons with SharedSparkContext {

  import spark.implicits._

  describe("Struct comparison tests") {
    it("Long comparison") {
      val sLess = InternalRow("Fulgrim", 3L)
      val sOtherLess = InternalRow("fake record", 3L)
      val sGreater = InternalRow("Angron", 12L)
      val asc = AscStruct(1, LongType)
      val desc = DescStruct(1, LongType)
      assert(asc.lt(sLess, sGreater))
      assert(asc.lteq(sLess, sOtherLess))
      assert(desc.gt(sLess, sGreater))
      assert(desc.gteq(sLess, sOtherLess))
    }

    it("Double comparison") {
      val sLess = InternalRow("Fulgrim", 3.27)
      val sOtherLess = InternalRow("fake record", 3.27)
      val sGreater = InternalRow("Angron", 12.41)
      val asc = AscStruct(1, DoubleType)
      val desc = DescStruct(1, DoubleType)
      assert(asc.lt(sLess, sGreater))
      assert(asc.lteq(sLess, sOtherLess))
      assert(desc.gt(sLess, sGreater))
      assert(desc.gteq(sLess, sOtherLess))
    }
  }

  describe("collect_top tests") {
    it("less functional test") {
      val df = sc.parallelize(DataStubs.Primarchs).toDF("loyal", "id", "name")
      val limited = df.
        groupBy($"loyal").
        agg(
          CollectLimit.collect_top(struct($"id", $"name"), AscStruct(0, IntegerType), 2).as("top")
        ).
        withColumn("primarch", explode($"top")).
        select($"loyal", $"primarch.id".as("id"), $"primarch.name".as("name"))
      limited.show(10, false)
      val correct = sc.parallelize(List(
        Row("yes", 1, "Lion El'Jonson"),
        Row("yes", 5, "Jaghatai Khan"),
        Row("no", 3, "Fulgrim"),
        Row("no", 4, "Perturabo")
      ))
      compareRDD(limited.rdd, correct) should be(None)
    }

    it("collect_top greater functional test") {
      val df = sc.parallelize(DataStubs.Primarchs).toDF("loyal", "id", "name")
      val limited = df.
        groupBy($"loyal").
        agg(
          CollectLimit.collect_top(struct($"id", $"name"), DescStruct(0, IntegerType), 2).as("top")
        ).
        withColumn("primarch", explode($"top")).
        select($"loyal", $"primarch.id".as("id"), $"primarch.name".as("name"))
      limited.show(10, false)
      val correct = sc.parallelize(List(
        Row("yes", 18, "Vulkan"),
        Row("yes", 19, "Corvus Corax"),
        Row("no", 20, "Alpharius Omegon"),
        Row("no", 17, "Lorgar Aurelian")
      ))
      compareRDD(limited.rdd, correct) should be(None)
    }

    it("incomplete buckets") {
      val df = sc.parallelize(DataStubs.Primarchs).toDF("loyal", "id", "name")
      val limited = df.
        groupBy($"id").
        agg(
          CollectLimit.collect_top(struct($"name"), DescStruct(0, StringType), 2).as("names")
        )
      limited.show(10, false)
      val result = limited.select($"id", explode($"names").as("nameStruct")).
        select($"id", $"nameStruct.name".as("name")).rdd
      val correct = df.select($"id", $"name").rdd
      compareRDD(result, correct) should be(None)
    }

    it("null test") {
      val df = sc.parallelize(
        List(Some(1), None, Some(5), None, Some(2), None, Some(3), None)
      ).toDF("num")
      val limited = df.
        agg(
          CollectLimit.collect_top(struct($"num"), DescStruct(0, IntegerType), 2).as("nums")
        ).select(explode($"nums").as("numStruct")).select($"numStruct.num".as("num"))
      val correct = sc.parallelize(List(Row(3), Row(5)))
      compareRDD(limited.rdd, correct) should be(None)
    }
  }
}
