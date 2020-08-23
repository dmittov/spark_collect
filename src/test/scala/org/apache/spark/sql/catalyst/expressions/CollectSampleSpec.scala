package org.apache.spark.sql.catalyst.expressions

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{explode, struct, udf}
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class CollectSampleSpec extends FunSpec with Matchers with DataFrameSuiteBase
  with RDDComparisons with SharedSparkContext {

  import spark.implicits._

  describe("Reservoir tests") {

    it("add one test") {
      val testsCount = 10000
      val testResults = 1 to testsCount map (_ => {
        val initReservoir = Reservoir[Int](
          mutable.PriorityQueue[WeightedItem[Int]](), 0.0, 0.0, 1
        ).add(0, 100.0)
        val updatedReservoir = initReservoir.add(1, 100.0)
        if (updatedReservoir.buffer.map(_.body).head == 1) 1.0d else 0.0
      })
      val successRate = testResults.sum / testResults.length
      val diff = math.abs(successRate - 0.5)
      // 3-sigma chance to fail
      assert(diff < 3.0 / 200)
    }

    it("fillup test") {
      val testsCount = 10000
      val testSize = 100
      val restResults = 1 to testsCount map (_ => {
        val order = Random.nextInt(testSize + 1)
        val testReservoir = (0 to testSize).foldLeft[Reservoir[Int]](
          Reservoir[Int](mutable.PriorityQueue[WeightedItem[Int]](), 0.0, 0.0, 2)
        ) {
          (reservoir: Reservoir[Int], position: Int) => {
            if (position == order) {
              reservoir.add(1, testSize.toDouble)
            } else {
              reservoir.add(0, 1.0)
            }
          }
        }
        val survivedItems: Set[Int] = testReservoir.buffer.map(_.body).toSet
        if (survivedItems contains 1) 1.0d else 0.0d

      })
      val successRate = restResults.sum / restResults.length
      print(successRate)
      assert(successRate > 0.45 && successRate < 0.55)
    }
  }

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
         List(Some(1), None, Some(5), None, None, None)
       ).toDF("num")
       val limited = df.
         agg(CollectLimit.collect_sample($"num", 2).as("nums")).
         select(sort_array($"nums").as("nums"))
       val correct = sc.parallelize(List(Row(List(1, 5))))
       compareRDD(limited.rdd, correct) should be(None)
     }
  }
}
