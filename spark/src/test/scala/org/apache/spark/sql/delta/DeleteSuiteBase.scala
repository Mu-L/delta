/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

trait DeleteBaseMixin
  extends QueryTest
  with SharedSparkSession
  with DeltaDMLTestUtils
  with DeltaTestUtilsForTempViews
  with DeltaExcludedBySparkVersionTestMixinShims {

  import testImplicits._

  protected def executeDelete(target: String, where: String = null): Unit

  protected def checkDelete(
      condition: Option[String],
      expectedResults: Seq[Row],
      tableName: Option[String] = None): Unit = {
    val target = tableName.getOrElse(tableSQLIdentifier)
    executeDelete(target = target, where = condition.orNull)
    checkAnswer(readDeltaTableByIdentifier(target), expectedResults)
  }

  protected def testInvalidTempViews(name: String)(
      text: String,
      expectedErrorMsgForSQLTempView: String = null,
      expectedErrorMsgForDataSetTempView: String = null,
      expectedErrorClassForSQLTempView: String = null,
      expectedErrorClassForDataSetTempView: String = null): Unit = {
    testWithTempView(s"test delete on temp view - $name") { isSQLTempView =>
      withTable("tab") {
        Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        if (isSQLTempView) {
          sql(s"CREATE TEMP VIEW v AS $text")
        } else {
          sql(text).createOrReplaceTempView("v")
        }
        val ex = intercept[AnalysisException] {
          executeDelete(
            "v",
            "key >= 1 and value < 3"
          )
        }
        testErrorMessageAndClass(
          isSQLTempView,
          ex,
          expectedErrorMsgForSQLTempView,
          expectedErrorMsgForDataSetTempView,
          expectedErrorClassForSQLTempView,
          expectedErrorClassForDataSetTempView)
      }
    }
  }

  // Need to be able to override this, because it works in some configurations.
  protected def testSuperSetColsTempView(): Unit = {
    testInvalidTempViews("superset cols")(
      text = "SELECT key, value, 1 FROM tab",
      // The analyzer can't tell whether the table originally had the extra column or not.
      expectedErrorMsgForSQLTempView = "Can't resolve column 1 in root",
      expectedErrorMsgForDataSetTempView = "Can't resolve column 1 in root"
    )
  }

  protected def testComplexTempViews(name: String)(
      text: String,
      expectResult: Seq[Row]): Unit = {
    testWithTempView(s"test delete on temp view - $name") { isSQLTempView =>
        withTable("tab") {
          Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
          createTempViewFromSelect(text, isSQLTempView)
          executeDelete(
            "v",
            "key >= 1 and value < 3"
          )
          checkAnswer(spark.read.format("delta").table("v"), expectResult)
        }
    }
  }
}

trait DeleteTempViewTests extends DeleteBaseMixin with DeltaDMLTestUtilsPathBased {
  import testImplicits._

  Seq(true, false).foreach { isPartitioned =>
    val name = s"test delete on temp view - basic - Partition=$isPartitioned"
    testWithTempView(name) { isSQLTempView =>
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)
      createTempViewFromTable(tableSQLIdentifier, isSQLTempView)
        checkDelete(
          condition = Some("key <= 1"),
          expectedResults = Row(2, 2) :: Nil,
          tableName = Some("v"))
    }
  }

  testInvalidTempViews("subset cols")(
    text = "SELECT key FROM tab",
    expectedErrorClassForSQLTempView = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    expectedErrorClassForDataSetTempView = "UNRESOLVED_COLUMN.WITH_SUGGESTION"
  )

  testSuperSetColsTempView()

  testComplexTempViews("nontrivial projection")(
    text = "SELECT value as key, key as value FROM tab",
    expectResult = Row(3, 0) :: Nil
  )

  testComplexTempViews("view with too many internal aliases")(
    text = "SELECT * FROM (SELECT * FROM tab AS t1) AS t2",
    expectResult = Row(0, 3) :: Nil
  )
}

trait DeleteBaseTests extends DeleteBaseMixin {
  import testImplicits._

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(condition = None, Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - delete from a Delta table - Partition=$isPartitioned") {
      withTable("deltaTable") {
        val partitions = if (isPartitioned) "key" :: Nil else Nil
        val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
        append(input, partitions)

        checkDelete(Some("value = 4 and key = 3"),
          Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
        checkDelete(Some("value = 4 and key = 1"),
          Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil)
        checkDelete(Some("value = 2 or key = 1"),
          Row(0, 3) :: Nil)
        checkDelete(Some("key = 0 or value = 99"), Nil)
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic key columns - Partition=$isPartitioned") {
      val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(input, partitions)

      checkDelete(Some("key > 2"), Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      checkDelete(Some("key < 2"), Row(2, 2) :: Nil)
      checkDelete(Some("key = 2"), Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where key columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(Some("key = 1"), Row(2, 2) :: Row(0, 3) :: Nil)
      checkDelete(Some("key = 2"), Row(0, 3) :: Nil)
      checkDelete(Some("key = 0"), Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where data columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(Some("value <= 2"), Row(1, 4) :: Row(0, 3) :: Nil)
      checkDelete(Some("value = 3"), Row(1, 4) :: Nil)
      checkDelete(Some("value != 0"), Nil)
    }
  }

  test("where data columns and partition columns") {
    val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
    append(input, Seq("key"))

    checkDelete(Some("value = 4 and key = 3"),
      Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    checkDelete(Some("value = 4 and key = 1"),
      Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil)
    checkDelete(Some("value = 2 or key = 1"),
      Row(0, 3) :: Nil)
    checkDelete(Some("key = 0 or value = 99"),
      Nil)
  }

  Seq(true, false).foreach { skippingEnabled =>
    Seq(true, false).foreach { isPartitioned =>
      test(s"data and partition columns - Partition=$isPartitioned Skipping=$skippingEnabled") {
        withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString) {
          val partitions = if (isPartitioned) "key" :: Nil else Nil
          val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
          append(input, partitions)

          checkDelete(Some("value = 4 and key = 3"),
            Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
          checkDelete(Some("value = 4 and key = 1"),
            Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil)
          checkDelete(Some("value = 2 or key = 1"),
            Row(0, 3) :: Nil)
          checkDelete(Some("key = 0 or value = 99"),
            Nil)
        }
      }
    }
  }

  test("Negative case - non-Delta target") {
    writeTable(
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value")
        .write
        .mode("overwrite")
        .format("parquet"),
      tableSQLIdentifier)
    intercept[SparkThrowable] {
      executeDelete(target = tableSQLIdentifier)
    } match {
      // Thrown when running with path-based SQL
      case e: DeltaAnalysisException if e.getCondition == "DELTA_TABLE_NOT_FOUND" =>
        checkError(e, "DELTA_TABLE_NOT_FOUND",
          parameters = Map("tableName" -> tableSQLIdentifier.stripPrefix("delta.")))
      case e: DeltaAnalysisException if e.getCondition == "DELTA_MISSING_TRANSACTION_LOG" =>
        checkErrorMatchPVals(e, "DELTA_MISSING_TRANSACTION_LOG",
          parameters = Map("operation" -> "read from", "path" -> ".*", "docLink" -> "https://.*"))
      // Thrown when running with path-based Scala API
      case e: DeltaAnalysisException if e.getCondition == "DELTA_MISSING_DELTA_TABLE" =>
        checkError(e, "DELTA_MISSING_DELTA_TABLE",
          parameters = Map("tableName" -> tableSQLIdentifier.stripPrefix("delta.")))
      // Thrown when running with name-based SQL
      case e: AnalysisException =>
        checkErrorMatchPVals(e, "UNSUPPORTED_FEATURE.TABLE_OPERATION",
          parameters = Map(
            "tableName" -> s".*$tableSQLIdentifier.*",
            "operation" -> "DELETE"))
    }
  }

  test("Negative case - non-deterministic condition") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    val e = intercept[AnalysisException] {
      executeDelete(target = tableSQLIdentifier, where = "rand() > 0.5")
    }.getMessage
    assert(e.contains("nondeterministic expressions are only allowed in") ||
      e.contains("The operator expects a deterministic expression"))
  }

  test("Negative case - DELETE the child directory",
      NameBasedAccessIncompatible) {
    withTempPath { tempDir =>
      val tempPath = tempDir.getCanonicalPath
      val df = Seq((2, 2), (3, 2)).toDF("key", "value")
      df.write.format("delta").partitionBy("key").save(tempPath)

      val e = intercept[AnalysisException] {
        executeDelete(target = s"delta.`$tempPath/key=2`", where = "value = 2")
      }.getMessage
      assert(e.contains("Expect a full scan of Delta sources, but found a partial scan"))
    }
  }

  test("delete cached table by name") {
    withTable("cached_delta_table") {
      Seq((2, 2), (1, 4)).toDF("key", "value")
        .write.format("delta").saveAsTable("cached_delta_table")

      spark.table("cached_delta_table").cache()
      spark.table("cached_delta_table").collect()
      executeDelete(target = "cached_delta_table", where = "key = 2")
      checkAnswer(spark.table("cached_delta_table"), Row(1, 4) :: Nil)
    }
  }

  test("delete cached table") {
    append(Seq((2, 2), (1, 4)).toDF("key", "value"))
    readDeltaTableByIdentifier().cache()
    readDeltaTableByIdentifier().collect()
    executeDelete(tableSQLIdentifier, where = "key = 2")
    checkAnswer(readDeltaTableByIdentifier(), Row(1, 4) :: Nil)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"condition having current_date - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(
        Seq((java.sql.Date.valueOf("1969-12-31"), 2),
          (java.sql.Date.valueOf("2099-12-31"), 4))
          .toDF("key", "value"), partitions)

      checkDelete(Some("CURRENT_DATE > key"),
        Row(java.sql.Date.valueOf("2099-12-31"), 4) :: Nil)
      checkDelete(Some("CURRENT_DATE <= key"), Nil)
    }
  }

  test("condition having current_timestamp - Partition by Timestamp") {
    append(
      Seq((java.sql.Timestamp.valueOf("2012-12-31 16:00:10.011"), 2),
        (java.sql.Timestamp.valueOf("2099-12-31 16:00:10.011"), 4))
        .toDF("key", "value"), Seq("key"))

    checkDelete(Some("CURRENT_TIMESTAMP > key"),
      Row(java.sql.Timestamp.valueOf("2099-12-31 16:00:10.011"), 4) :: Nil)
    checkDelete(Some("CURRENT_TIMESTAMP <= key"), Nil)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"foldable condition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      val allRows = Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil

      checkDelete(Some("false"), allRows)
      checkDelete(Some("1 <> 1"), allRows)
      checkDelete(Some("1 > null"), allRows)
      checkDelete(Some("true"), Nil)
      checkDelete(Some("1 = 1"), Nil)
    }
  }

  test("SC-12232: should not delete the rows where condition evaluates to null") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // "null = null" evaluates to null
    checkDelete(Some("value = null"),
      Row("a", null) :: Row("b", null) :: Row("c", "v") :: Row("d", "vv") :: Nil)

    // these expressions evaluate to null when value is null
    checkDelete(Some("value = 'v'"),
      Row("a", null) :: Row("b", null) :: Row("d", "vv") :: Nil)
    checkDelete(Some("value <> 'v'"),
      Row("a", null) :: Row("b", null) :: Nil)
  }

  test("SC-12232: delete rows with null values using isNull") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // when value is null, this expression evaluates to true
    checkDelete(Some("value is null"),
      Row("c", "v") :: Row("d", "vv") :: Nil)
  }

  test("SC-12232: delete rows with null values using EqualNullSafe") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // when value is null, this expression evaluates to true
    checkDelete(Some("value <=> null"),
      Row("c", "v") :: Row("d", "vv") :: Nil)
  }

  test("do not support subquery test") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("c", "d").createOrReplaceTempView("source")

    // basic subquery
    val e0 = intercept[AnalysisException] {
      executeDelete(target = tableSQLIdentifier, "key < (SELECT max(c) FROM source)")
    }.getMessage
    assert(e0.contains("Subqueries are not supported"))

    // subquery with EXISTS
    val e1 = intercept[AnalysisException] {
      executeDelete(target = tableSQLIdentifier, "EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e1.contains("Subqueries are not supported"))

    // subquery with NOT EXISTS
    val e2 = intercept[AnalysisException] {
      executeDelete(target = tableSQLIdentifier, "NOT EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e2.contains("Subqueries are not supported"))

    // subquery with IN
    val e3 = intercept[AnalysisException] {
      executeDelete(target = tableSQLIdentifier, "key IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e3.contains("Subqueries are not supported"))

    // subquery with NOT IN
    val e4 = intercept[AnalysisException] {
      executeDelete(target = tableSQLIdentifier, "key NOT IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e4.contains("Subqueries are not supported"))
  }

  test("schema pruning on data condition") {
    val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
    append(input, Nil)
    // Start from a cached snapshot state
    deltaLog.update().stateDF

    val executedPlans = DeltaTestUtils.withPhysicalPlansCaptured(spark) {
      checkDelete(Some("key = 2"),
        Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    }

    val scans = executedPlans.flatMap(_.collect {
      case f: FileSourceScanExec => f
    })

    // The first scan is for finding files to delete. We only are matching against the key
    // so that should be the only field in the schema
    assert(scans.head.schema.findNestedField(Seq("key")).nonEmpty)
    assert(scans.head.schema.findNestedField(Seq("value")).isEmpty)
  }


  test("nested schema pruning on data condition") {
    val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
      .select(struct("key", "value").alias("nested"))
    append(input, Nil)
    // Start from a cached snapshot state
    deltaLog.update().stateDF

    val executedPlans = DeltaTestUtils.withPhysicalPlansCaptured(spark) {
      checkDelete(Some("nested.key = 2"),
        Row(Row(1, 4)) :: Row(Row(1, 1)) :: Row(Row(0, 3)) :: Nil)
    }

    val scans = executedPlans.flatMap(_.collect {
      case f: FileSourceScanExec => f
    })

    assert(scans.head.schema == StructType.fromDDL("nested STRUCT<key: int>"))
  }

  /**
   * @param function the unsupported function.
   * @param functionType The type of the unsupported expression to be tested.
   * @param data the data in the table.
   * @param where the where clause containing the unsupported expression.
   * @param expectException whether an exception is expected to be thrown
   * @param customErrorRegex customized error regex.
   */
  private def testUnsupportedExpression(
      function: String,
      functionType: String,
      data: => DataFrame,
      where: String,
      expectException: Boolean,
      customErrorRegex: Option[String] = None) {
    test(s"$functionType functions in delete - expect exception: $expectException") {
      withTable("deltaTable") {
        data.write.format("delta").saveAsTable("deltaTable")

        val expectedErrorRegex = "(?s).*(?i)unsupported.*(?i).*Invalid expressions.*"

        var catchException = true

        var errorRegex = if (functionType.equals("Generate")) {
          ".*Subqueries are not supported in the DELETE.*"
        } else customErrorRegex.getOrElse(expectedErrorRegex)


        if (catchException) {
          val dataBeforeException = spark.read.format("delta").table("deltaTable").collect()
          val e = intercept[Exception] {
            executeDelete(target = "deltaTable", where = where)
          }
          val message = if (e.getCause != null) {
            e.getCause.getMessage
          } else e.getMessage
          assert(message.matches(errorRegex))
          checkAnswer(spark.read.format("delta").table("deltaTable"), dataBeforeException)
        } else {
          executeDelete(target = "deltaTable", where = where)
        }
      }
    }
  }

  testUnsupportedExpression(
    function = "row_number",
    functionType = "Window",
    data = Seq((2, 2), (1, 4)).toDF("key", "value"),
    where = "row_number() over (order by value) > 1",
    expectException = true
  )

  testUnsupportedExpression(
    function = "max",
    functionType = "Aggregate",
    data = Seq((2, 2), (1, 4)).toDF("key", "value"),
    where = "key > max(value)",
    expectException = true
  )

  // Explode functions are supported in where if only one row generated.
  testUnsupportedExpression(
    function = "explode",
    functionType = "Generate",
    data = Seq((2, List(2))).toDF("key", "value"),
    where = "key = (select explode(value) from deltaTable)",
    expectException = false // generate only one row, no exception.
  )

  // Explode functions are supported in where but if there's more than one row generated,
  // it will throw an exception.
  testUnsupportedExpression(
    function = "explode",
    functionType = "Generate",
    data = Seq((2, List(2)), (1, List(4, 5))).toDF("key", "value"),
    where = "key = (select explode(value) from deltaTable)",
    expectException = true, // generate more than one row. Exception expected.
    customErrorRegex =
      Some(".*More than one row returned by a subquery used as an expression(?s).*")
  )

  testSparkMasterOnly("Variant type") {
    val dstDf = sql(
      """SELECT parse_json(cast(id as string)) v, id i
      FROM range(3)""")
    append(dstDf)

    executeDelete(target = tableSQLIdentifier, where = "to_json(v) = '1'")

    checkAnswer(readDeltaTableByIdentifier().selectExpr("i", "to_json(v)"),
      Seq(Row(0, "0"), Row(2, "2")))
  }

  test("delete on partitioned table with special chars") {
    val partValue = "part%one"
    append(
      spark.range(0, 3, 1, 1).toDF("key").withColumn("value", lit(partValue)),
      partitionBy = Seq("value"))
    checkDelete(
      condition = Some(s"value = '$partValue' and key = 1"),
      expectedResults = Row(0, partValue) :: Row(2, partValue) :: Nil)
    checkDelete(
      condition = Some(s"value = '$partValue' and key = 2"),
      expectedResults = Row(0, partValue) :: Nil)
    checkDelete(
      condition = Some(s"value = '$partValue'"),
      expectedResults = Nil)
  }
}
