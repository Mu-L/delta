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

package org.apache.spark.sql.delta.icebergShaded

import org.apache.spark.sql.delta.{DeltaConfig, DeltaConfigs, IcebergCompat, Snapshot}
import org.apache.spark.sql.delta.DeltaConfigs.{LOG_RETENTION, TOMBSTONE_RETENTION}
import org.apache.spark.sql.delta.metering.DeltaLogging
import shadedForDelta.org.apache.iceberg.{PartitionSpec, StructLike, TableProperties => IcebergTableProperties}

import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Utils for converting a Delta Table to Iceberg Table
 */
object DeltaToIcebergConvert
  extends DeltaLogging
  {

  object TableProperties {
    /**
     * We generate Iceberg Table properties from Delta table properties
     * using two methods.
     * 1. If a Delta property key starts with "delta.universalformat.config.iceberg"
     * we strip the prefix from the key and include the property pair.
     * Note the key is already normalized to lower case.
     * 2. We compute Iceberg properties from Delta using custom logic
     * This now includes
     * a) Iceberg format version
     * b) Iceberg snapshot retention
     */
    def apply(deltaProperties: Map[String, String]): Map[String, String] = {
      val prefix = DeltaConfigs.DELTA_UNIVERSAL_FORMAT_ICEBERG_CONFIG_PREFIX
      val copiedFromDelta =
        deltaProperties
          .filterKeys(_.startsWith(prefix))
          .map { case (key, value) => key.stripPrefix(prefix) -> value }
          .toSeq
          .toMap

      val computers = Seq(FormatVersionComputer, RetentionPeriodComputer)
      val computed: Map[String, String] = computers
        .map(_.apply(deltaProperties, copiedFromDelta))
        .reduce((a, b) => a ++ b)

      copiedFromDelta ++ computed
    }

    private trait IcebergPropertiesComputer {
      /**
       * Compute Iceberg properties from Delta properties.
       * @param deltaProperties Delta properties
       * @param provided User-provided Iceberg properties
       * @return computed Iceberg properties
       */
      def apply(
          deltaProperties: Map[String, String],
          provided: Map[String, String]): Map[String, String]
    }

    /**
     * Compute Iceberg FORMAT_VERSION from IcebergCompat
     */
    private object FormatVersionComputer extends IcebergPropertiesComputer {
      override def apply(deltaProperties: Map[String, String],
                           provided: Map[String, String]): Map[String, String] =
        IcebergCompat
          .anyEnabled(deltaProperties)
          .map(IcebergTableProperties.FORMAT_VERSION -> _.icebergFormatVersion.toString)
          .toMap
    }

    /**
     * Compute Iceberg MAX_SNAPSHOT_AGE_MS as the minimal of
     * Delta's LOG_RETENTION and TOMBSTONE_RETENTION.
     * If users explicitly provide a MAX_SNAPSHOT_AGE_MS, also ensure the provided
     * value is no larger than Delta's retention.
     */
    private object RetentionPeriodComputer extends IcebergPropertiesComputer {
      override def apply(deltaProperties: Map[String, String],
                           provided: Map[String, String]): Map[String, String] = {

        def getAsMilliSeconds(conf: DeltaConfig[CalendarInterval],
                              properties: Map[String, String],
                              useDefault: Boolean = false): Option[Long] =
          properties.get(conf.key)
            .orElse(if (useDefault) Some(conf.defaultValue) else None)
            .map(conf.fromString)
            .map(DeltaConfigs.getMilliSeconds)

        // Set Iceberg max snapshot age as minimal of Delta log retention and tombstone retention
        val deltaRetention = (
          getAsMilliSeconds(LOG_RETENTION, deltaProperties),
          getAsMilliSeconds(TOMBSTONE_RETENTION, deltaProperties)
        ) match {
          case (Some(a), Some(b)) => Some(a min b)
          case (a, b) => a orElse b
        }

        // If user provided max snapshot age, check that it is smaller than Delta's retention
        lazy val maxAllowedRetention =
          getAsMilliSeconds(LOG_RETENTION, deltaProperties, useDefault = true).get min
          getAsMilliSeconds(TOMBSTONE_RETENTION, deltaProperties, useDefault = true).get

        provided.get(IcebergTableProperties.MAX_SNAPSHOT_AGE_MS).foreach { providedRetention =>
          if (providedRetention.toLong > maxAllowedRetention) {
            throw new IllegalArgumentException(
              s"Uniform iceberg's ${IcebergTableProperties.MAX_SNAPSHOT_AGE_MS} should be set >= " +
                s" min of delta's ${LOG_RETENTION.key} and" +
                s" ${TOMBSTONE_RETENTION.key}." +
                s" Current delta retention min in MS: $maxAllowedRetention," +
                s" Proposed iceberg retention in Ms: $providedRetention")
          }
        }

        deltaRetention
          .filter(_ < IcebergTableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT)
          .map { IcebergTableProperties.MAX_SNAPSHOT_AGE_MS -> _.toString }
          .toMap
      }
    }
  }

  object Partition {

    private[delta] def convertPartitionValues(
        snapshot: Snapshot,
        partitionSpec: PartitionSpec,
        partitionValues: Map[String, String],
        logicalToPhysicalPartitionNames: Map[String, String]): StructLike = {
      val schema = snapshot.schema
      val ICEBERG_NULL_PARTITION_VALUE = "__HIVE_DEFAULT_PARTITION__"
      val partitionPath = partitionSpec.fields()
      val partitionVals = new Array[Any](partitionSpec.fields().size())
      val nameToDataTypes: Map[String, DataType] =
        schema.fields.map(f => f.name -> f.dataType).toMap
      for (i <- partitionVals.indices) {
        val logicalPartCol = partitionPath.get(i).name()
        val physicalPartKey = logicalToPhysicalPartitionNames(logicalPartCol)
        // ICEBERG_NULL_PARTITION_VALUE is referred in Iceberg lib to mark NULL partition value
        val partValue = Option(partitionValues.getOrElse(physicalPartKey, null))
          .getOrElse(ICEBERG_NULL_PARTITION_VALUE)
        val partitionColumnDataType = nameToDataTypes(logicalPartCol)
        val icebergPartitionValue =
          IcebergTransactionUtils.stringToIcebergPartitionValue(
            partitionColumnDataType, partValue, snapshot.version)
        partitionVals(i) = icebergPartitionValue
      }
      new IcebergTransactionUtils.Row(partitionVals)
    }
  }
}
