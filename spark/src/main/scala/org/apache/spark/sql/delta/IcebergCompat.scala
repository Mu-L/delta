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

import org.apache.spark.sql.delta.DeltaConfigs._
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Utils to validate the IcebergCompatV1 table feature, which is responsible for keeping Delta
 * tables in valid states (see the Delta spec for full invariants, dependencies, and requirements)
 * so that they are capable of having Delta to Iceberg metadata conversion applied to them. The
 * IcebergCompatV1 table feature does not implement, specify, or control the actual metadata
 * conversion; that is handled by the Delta UniForm feature.
 *
 * Note that UniForm (Iceberg) depends on IcebergCompatV1, but IcebergCompatV1 does not depend on or
 * require UniForm (Iceberg). It is perfectly valid for a Delta table to have IcebergCompatV1
 * enabled but UniForm (Iceberg) not enabled.
 */

object IcebergCompatV1 extends IcebergCompatBase(
  version = 1,
  icebergFormatVersion = 2,
  config = DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED,
  tableFeature = IcebergCompatV1TableFeature,
  requiredTableProperties = Seq(RequireColumnMapping),
  incompatibleTableFeatures = Set(DeletionVectorsTableFeature),
  checks = Seq(
    CheckOnlySingleVersionEnabled,
    CheckAddFileHasStats,
    CheckNoPartitionEvolution,
    CheckNoListMapNullType,
    CheckDeletionVectorDisabled,
    CheckTypeWideningSupported
  )
)

object IcebergCompatV2 extends IcebergCompatBase(
  version = 2,
  icebergFormatVersion = 2,
  config = DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED,
  tableFeature = IcebergCompatV2TableFeature,
  requiredTableProperties = Seq(RequireColumnMapping),
  incompatibleTableFeatures = Set(DeletionVectorsTableFeature),
  checks = Seq(
    CheckOnlySingleVersionEnabled,
    CheckAddFileHasStats,
    CheckTypeInV2AllowList,
    CheckPartitionDataTypeInV2AllowList,
    CheckNoPartitionEvolution,
    CheckDeletionVectorDisabled,
    CheckTypeWideningSupported
  )
)

/**
 * All IcebergCompatVx should extend from this base class
 *
 * @param version the compat version number
 * @param icebergFormatVersion iceberg format version written by this compat
 * @param config  the DeltaConfig for this IcebergCompat version
 * @param requiredTableFeatures a list of table features it relies on
 * @param requiredTableProperties a list of table properties it relies on.
 *                                See [[RequiredDeltaTableProperty]]
 * @param incompatibleTableFeatures a set of table features it is incompatible
 *                                  with. Used by [[IcebergCompat.isAnyIncompatibleEnabled]]
 * @param checks  a list of checks this IcebergCompatVx will perform.
 *                @see [[RequiredDeltaTableProperty]]
 */
case class IcebergCompatBase(
    version: Int,
    icebergFormatVersion: Int,
    config: DeltaConfig[Option[Boolean]],
    tableFeature: TableFeature,
    requiredTableProperties: Seq[RequiredDeltaTableProperty[_<:Any]],
    incompatibleTableFeatures: Set[TableFeature] = Set.empty,
    checks: Seq[IcebergCompatCheck]) extends DeltaLogging {
  def isEnabled(metadata: Metadata): Boolean = config.fromMetaData(metadata).getOrElse(false)

  /**
   * @return true if the feature should be auto enabled on the table created / updated with
   *         the schema
   */
  def shouldAutoEnable(schema: StructType, properties: Map[String, String]): Boolean = false
  /**
   * Expected to be called after the newest metadata and protocol have been ~ finalized.
   *
   * Furthermore, this should be called *after*
   * [[UniversalFormat.enforceIcebergInvariantsAndDependencies]].
   *
   * If you are enabling IcebergCompatV1 and are creating a new table, this method will
   * automatically upgrade the table protocol to support ColumnMapping and set it to 'name' mode,
   * too.
   *
   * If you are disabling IcebergCompatV1, this method will also disable Universal Format (Iceberg),
   * if it is enabled.
   *
   * @param actions The actions to be committed in the txn. We will only look at the [[AddFile]]s.
   *
   * @return tuple of options of (updatedProtocol, updatedMetadata). For either action, if no
   *         updates need to be applied, will return None.
   */
  def enforceInvariantsAndDependencies(
      spark: SparkSession,
      prevSnapshot: Snapshot,
      newestProtocol: Protocol,
      newestMetadata: Metadata,
      operation: Option[DeltaOperations.Operation],
      actions: Seq[Action]): (Option[Protocol], Option[Metadata]) = {
    val prevProtocol = prevSnapshot.protocol
    val prevMetadata = prevSnapshot.metadata
    val wasEnabled = this.isEnabled(prevMetadata)
    val isEnabled = this.isEnabled(newestMetadata)
    val tableId = newestMetadata.id

    val isCreatingOrReorgTable = UniversalFormat.isCreatingOrReorgTable(operation)

    (wasEnabled, isEnabled) match {
      case (_, false) => (None, None) // not enable or disabling, Ignore
      case (_, true) => // Enabling now or already-enabled
        val tblFeatureUpdates = scala.collection.mutable.Set.empty[TableFeature]
        val tblPropertyUpdates = scala.collection.mutable.Map.empty[String, String]

        // Check we have all required table features
        tableFeature.requiredFeatures.foreach { f =>
          (prevProtocol.isFeatureSupported(f), newestProtocol.isFeatureSupported(f)) match {
            case (_, true) => // all good
            case (false, false) => // txn has not supported it! auto-add the table feature
              tblFeatureUpdates += f
            case (true, false) => // txn is removing/un-supporting it!
              handleDisablingRequiredTableFeature(f)
          }
        }

        // Check we have all required delta table properties
        requiredTableProperties.foreach {
          case RequiredDeltaTableProperty(
              deltaConfig, validator, autoSetValue, autoEnableOnExistingTable) =>
            val newestValue = deltaConfig.fromMetaData(newestMetadata)
            val newestValueOkay = validator(newestValue)
            val newestValueExplicitlySet = newestMetadata.configuration.contains(deltaConfig.key)

            if (!newestValueOkay) {
              if (!newestValueExplicitlySet &&
                  (isCreatingOrReorgTable || autoEnableOnExistingTable)) {
                // This case covers both CREATE and REPLACE TABLE commands that
                // did not explicitly specify the required deltaConfig. In these
                // cases, we set the property automatically.
                // If autoEnableOnExistingTable = true, it auto sets in all cases
                tblPropertyUpdates += deltaConfig.key -> autoSetValue
              } else {
                // In all other cases, if the property value is not compatible
                // with the IcebergV1 requirements, we fail
                handleMissingRequiredTableProperties(
                  deltaConfig.key, newestValue.toString, autoSetValue)
              }
            }
        }

        // Update Protocol and Metadata if necessary
        val protocolResult = if (tblFeatureUpdates.nonEmpty) {
          logInfo(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, tableId)}] " +
            log"IcebergCompatV1 auto-supporting table features: " +
            log"${MDC(DeltaLogKeys.TABLE_FEATURES, tblFeatureUpdates.map(_.name))}")
          Some(newestProtocol.merge(tblFeatureUpdates.map(Protocol.forTableFeature).toSeq: _*))
        } else None

        val metadataResult = if (tblPropertyUpdates.nonEmpty) {
          logInfo(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, tableId)}] " +
            log"IcebergCompatV1 auto-setting table properties: " +
            log"${MDC(DeltaLogKeys.TBL_PROPERTIES, tblPropertyUpdates)}")
          val newConfiguration = newestMetadata.configuration ++ tblPropertyUpdates.toMap
          var tmpNewMetadata = newestMetadata.copy(configuration = newConfiguration)

          requiredTableProperties.foreach { tp =>
            tmpNewMetadata = tp.postProcess(prevMetadata, tmpNewMetadata, isCreatingOrReorgTable)
          }

          Some(tmpNewMetadata)
        } else None

        // Apply additional checks
        val context = IcebergCompatContext(
          spark,
          prevSnapshot,
          protocolResult.getOrElse(newestProtocol),
          metadataResult.getOrElse(newestMetadata),
          operation,
          actions,
          tableId,
          version
        )
        checks.foreach(_.apply(context))

        (protocolResult, metadataResult)
    }
  }

  protected def handleMissingTableFeature(feature: TableFeature): Unit =
    throw DeltaErrors.icebergCompatMissingRequiredTableFeatureException(version, feature)

  protected def handleDisablingRequiredTableFeature(feature: TableFeature): Unit =
    throw DeltaErrors.icebergCompatDisablingRequiredTableFeatureException(version, feature)

  protected def handleMissingRequiredTableProperties(
      confKey: String, actualVal: String, requiredVal: String): Unit =
    throw DeltaErrors.icebergCompatWrongRequiredTablePropertyException(
      version, confKey, actualVal, requiredVal)
}

/**
 * Util methods to manage between IcebergCompat versions
 */
case class IcebergCompatVersionBase(knownVersions: Set[IcebergCompatBase]) {
  /**
   * Fetch from Metadata the current enabled IcebergCompat version.
   * @return a number indicate the version. E.g., 1 for CompatV1.
   *         None if no version enabled.
   */
  def getEnabledVersion(metadata: Metadata): Option[Int] =
    knownVersions
      .find{ _.config.fromMetaData(metadata).getOrElse(false) }
      .map{ _.version }

  /**
   * Get the IcebergCompat by version. If version is not valid,
   * throw an exception.
   * @return the IcebergCompatVx object
   */
  def getForVersion(version: Int): IcebergCompatBase =
    knownVersions
      .find(_.version == version)
      .getOrElse(
        throw DeltaErrors.icebergCompatVersionNotSupportedException(
          version, knownVersions.size
        )
    )

  /**
   * @return any enabled IcebergCompat in the conf
   */
  def anyEnabled(conf: Map[String, String]): Option[IcebergCompatBase] =
    knownVersions.find { compat =>
      conf.getOrElse[String](compat.config.key, "false").toBoolean
    }

  def anyEnabled(metadata: Metadata): Option[IcebergCompatBase] =
    knownVersions.find { _.config.fromMetaData(metadata).getOrElse(false) }

  /**
   * @return true if any version of IcebergCompat is enabled
   */
  def isAnyEnabled(conf: Map[String, String]): Boolean = anyEnabled(conf).nonEmpty

  def isAnyEnabled(metadata: Metadata): Boolean =
    knownVersions.exists { _.config.fromMetaData(metadata).getOrElse(false) }

  /**
   * @return true if a CompatVx greater or eq to the required version is enabled
   */
  def isGeqEnabled(metadata: Metadata, requiredVersion: Int): Boolean =
    anyEnabled(metadata).exists(_.version >= requiredVersion)
  /**
   * @return true if any version of IcebergCompat is enabled, and is incompatible
   *         with the given table feature
   */
  def isAnyIncompatibleEnabled(
      configuration: Map[String, String], feature: TableFeature): Boolean =
    knownVersions.exists { compat =>
      configuration.getOrElse[String](compat.config.key, "false").toBoolean &&
        compat.incompatibleTableFeatures.contains(feature)
    }
}

object IcebergCompat extends IcebergCompatVersionBase(
    Set(IcebergCompatV1, IcebergCompatV2)
  ) with DeltaLogging



/**
 * Wrapper class for table property validation
 *
 * @param deltaConfig [[DeltaConfig]] we are checking
 * @param validator A generic method to validate the given value
 * @param autoSetValue The value to set if we can auto-set this value
 * @param autoEnableOnExistingTable this can be true only when the feature
 *                                  can be confidently enabled on existing table
 */
case class RequiredDeltaTableProperty[T](
      deltaConfig: DeltaConfig[T],
      validator: T => Boolean,
      autoSetValue: String,
      autoEnableOnExistingTable: Boolean = false) {
  /**
   * A callback after all required properties are added to the new metadata.
   * @return Updated metadata. None if no change
   */
  def postProcess(
      prevMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean) : Metadata = newMetadata
}

class RequireColumnMapping(allowedModes: Seq[DeltaColumnMappingMode])
  extends RequiredDeltaTableProperty(
    deltaConfig = DeltaConfigs.COLUMN_MAPPING_MODE,
    validator = (mode: DeltaColumnMappingMode) => allowedModes.contains(mode),
    autoSetValue = if (allowedModes.contains(NameMapping)) NameMapping.name else IdMapping.name) {

  override def postProcess(
      prevMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {
    if (!prevMetadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_MODE.key) &&
        newMetadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_MODE.key)) {
      assert(isCreatingNewTable, "we only auto-upgrade Column Mapping on new tables")
      val tmpNewMetadata = DeltaColumnMapping.assignColumnIdAndPhysicalName(
        newMetadata = newMetadata,
        oldMetadata = prevMetadata,
        isChangingModeOnExistingTable = false,
        isOverwritingSchema = false
      )
      DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(tmpNewMetadata)
      tmpNewMetadata
    } else {
      newMetadata
    }
  }
}

object RequireColumnMapping extends RequireColumnMapping(Seq(NameMapping, IdMapping))


case class IcebergCompatContext(
    spark: SparkSession,
    prevSnapshot: Snapshot,
    newestProtocol: Protocol,
    newestMetadata: Metadata,
    operation: Option[DeltaOperations.Operation],
    actions: Seq[Action],
    tableId: String,
    version: Integer) {
  def prevMetadata: Metadata = prevSnapshot.metadata

  def prevProtocol: Protocol = prevSnapshot.protocol
}

trait IcebergCompatCheck extends (IcebergCompatContext => Unit)

/**
 * Checks that ensures no more than one IcebergCompatVx is enabled.
 */
object CheckOnlySingleVersionEnabled extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    val numEnabled = IcebergCompat.knownVersions.toSeq
      .map { compat =>
        if (compat.isEnabled(context.newestMetadata)) 1 else 0
      }.sum
    if (numEnabled > 1) {
      throw DeltaErrors.icebergCompatVersionMutualExclusive(context.version)
    }
  }
}

object CheckAddFileHasStats extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    // If this field is empty, then the AddFile is missing the `numRecords` statistic.
    context.actions.collect { case a: AddFile if a.numLogicalRecords.isEmpty =>
      throw new UnsupportedOperationException(s"[tableId=${context.tableId}] " +
        s"IcebergCompatV${context.version} requires all AddFiles to contain " +
        s"the numRecords statistic. AddFile ${a.path} is missing this statistic. " +
        s"Stats: ${a.stats}")
    }
  }
}

object CheckNoPartitionEvolution extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    // Note: Delta doesn't support partition evolution, but you can change the partitionColumns
    // by doing a REPLACE or DataFrame overwrite.
    //
    // Iceberg-Spark itself *doesn't* support the following cases
    // - CREATE TABLE partitioned by colA; REPLACE TABLE partitioned by colB
    // - CREATE TABLE partitioned by colA; REPLACE TABLE not partitioned
    //
    // While Iceberg-Spark *does* support
    // - CREATE TABLE not partitioned; REPLACE TABLE not partitioned
    // - CREATE TABLE not partitioned; REPLACE TABLE partitioned by colA
    // - CREATE TABLE partitioned by colA dataType1; REPLACE TABLE partitioned by colA dataType2
    if (context.prevMetadata.partitionColumns.nonEmpty &&
      context.prevMetadata.partitionColumns != context.newestMetadata.partitionColumns) {
      throw DeltaErrors.icebergCompatReplacePartitionedTableException(
        context.version,
        context.prevMetadata.partitionColumns,
        context.newestMetadata.partitionColumns)
    }
  }
}

object CheckNoListMapNullType extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    SchemaUtils.findAnyTypeRecursively(context.newestMetadata.schema) { f =>
      f.isInstanceOf[MapType] || f.isInstanceOf[ArrayType] || f.isInstanceOf[NullType]
    } match {
      case Some(unsupportedType) =>
        throw DeltaErrors.icebergCompatUnsupportedDataTypeException(
          context.version, unsupportedType, context.newestMetadata.schema)
      case _ =>
    }
  }
}

class CheckTypeInAllowList extends IcebergCompatCheck {
  def allowTypes: Set[Class[_]] = Set()

  override def apply(context: IcebergCompatContext): Unit = {
    SchemaUtils
      .findAnyTypeRecursively(context.newestMetadata.schema)(t => !allowTypes.contains(t.getClass))
    match {
      case Some(unsupportedType) =>
        throw DeltaErrors.icebergCompatUnsupportedDataTypeException(
          context.version, unsupportedType, context.newestMetadata.schema)
      case _ =>
    }
  }
}

object CheckTypeInV2AllowList extends CheckTypeInAllowList {
  override val allowTypes: Set[Class[_]] = Set[Class[_]] (
    ByteType.getClass, ShortType.getClass,
    IntegerType.getClass, LongType.getClass,
    FloatType.getClass, DoubleType.getClass, classOf[DecimalType],
    StringType.getClass, BinaryType.getClass,
    BooleanType.getClass,
    TimestampType.getClass, TimestampNTZType.getClass, DateType.getClass,
    classOf[ArrayType], classOf[MapType], classOf[StructType])
}


object CheckPartitionDataTypeInV2AllowList extends IcebergCompatCheck {
  private val allowedTypes = Set[Class[_]] (
    ByteType.getClass, ShortType.getClass, IntegerType.getClass, LongType.getClass,
    FloatType.getClass, DoubleType.getClass, DecimalType.getClass,
    StringType.getClass, BinaryType.getClass,
    BooleanType.getClass,
    TimestampType.getClass, TimestampNTZType.getClass, DateType.getClass
  )
  override def apply(context: IcebergCompatContext): Unit = {
    val partitionSchema = context.newestMetadata.partitionSchema
    partitionSchema.fields.find(field => !allowedTypes.contains(field.dataType.getClass))
    match {
      case Some(field) =>
        throw DeltaErrors.icebergCompatUnsupportedPartitionDataTypeException(
            context.version, field.dataType, partitionSchema)
       case _ =>
    }
  }
}

/**
 * Check if the deletion vector has been disabled by previous snapshot
 * or newest metadata and protocol depending on whether the operation
 * is REORG UPGRADE UNIFORM or not.
 */
object CheckDeletionVectorDisabled extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    if (context.newestProtocol.isFeatureSupported(DeletionVectorsTableFeature)) {
      // note: user will need to *separately* disable deletion vectors if this check fails,
      //       i.e., ALTER TABLE SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
      val isReorgUpgradeUniform = UniversalFormat.isReorgUpgradeUniform(context.operation)
      // for REORG UPGRADE UNIFORM, we only need to check whether DV
      // is enabled in the newest metadata and protocol, this conforms with
      // the semantics of REORG UPGRADE UNIFORM, which will automatically disable
      // DV and rewrite all the parquet files with DV removed as for now.
      if (isReorgUpgradeUniform) {
        if (DeletionVectorUtils.deletionVectorsWritable(
              protocol = context.newestProtocol,
              metadata = context.newestMetadata
        )) {
          throw DeltaErrors.icebergCompatDeletionVectorsShouldBeDisabledException(context.version)
        }
      } else {
        // for other commands, we need to check whether DV is disabled from the
        // previous snapshot, in case there are concurrent writers.
        // plus, we also need to check from the newest metadata and protocol,
        // in case we are creating a new uniform table with DV enabled.
        if (DeletionVectorUtils.deletionVectorsWritable(context.prevSnapshot) ||
          DeletionVectorUtils.deletionVectorsWritable(
            protocol = context.newestProtocol,
            metadata = context.newestMetadata
          )) {
          throw DeltaErrors.icebergCompatDeletionVectorsShouldBeDisabledException(context.version)
        }
      }
    }
  }
}

/**
 * Checks that the table didn't go through any type changes that Iceberg doesn't support. See
 * `TypeWidening.isTypeChangeSupportedByIceberg()` for supported type changes.
 * Note that this check covers both:
 * - When the table had an unsupported type change applied in the past and Uniform is being enabled.
 * - When Uniform is enabled and a new, unsupported type change is being applied.
 */
object CheckTypeWideningSupported extends IcebergCompatCheck {
  override def apply(context: IcebergCompatContext): Unit = {
    val skipCheck = context.spark.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_TYPE_WIDENING_ALLOW_UNSUPPORTED_ICEBERG_TYPE_CHANGES)

    if (skipCheck || !TypeWidening.isSupported(context.newestProtocol)) return

    TypeWideningMetadata.getAllTypeChanges(context.newestMetadata.schema).foreach {
      case (fieldPath, TypeChange(_, fromType: AtomicType, toType: AtomicType, _))
        // We ignore type changes that are not generally supported with type widening to reduce the
        // risk of this check misfiring. These are handled by `TypeWidening.assertTableReadable()`.
        // The error here only captures type changes that are supported in Delta but not Iceberg.
        if TypeWidening.isTypeChangeSupported(fromType, toType) &&
          !TypeWidening.isTypeChangeSupportedByIceberg(fromType, toType) =>
        throw DeltaErrors.icebergCompatUnsupportedTypeWideningException(
          context.version, fieldPath, fromType, toType)
      case _ => () // ignore
    }
  }
}
