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

package org.apache.spark.sql.delta.actions

import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import com.fasterxml.jackson.annotation.JsonIgnore

/**
 * Trait to be mixed into the [[Protocol]] case class to enable Table Features.
 *
 * Protocol reader version 3 and writer version 7 start to support reader and writer table
 * features. Reader version 3 supports only reader-writer features in an <b>explicit</b> way,
 * by adding its name to `readerFeatures`. Similarly, writer version 7 supports only writer-only
 * or reader-writer features in an <b>explicit</b> way, by adding its name to `writerFeatures`.
 * When reading or writing a table, clients MUST respect all supported features.
 *
 * See also the document of [[TableFeature]] for feature-specific terminologies.
 */
trait TableFeatureSupport { this: Protocol =>

  /** Check if this protocol is capable of adding features into its `readerFeatures` field. */
  def supportsReaderFeatures: Boolean =
    TableFeatureProtocolUtils.supportsReaderFeatures(minReaderVersion)

  /** Check if this protocol is capable of adding features into its `writerFeatures` field. */
  def supportsWriterFeatures: Boolean =
    TableFeatureProtocolUtils.supportsWriterFeatures(minWriterVersion)

  /**
   * Get a new Protocol object that has `feature` supported. Writer-only features will be added to
   * `writerFeatures` field, and reader-writer features will be added to `readerFeatures` and
   * `writerFeatures` fields.
   *
   * If `feature` is already implicitly supported in the current protocol's legacy reader or
   * writer protocol version, the new protocol will not modify the original protocol version,
   * i.e., the feature will not be explicitly added to the protocol's `readerFeatures` or
   * `writerFeatures`. This is to avoid unnecessary protocol upgrade for feature that it already
   * supports.
   */
  def withFeature(feature: TableFeature): Protocol = {
    def shouldAddRead: Boolean = {
      if (supportsReaderFeatures) return true
      if (feature.minReaderVersion <= minReaderVersion) return false

      throw DeltaErrors.tableFeatureRequiresHigherReaderProtocolVersion(
        feature.name,
        minReaderVersion,
        feature.minReaderVersion)
    }

    def shouldAddWrite: Boolean = {
      if (supportsWriterFeatures) return true
      if (feature.minWriterVersion <= minWriterVersion) return false

      throw DeltaErrors.tableFeatureRequiresHigherWriterProtocolVersion(
        feature.name,
        minWriterVersion,
        feature.minWriterVersion)
    }

    var shouldAddToReaderFeatures = feature.isReaderWriterFeature
    var shouldAddToWriterFeatures = true
    if (feature.isLegacyFeature) {
      if (feature.isReaderWriterFeature) {
        shouldAddToReaderFeatures = shouldAddRead
      }
      shouldAddToWriterFeatures = shouldAddWrite
    }

    val protocolWithDependencies = withFeatures(feature.requiredFeatures)
    protocolWithDependencies.withFeature(
      feature.name,
      addToReaderFeatures = shouldAddToReaderFeatures,
      addToWriterFeatures = shouldAddToWriterFeatures)
  }

  /**
   * Get a new Protocol object with multiple features supported.
   *
   * See the documentation of [[withFeature]] for more information.
   */
  def withFeatures(features: Iterable[TableFeature]): Protocol = {
    features.foldLeft(this)(_.withFeature(_))
  }

  /**
   * Get a new Protocol object with an additional feature descriptor. If `addToReaderFeatures` is
   * set to `true`, the descriptor will be added to the protocol's `readerFeatures` field. If
   * `addToWriterFeatures` is set to `true`, the descriptor will be added to the protocol's
   * `writerFeatures` field.
   *
   * The method does not require the feature to be recognized by the client, therefore will not
   * try keeping the protocol's `readerFeatures` and `writerFeatures` in sync. Use with caution.
   */
  private[actions] def withFeature(
      name: String,
      addToReaderFeatures: Boolean,
      addToWriterFeatures: Boolean): Protocol = {
    if (addToReaderFeatures && !supportsReaderFeatures) {
      throw DeltaErrors.tableFeatureRequiresHigherReaderProtocolVersion(
        name,
        currentVersion = minReaderVersion,
        requiredVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION)
    }
    if (addToWriterFeatures && !supportsWriterFeatures) {
      throw DeltaErrors.tableFeatureRequiresHigherWriterProtocolVersion(
        name,
        currentVersion = minWriterVersion,
        requiredVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
    }

    val addedReaderFeatureOpt = if (addToReaderFeatures) Some(name) else None
    val addedWriterFeatureOpt = if (addToWriterFeatures) Some(name) else None

    copy(
      readerFeatures = this.readerFeatures.map(_ ++ addedReaderFeatureOpt),
      writerFeatures = this.writerFeatures.map(_ ++ addedWriterFeatureOpt))
  }

  /**
   * Get a new Protocol object with additional feature descriptors added to the protocol's
   * `readerFeatures` field.
   *
   * The method does not require the features to be recognized by the client, therefore will not
   * try keeping the protocol's `readerFeatures` and `writerFeatures` in sync. Use with caution.
   */
  private[delta] def withReaderFeatures(names: Iterable[String]): Protocol = {
    names.foldLeft(this)(
      _.withFeature(_, addToReaderFeatures = true, addToWriterFeatures = false))
  }

  /**
   * Get a new Protocol object with additional feature descriptors added to the protocol's
   * `writerFeatures` field.
   *
   * The method does not require the features to be recognized by the client, therefore will not
   * try keeping the protocol's `readerFeatures` and `writerFeatures` in sync. Use with caution.
   */
  private[delta] def withWriterFeatures(names: Iterable[String]): Protocol = {
    names.foldLeft(this)(
      _.withFeature(_, addToReaderFeatures = false, addToWriterFeatures = true))
  }

  /**
   * Get all feature names in this protocol's `readerFeatures` field. Returns an empty set when
   * this protocol does not support reader features.
   */
  def readerFeatureNames: Set[String] = this.readerFeatures.getOrElse(Set())

  /**
   * Get a set of all feature names in this protocol's `writerFeatures` field. Returns an empty
   * set when this protocol does not support writer features.
   */
  def writerFeatureNames: Set[String] = this.writerFeatures.getOrElse(Set())

  /**
   * Get a set of all feature names in this protocol's `readerFeatures` and `writerFeatures`
   * field. Returns an empty set when this protocol supports none of reader and writer features.
   */
  @JsonIgnore
  lazy val readerAndWriterFeatureNames: Set[String] = readerFeatureNames ++ writerFeatureNames

  /**
   * Same as above but returns a sequence of [[TableFeature]] instead of a set of feature names.
   */
  @JsonIgnore
  lazy val readerAndWriterFeatures: Seq[TableFeature] =
    readerAndWriterFeatureNames.toSeq.flatMap(TableFeature.featureNameToFeature)

  /**
   * A sequence of native [[TableFeature]]s. This is derived by filtering out all explicitly
   * supported legacy features.
   */
  @JsonIgnore
  lazy val nativeReaderAndWriterFeatures: Seq[TableFeature] =
    readerAndWriterFeatures.filterNot(_.isLegacyFeature)

  /**
   * Get all features that are implicitly supported by this protocol, for example, `Protocol(1,2)`
   * implicitly supports `appendOnly` and `invariants`. When this protocol is capable of requiring
   * writer features, no feature can be implicitly supported.
   */
  @JsonIgnore
  lazy val implicitlySupportedFeatures: Set[TableFeature] = {
    if (supportsReaderFeatures && supportsWriterFeatures) {
      // this protocol uses both reader and writer features, no feature can be implicitly supported
      Set()
    } else {
      TableFeature.allSupportedFeaturesMap.values
        .filter(_.isLegacyFeature)
        .filterNot(supportsReaderFeatures || this.minReaderVersion < _.minReaderVersion)
        .filterNot(supportsWriterFeatures || this.minWriterVersion < _.minWriterVersion)
        .toSet
    }
  }

  /**
   * Get all features that are supported by this protocol, implicitly and explicitly. When the
   * protocol supports table features, this method returns the same set of features as
   * [[readerAndWriterFeatureNames]]; when the protocol does not support table features, this
   * method becomes equivalent to [[implicitlySupportedFeatures]].
   */
  @JsonIgnore
  lazy val implicitlyAndExplicitlySupportedFeatures: Set[TableFeature] = {
    readerAndWriterFeatureNames.flatMap(TableFeature.featureNameToFeature) ++
      implicitlySupportedFeatures
  }

  /**
   * Determine whether this protocol can be safely upgraded to a new protocol `to`. This means:
   *   - all features supported by this protocol are supported by `to`.
   *
   * Examples regarding feature status:
   *   - from `[appendOnly]` to `[appendOnly]` => allowed.
   *   - from `[appendOnly, changeDataFeed]` to `[appendOnly]` => not allowed.
   *   - from `[appendOnly]` to `[appendOnly, changeDataFeed]` => allowed.
   */
  def canUpgradeTo(to: Protocol): Boolean =
    // All features supported by `this` are supported by `to`.
    implicitlyAndExplicitlySupportedFeatures.subsetOf(to.implicitlyAndExplicitlySupportedFeatures)

  /**
   * Determine whether this protocol can be safely downgraded to a new protocol `to`.
   * All the implicit and explicit features between the two protocols need to match,
   * excluding the dropped feature. We also need to take into account that in some cases
   * the downgrade process may add the CheckpointProtectionTableFeature.
   *
   * Note, the conditions above also account for cases where we downgrade from table features
   * to legacy protocol versions.
   */
  def canDowngradeTo(to: Protocol, droppedFeatureName: String): Boolean = {
    val thisFeatures = this.implicitlyAndExplicitlySupportedFeatures
    val toFeatures = to.implicitlyAndExplicitlySupportedFeatures
    val allowedNewFeatures: Set[TableFeature] = Set(CheckpointProtectionTableFeature)
    val droppedFeature = Seq(droppedFeatureName).flatMap(TableFeature.featureNameToFeature)
    val newFeatures = toFeatures -- thisFeatures
    newFeatures.subsetOf(allowedNewFeatures) &&
      (thisFeatures -- droppedFeature == toFeatures -- newFeatures)
  }

  /**
   * True if this protocol can be upgraded or downgraded to the 'to' protocol.
   */
  def canTransitionTo(to: Protocol, op: Operation): Boolean = {
    op match {
      case drop: DeltaOperations.DropTableFeature => canDowngradeTo(to, drop.featureName)
      case _ => canUpgradeTo(to)
    }
  }

  /**
   * Merge this protocol with multiple `protocols` to have the highest reader and writer versions
   * plus all explicitly and implicitly supported features.
   */
  def merge(others: Protocol*): Protocol = {
    val protocols = this +: others
    val mergedReaderVersion = protocols.map(_.minReaderVersion).max
    val mergedWriterVersion = protocols.map(_.minWriterVersion).max
    val mergedReaderFeatures = protocols.flatMap(_.readerFeatureNames)
    val mergedWriterFeatures = protocols.flatMap(_.writerFeatureNames)
    val mergedImplicitFeatures = protocols.flatMap(_.implicitlySupportedFeatures)

    val mergedProtocol = Protocol(mergedReaderVersion, mergedWriterVersion)
      .withReaderFeatures(mergedReaderFeatures)
      .withWriterFeatures(mergedWriterFeatures)
      .withFeatures(mergedImplicitFeatures)

    // The merged protocol is always normalized in order to represent the protocol
    // with the weakest possible form. This enables backward compatibility.
    // This is preceded by a denormalization step. This allows to fix invalid legacy Protocols.
    // For example, (2, 3) is normalized to (1, 3). This is because there is no legacy feature
    // in the set with reader version 2 unless the writer version is at least 5.
    mergedProtocol.denormalizedNormalized
  }

  /**
   * Remove writer feature from protocol. To remove a writer feature we only need to
   * remove it from the writerFeatures set.
   */
  private[delta] def removeWriterFeature(targetWriterFeature: TableFeature): Protocol = {
    require(targetWriterFeature.isRemovable)
    require(!targetWriterFeature.isReaderWriterFeature)
    copy(writerFeatures = writerFeatures.map(_ - targetWriterFeature.name))
  }

  /**
   * Remove reader+writer feature from protocol. To remove a reader+writer feature we need to
   * remove it from the readerFeatures set and the writerFeatures set.
   */
  private[delta] def removeReaderWriterFeature(
      targetReaderWriterFeature: TableFeature): Protocol = {
    require(targetReaderWriterFeature.isRemovable)
    require(targetReaderWriterFeature.isReaderWriterFeature)
    val newReaderFeatures = readerFeatures.map(_ - targetReaderWriterFeature.name)
    val newWriterFeatures = writerFeatures.map(_ - targetReaderWriterFeature.name)
    copy(readerFeatures = newReaderFeatures, writerFeatures = newWriterFeatures)
  }

  /**
   * Remove feature wrapper for removing either Reader/Writer or Writer features. We assume
   * the feature exists in the protocol. There is a relevant validation at
   * [[AlterTableDropFeatureDeltaCommand]]. We also require targetFeature is removable.
   *
   * After removing the feature we normalize the protocol.
   */
  def removeFeature(targetFeature: TableFeature): Protocol = {
    require(targetFeature.isRemovable)
    val currentProtocol = this.denormalized
    val newProtocol = targetFeature match {
      case f@(_: ReaderWriterFeature | _: LegacyReaderWriterFeature) =>
        currentProtocol.removeReaderWriterFeature(f)
      case f@(_: WriterFeature | _: LegacyWriterFeature) =>
        currentProtocol.removeWriterFeature(f)
      case f =>
        throw DeltaErrors.dropTableFeatureNonRemovableFeature(f.name)
    }
    newProtocol.normalized
  }


  /**
   * Protocol normalization is the process of converting a table features protocol to the weakest
   * possible form. This primarily refers to converting a table features protocol to a legacy
   * protocol. A Table Features protocol can be represented with the legacy representation only
   * when the features set of the former exactly matches a legacy protocol.
   *
   * Normalization can also decrease the reader version of a table features protocol when it is
   * higher than necessary.
   *
   * For example:
   * (1, 7, AppendOnly, Invariants, CheckConstraints) -> (1, 3)
   * (3, 7, RowTracking) -> (1, 7, RowTracking)
   */
  def normalized: Protocol = {
    // Normalization can only be applied to table feature protocols.
    if (!supportsWriterFeatures) return this

    val (minReaderVersion, minWriterVersion) =
      TableFeatureProtocolUtils.minimumRequiredVersions(readerAndWriterFeatures)
    val newProtocol = Protocol(minReaderVersion, minWriterVersion)

    if (this.implicitlyAndExplicitlySupportedFeatures ==
      newProtocol.implicitlyAndExplicitlySupportedFeatures) {
      newProtocol
    } else {
      Protocol(minReaderVersion, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(readerAndWriterFeatures)
    }
  }

  /**
   * Protocol denormalization is the process of converting a legacy protocol to the
   * the equivalent table features protocol. This is the inverse of protocol normalization.
   * It can be used to allow operations on legacy protocols that yield result which
   * cannot be represented anymore by a legacy protocol.
   */
  def denormalized: Protocol = {
    // Denormalization can only be applied to legacy protocols.
    if (supportsWriterFeatures) return this

    val (minReaderVersion, _) =
      TableFeatureProtocolUtils.minimumRequiredVersions(implicitlySupportedFeatures.toSeq)

    Protocol(minReaderVersion, TABLE_FEATURES_MIN_WRITER_VERSION)
      .withFeatures(implicitlySupportedFeatures)
  }

  /**
   * Helper method that applies both denormalization and normalization. This can be used to
   * normalize invalid legacy protocols such as (2, 3), (1, 5). A legacy protocol is invalid
   * when the version numbers are higher than required to support the implied feature set.
   */
  def denormalizedNormalized: Protocol = denormalized.normalized

  /**
   * Check if a `feature` is supported by this protocol. This means either (a) the protocol does
   * not support table features and implicitly supports the feature, or (b) the protocol supports
   * table features and references the feature.
   */
  def isFeatureSupported(feature: TableFeature): Boolean = {
    // legacy feature + legacy protocol
    (feature.isLegacyFeature && this.implicitlySupportedFeatures.contains(feature)) ||
      // new protocol
      readerAndWriterFeatureNames.contains(feature.name)
  }

  /** Returns whether this client supports writing in a table with this protocol. */
  def supportedForWrite(): Boolean = {
    val supportedWriterVersions = Action.supportedWriterVersionNumbers
    val supportedWriterFeatures = Action.supportedProtocolVersion().writerFeatureNames
    val testUnsupportedFeatures: Set[String] = TableFeature.testUnsupportedFeatures
      .filterNot(_.isReaderWriterFeature)
      .map(_.name)

    supportedWriterVersions.contains(this.minWriterVersion) &&
      this.writerFeatureNames.subsetOf(supportedWriterFeatures -- testUnsupportedFeatures)
  }

  /** Returns whether this client supports reading a table with this protocol. */
  def supportedForRead(): Boolean = {
    val supportedReaderVersions = Action.supportedReaderVersionNumbers
    val supportedReaderFeatures = Action.supportedProtocolVersion().readerFeatureNames
    val testUnsupportedFeatures: Set[String] = TableFeature.testUnsupportedFeatures
      .filter(_.isReaderWriterFeature).map(_.name)

    supportedReaderVersions.contains(this.minReaderVersion) &&
      this.readerFeatureNames.subsetOf(supportedReaderFeatures -- testUnsupportedFeatures)
  }
}

object TableFeatureProtocolUtils {

  /** Prop prefix in table properties. */
  val FEATURE_PROP_PREFIX = "delta.feature."

  /** Prop prefix in Spark sessions configs. */
  val DEFAULT_FEATURE_PROP_PREFIX = "spark.databricks.delta.properties.defaults.feature."

  /**
   * The string constant "enabled" for uses in table properties.
   * @deprecated
   *   This value is deprecated to avoid confusion with features that are actually enabled by
   *   table metadata. Use [[FEATURE_PROP_SUPPORTED]] instead.
   */
  val FEATURE_PROP_ENABLED = "enabled"

  /** The string constant "supported" for uses in table properties. */
  val FEATURE_PROP_SUPPORTED = "supported"

  /** Min reader version that supports reader features. */
  val TABLE_FEATURES_MIN_READER_VERSION = 3

  /** Min reader version that supports writer features. */
  val TABLE_FEATURES_MIN_WRITER_VERSION = 7

  /** Get the table property config key for the `feature`. */
  def propertyKey(feature: TableFeature): String = propertyKey(feature.name)

  /** Get the table property config key for the `featureName`. */
  def propertyKey(featureName: String): String =
    s"$FEATURE_PROP_PREFIX$featureName"

  /** Get the session default config key for the `feature`. */
  def defaultPropertyKey(feature: TableFeature): String = defaultPropertyKey(feature.name)

  /** Get the session default config key for the `featureName`. */
  def defaultPropertyKey(featureName: String): String =
    s"$DEFAULT_FEATURE_PROP_PREFIX$featureName"

  /**
   * Determine whether a [[Protocol]] with the given reader protocol version is capable of adding
   * features into its `readerFeatures` field.
   */
  def supportsReaderFeatures(readerVersion: Int): Boolean = {
    readerVersion >= TABLE_FEATURES_MIN_READER_VERSION
  }

  /**
   * Determine whether a [[Protocol]] with the given writer protocol version is capable of adding
   * features into its `writerFeatures` field.
   */
  def supportsWriterFeatures(writerVersion: Int): Boolean = {
    writerVersion >= TABLE_FEATURES_MIN_WRITER_VERSION
  }

  /**
   * Get a set of [[TableFeature]]s representing supported features set in a table properties map.
   */
  def getSupportedFeaturesFromTableConfigs(configs: Map[String, String]): Set[TableFeature] = {
    val featureConfigs = configs.filterKeys(_.startsWith(FEATURE_PROP_PREFIX))
    val unsupportedFeatureConfigs = mutable.Set.empty[String]
    val collectedFeatures = featureConfigs.flatMap { case (key, value) =>
      // Feature name is lower cased in table properties but not in Spark session configs.
      // Feature status is not lower cased in any case.
      val name = key.stripPrefix(FEATURE_PROP_PREFIX).toLowerCase(Locale.ROOT)
      val status = value.toLowerCase(Locale.ROOT)
      if (status != FEATURE_PROP_SUPPORTED && status != FEATURE_PROP_ENABLED) {
        throw DeltaErrors.unsupportedTableFeatureStatusException(name, status)
      }
      val featureOpt = TableFeature.featureNameToFeature(name)
      if (!featureOpt.isDefined) {
        unsupportedFeatureConfigs += key
      }
      featureOpt
    }.toSet
    if (unsupportedFeatureConfigs.nonEmpty) {
      throw DeltaErrors.unsupportedTableFeatureConfigsException(unsupportedFeatureConfigs)
    }
    collectedFeatures
  }

  /**
   * Checks if the the given table property key is a Table Protocol property, i.e.,
   * `delta.minReaderVersion`, `delta.minWriterVersion`, ``delta.ignoreProtocolDefaults``, or
   * anything that starts with `delta.feature.`
   */
  def isTableProtocolProperty(key: String): Boolean = {
    key == Protocol.MIN_READER_VERSION_PROP ||
    key == Protocol.MIN_WRITER_VERSION_PROP ||
    key == DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key ||
    key.startsWith(TableFeatureProtocolUtils.FEATURE_PROP_PREFIX)
  }

  /**
   * Returns the minimum reader/writer versions required to support all provided features.
   */
  def minimumRequiredVersions(features: Seq[TableFeature]): (Int, Int) =
    ((features.map(_.minReaderVersion) :+ 1).max, (features.map(_.minWriterVersion) :+ 1).max)
}

object DropTableFeatureUtils extends DeltaLogging {
  private val MAX_CHECKPOINT_RETRIES = 3

  // The number of barrier checkpoints to create before the version requiring checkpoint protection.
  val NUMBER_OF_BARRIER_CHECKPOINTS = 3

  /**
   * Helper function for creating checkpoints. If checkpoint creation fails we retry up
   * to [[MAX_CHECKPOINT_RETRIES]] times.
   *
   * @param snapshotRefreshStartTimeTs The timestamp to use as a starting point for refreshing
   *                                   the snapshot. This value is used to improve the performance
   *                                   of the snapshot refresh operation.
   */
  def createCheckpointWithRetries(
      table: DeltaTableV2,
      snapshotRefreshStartTimeTs: Long): Boolean = {
    val log = table.deltaLog
    val snapshot = log.update(checkIfUpdatedSinceTs = Some(snapshotRefreshStartTimeTs))

    def checkpointAndVerify(snapshot: Snapshot): Boolean = {
      try {
        log.checkpoint(snapshot)
        val upperBoundVersion = Some(CheckpointInstance(version = snapshot.version + 1))
        val lastVerifiedCheckpoint = log.findLastCompleteCheckpointBefore(upperBoundVersion)
        lastVerifiedCheckpoint.exists(_.version == snapshot.version)
      } catch {
        case NonFatal(e) =>
          recordDeltaEvent(
            deltaLog = log,
            opType = "dropFeature.checkpointAndVerify.error",
            data = Map(
              "message" -> e.getMessage,
              "stackTrace" -> e.getStackTrace().mkString("\n")))
          false
      }
    }

    (1 to MAX_CHECKPOINT_RETRIES).collectFirst {
      case _ if checkpointAndVerify(snapshot) => true
    }.getOrElse(false)
  }

  def createEmptyCommitAndCheckpoint(
      table: DeltaTableV2,
      snapshotRefreshStartTs: Long,
      retryOnFailure: Boolean = false): Boolean = {
    val log = table.deltaLog
    val snapshot = log.update(checkIfUpdatedSinceTs = Some(snapshotRefreshStartTs))
    val emptyCommitTS = table.deltaLog.clock.nanoTime()
    log.startTransaction(table.catalogTable, Some(snapshot))
      .commit(Nil, DeltaOperations.EmptyCommit)

    // retryOnFailure is temporary to avoid affecting the behavior of the legacy Drop Feature
    // command behavior.
    if (retryOnFailure) {
      createCheckpointWithRetries(table, emptyCommitTS)
    } else {
      log.checkpoint(log.update(checkIfUpdatedSinceTs = Some(emptyCommitTS)))
      true
    }
  }

  def truncateHistoryLogRetentionMillis(metadata: Metadata): Long = {
    val truncateHistoryLogRetention = DeltaConfigs
      .TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
      .fromMetaData(metadata)

    DeltaConfigs.getMilliSeconds(truncateHistoryLogRetention)
  }

  /**
   * Returns new metadata without `tablePropertiesToRemoveAtDowngradeCommit` table properties.
   */
  def getDowngradedProtocolMetadata(
      feature: RemovableFeature,
      metadata: Metadata): Metadata = {
    val propKeys = feature.tablePropertiesToRemoveAtDowngradeCommit
    val normalizedKeys = DeltaConfigs.normalizeConfigKeys(propKeys)
    val newConfiguration = metadata.configuration.filterNot {
      case (key, _) => normalizedKeys.contains(key)
    }
    metadata.copy(configuration = newConfiguration)
  }
}
