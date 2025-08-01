/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;
import static io.delta.kernel.internal.skipping.StatsSchemaHelper.getStatsSchema;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnCheckpointFileSchema;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnScanFileSchema;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Scan;
import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.metrics.ScanMetrics;
import io.delta.kernel.internal.metrics.ScanReportImpl;
import io.delta.kernel.internal.metrics.Timer;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.replay.PaginationContext;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.skipping.DataSkippingPredicate;
import io.delta.kernel.internal.skipping.DataSkippingUtils;
import io.delta.kernel.internal.util.*;
import io.delta.kernel.metrics.ScanReport;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

/** Implementation of {@link Scan} */
public class ScanImpl implements Scan {
  /**
   * Schema of the snapshot from the Delta log being scanned in this scan. It is a logical schema
   * with metadata properties to derive the physical schema.
   */
  private final StructType snapshotSchema;

  /** Schema that we actually want to read. */
  private final StructType readSchema;

  private final Protocol protocol;
  private final Metadata metadata;
  private final LogReplay logReplay;
  private final Path dataPath;
  private final Optional<Predicate> filter;
  private final Optional<Tuple2<Predicate, Predicate>> partitionAndDataFilters;
  private final Supplier<Map<String, StructField>> partitionColToStructFieldMap;
  private boolean accessedScanFiles;
  private final SnapshotReport snapshotReport;
  private final ScanMetrics scanMetrics = new ScanMetrics();

  public ScanImpl(
      StructType snapshotSchema,
      StructType readSchema,
      Protocol protocol,
      Metadata metadata,
      LogReplay logReplay,
      Optional<Predicate> filter,
      Path dataPath,
      SnapshotReport snapshotReport) {
    this.snapshotSchema = snapshotSchema;
    this.readSchema = readSchema;
    this.protocol = protocol;
    this.metadata = metadata;
    this.logReplay = logReplay;
    this.filter = filter;
    this.partitionAndDataFilters = splitFilters(filter);
    this.dataPath = dataPath;
    this.partitionColToStructFieldMap =
        () -> {
          Set<String> partitionColNames = metadata.getPartitionColNames();
          return metadata.getSchema().fields().stream()
              .filter(field -> partitionColNames.contains(field.getName().toLowerCase(Locale.ROOT)))
              .collect(toMap(field -> field.getName().toLowerCase(Locale.ROOT), identity()));
        };
    this.snapshotReport = snapshotReport;
  }

  /**
   * Get an iterator of data files in this version of scan that survived the predicate pruning.
   *
   * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
   */
  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    return getScanFiles(engine, false /* includeStats */);
  }

  /**
   * Get an iterator of data files in this version of scan that survived the predicate pruning.
   *
   * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
   */
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(
      Engine engine, boolean includeStats) {
    return getScanFiles(engine, includeStats, Optional.empty() /* paginationContextOpt */);
  }

  /**
   * Get an iterator of data files in this version of scan that survived the predicate pruning.
   *
   * <p>When {@code includeStats=true} the JSON file statistics are always read from the log and
   * included in the returned columnar batches which have schema {@link
   * InternalScanFileUtils#SCAN_FILE_SCHEMA_WITH_STATS}. When {@code includeStats=false} the JSON
   * file statistics may or may not be present in the returned columnar batches.
   *
   * @param engine the {@link Engine} instance to use
   * @param includeStats whether to read and include the JSON statistics
   * @param paginationContextOpt pagination context if present
   * @return the surviving scan files as {@link FilteredColumnarBatch}s
   */
  protected CloseableIterator<FilteredColumnarBatch> getScanFiles(
      Engine engine, boolean includeStats, Optional<PaginationContext> paginationContextOpt) {
    if (accessedScanFiles) {
      throw new IllegalStateException("Scan files are already fetched from this instance");
    }
    accessedScanFiles = true;

    // Generate data skipping filter and decide if we should read the stats column
    Optional<DataSkippingPredicate> dataSkippingFilter = getDataSkippingFilter();
    boolean hasDataSkippingFilter = dataSkippingFilter.isPresent();
    boolean shouldReadStats = hasDataSkippingFilter || includeStats;

    Timer.Timed planningDuration = scanMetrics.totalPlanningTimer.start();
    // ScanReportReporter stores the current context and can be invoked (in the future) with
    // `reportError` or `reportSuccess` to stop the planning duration timer and push a report to
    // the engine
    ScanReportReporter reportReporter =
        (exceptionOpt, isFullyConsumed) -> {
          planningDuration.stop();
          ScanReport scanReport =
              new ScanReportImpl(
                  dataPath.toString() /* tablePath */,
                  logReplay.getVersion() /* table version */,
                  snapshotSchema,
                  snapshotReport.getReportUUID(),
                  filter,
                  readSchema,
                  getPartitionsFilters() /* partitionPredicate */,
                  dataSkippingFilter.map(p -> p),
                  isFullyConsumed,
                  scanMetrics,
                  exceptionOpt);
          engine.getMetricsReporters().forEach(reporter -> reporter.report(scanReport));
        };

    try {
      // Get active AddFiles via log replay
      // If there is a partition predicate, construct a predicate to prune checkpoint files
      // while constructing the table state.
      CloseableIterator<FilteredColumnarBatch> scanFileIter =
          logReplay.getAddFilesAsColumnarBatches(
              engine,
              shouldReadStats,
              getPartitionsFilters()
                  .map(
                      predicate ->
                          rewritePartitionPredicateOnCheckpointFileSchema(
                              predicate, partitionColToStructFieldMap.get())),
              scanMetrics,
              paginationContextOpt);

      // Apply partition pruning
      scanFileIter = applyPartitionPruning(engine, scanFileIter);

      // Apply data skipping
      if (hasDataSkippingFilter) {
        // there was a usable data skipping filter --> apply data skipping
        scanFileIter = applyDataSkipping(engine, scanFileIter, dataSkippingFilter.get());
      }

      // TODO when !includeStats drop the stats column if present before returning
      return wrapWithMetricsReporting(scanFileIter, reportReporter);

    } catch (Exception e) {
      reportReporter.reportError(e);
      throw e;
    }
  }

  @Override
  public Row getScanState(Engine engine) {
    StructType physicalSchema = createPhysicalSchema();

    // Compute the physical data read schema (i.e., the columns to read from a Parquet data file).
    // The only difference to the physical schema is that we exclude partition columns. All other
    // logic (e.g., row tracking columns, row index for DVs) is already handled before.
    List<String> partitionColumns = VectorUtils.toJavaList(metadata.getPartitionColumns());
    StructType physicalDataReadSchema =
        PartitionUtils.physicalSchemaWithoutPartitionColumns(
            readSchema /* logical read schema */, physicalSchema, new HashSet<>(partitionColumns));

    return ScanStateRow.of(
        metadata,
        protocol,
        readSchema.toJson(),
        physicalSchema.toJson(),
        physicalDataReadSchema.toJson(),
        dataPath.toUri().toString());
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return getDataFilters();
  }

  /** Helper method to create a copy of a column that is marked as an internal column. */
  public static StructField createInternalColumn(StructField field) {
    FieldMetadata metadata =
        FieldMetadata.builder()
            .fromMetadata(field.getMetadata())
            .putBoolean(StructField.IS_INTERNAL_COLUMN_KEY, true)
            .build();
    return field.withNewMetadata(metadata);
  }

  /**
   * Transform the logical schema requested by the connector into a physical schema that is passed
   * to the engine's parquet reader.
   *
   * <p>The logical-to-physical conversion is reversed in {@link Scan#transformPhysicalData(Engine,
   * Row, Row, CloseableIterator)} when physical data batches returned by the parquet reader are
   * converted into logical data batches requested by the connector.
   *
   * <p>The logical-to-physical conversion follows these high-level steps:
   *
   * <ul>
   *   <li>Regular columns are converted based on the column mapping mode.
   *   <li>Metadata columns are converted to their physical counterparts if applicable.
   *   <li>Additional columns (such as the row index) are requested if necessary.
   * </ul>
   *
   * @return The physical schema to read data from the data files.
   */
  private StructType createPhysicalSchema() {
    ArrayList<StructField> physicalFields = new ArrayList<>();
    ColumnMapping.ColumnMappingMode mode =
        ColumnMapping.getColumnMappingMode(metadata.getConfiguration());

    for (StructField logicalField : readSchema.fields()) {
      physicalFields.addAll(convertField(logicalField, mode));
    }

    if (protocol.getReaderFeatures().contains("deletionVectors")
        && physicalFields.stream()
            .map(StructField::getName)
            .noneMatch(name -> name.equals(StructField.METADATA_ROW_INDEX_COLUMN_NAME))) {
      // If the row index column is not already present, add it to the physical read schema
      physicalFields.add(createInternalColumn(StructField.METADATA_ROW_INDEX_COLUMN));
    }

    return new StructType(physicalFields);
  }

  private List<StructField> convertField(
      StructField logicalField, ColumnMapping.ColumnMappingMode mode) {
    if (logicalField.isDataColumn()) {
      return Collections.singletonList(
          ColumnMapping.convertToPhysicalColumn(logicalField, snapshotSchema, mode));
    }

    if (RowTracking.isRowTrackingColumn(logicalField)) {
      return MaterializedRowTrackingColumn.convertToPhysicalColumn(
          logicalField, readSchema, metadata);
    }

    // As of now, metadata columns other than row tracking columns do not require any special
    // handling, so we can just add them to the physical schema as is.
    return Collections.singletonList(logicalField);
  }

  private Optional<Tuple2<Predicate, Predicate>> splitFilters(Optional<Predicate> filter) {
    return filter.map(
        predicate ->
            PartitionUtils.splitMetadataAndDataPredicates(
                predicate, metadata.getPartitionColNames()));
  }

  private Optional<Predicate> getDataFilters() {
    return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._2));
  }

  private Optional<Predicate> getPartitionsFilters() {
    return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._1));
  }

  /** Consider `ALWAYS_TRUE` as no predicate. */
  private Optional<Predicate> removeAlwaysTrue(Optional<Predicate> predicate) {
    return predicate.filter(filter -> !filter.getName().equalsIgnoreCase("ALWAYS_TRUE"));
  }

  private CloseableIterator<FilteredColumnarBatch> applyPartitionPruning(
      Engine engine, CloseableIterator<FilteredColumnarBatch> scanFileIter) {
    Optional<Predicate> partitionPredicate = getPartitionsFilters();
    if (!partitionPredicate.isPresent()) {
      // There is no partition filter, return the scan file iterator as is.
      return scanFileIter;
    }

    Predicate predicateOnScanFileBatch =
        rewritePartitionPredicateOnScanFileSchema(
            partitionPredicate.get(), partitionColToStructFieldMap.get());

    return new CloseableIterator<FilteredColumnarBatch>() {
      PredicateEvaluator predicateEvaluator = null;

      @Override
      public boolean hasNext() {
        return scanFileIter.hasNext();
      }

      @Override
      public FilteredColumnarBatch next() {
        FilteredColumnarBatch next = scanFileIter.next();
        if (predicateEvaluator == null) {
          predicateEvaluator =
              wrapEngineException(
                  () ->
                      engine
                          .getExpressionHandler()
                          .getPredicateEvaluator(
                              next.getData().getSchema(), predicateOnScanFileBatch),
                  "Get the predicate evaluator for partition pruning with schema=%s and"
                      + " filter=%s",
                  next.getData().getSchema(),
                  predicateOnScanFileBatch);
        }
        ColumnVector newSelectionVector =
            wrapEngineException(
                () -> predicateEvaluator.eval(next.getData(), next.getSelectionVector()),
                "Evaluating the partition expression %s",
                predicateOnScanFileBatch);
        return new FilteredColumnarBatch(next.getData(), Optional.of(newSelectionVector));
      }

      @Override
      public void close() throws IOException {
        scanFileIter.close();
      }
    };
  }

  private Optional<DataSkippingPredicate> getDataSkippingFilter() {
    return getDataFilters()
        .flatMap(
            dataFilters ->
                DataSkippingUtils.constructDataSkippingFilter(
                    dataFilters, metadata.getDataSchema()));
  }

  private CloseableIterator<FilteredColumnarBatch> applyDataSkipping(
      Engine engine,
      CloseableIterator<FilteredColumnarBatch> scanFileIter,
      DataSkippingPredicate dataSkippingFilter) {
    // Get the stats schema
    // It's possible to instead provide the referenced columns when building the schema but
    // pruning it after is much simpler
    StructType prunedStatsSchema =
        DataSkippingUtils.pruneStatsSchema(
            getStatsSchema(metadata.getDataSchema()), dataSkippingFilter.getReferencedCols());

    // Skipping happens in two steps:
    // 1. The predicate produces false for any file whose stats prove we can safely skip it. A
    //    value of true means the stats say we must keep the file, and null means we could not
    //    determine whether the file is safe to skip, because its stats were missing/null.
    // 2. The coalesce(skip, true) converts null (= keep) to true
    Predicate filterToEval =
        new Predicate(
            "=",
            new ScalarExpression(
                "COALESCE", Arrays.asList(dataSkippingFilter, Literal.ofBoolean(true))),
            AlwaysTrue.ALWAYS_TRUE);

    PredicateEvaluator predicateEvaluator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getPredicateEvaluator(prunedStatsSchema, filterToEval),
            "Get the predicate evaluator for data skipping with schema=%s and filter=%s",
            prunedStatsSchema,
            filterToEval);

    return scanFileIter.map(
        filteredScanFileBatch -> {
          ColumnVector newSelectionVector =
              wrapEngineException(
                  () ->
                      predicateEvaluator.eval(
                          DataSkippingUtils.parseJsonStats(
                              engine, filteredScanFileBatch, prunedStatsSchema),
                          filteredScanFileBatch.getSelectionVector()),
                  "Evaluating the data skipping filter %s",
                  filterToEval);

          return new FilteredColumnarBatch(
              filteredScanFileBatch.getData(), Optional.of(newSelectionVector));
        });
  }

  /**
   * Wraps a scan file iterator such that we emit {@link ScanReport} to the engine upon success and
   * failure. Since most of our scan building code is lazily executed (since it occurs as
   * maps/filters over an iterator) potential errors don't occur just within `getScanFile`s
   * execution, but rather may occur as the returned iterator is consumed. Similarly, we cannot
   * report a successful scan until the iterator has been fully consumed and the log read/filtered
   * etc. This means we cannot report the successful scan within `getScanFiles` but rather must
   * report after the iterator has been consumed.
   *
   * <p>This method wraps an inner scan file iterator with an outer iterator wrapper that reports
   * {@link ScanReport}s as needed. It reports a failed {@link ScanReport} in the case of any
   * exceptions originating from the inner iterator `next` and `hasNext` impl. It reports a complete
   * or incomplete {@link ScanReport} when the iterator is closed.
   */
  private CloseableIterator<FilteredColumnarBatch> wrapWithMetricsReporting(
      CloseableIterator<FilteredColumnarBatch> scanIter, ScanReportReporter reporter) {
    return new CloseableIterator<FilteredColumnarBatch>() {

      /* Whether this iterator has reported an error report */
      private boolean errorReported = false;

      @Override
      public void close() throws IOException {
        try {
          // If a ScanReport has already been pushed in the case of an exception don't double report
          if (!errorReported) {
            if (!scanIter.hasNext()) {
              // The entire scan file iterator has been successfully consumed report a complete Scan
              reporter.reportCompleteScan();
            } else {
              // The scan file iterator has NOT been fully consumed before being closed
              // We have no way of knowing the reason why, this could be due to an exception in the
              // connector code, or intentional early termination such as for a LIMIT query
              reporter.reportIncompleteScan();
            }
          }
        } finally {
          scanIter.close();
        }
      }

      @Override
      public boolean hasNext() {
        return wrapWithErrorReporting(() -> scanIter.hasNext());
      }

      @Override
      public FilteredColumnarBatch next() {
        return wrapWithErrorReporting(() -> scanIter.next());
      }

      private <T> T wrapWithErrorReporting(Supplier<T> s) {
        try {
          return s.get();
        } catch (Exception e) {
          reporter.reportError(e);
          errorReported = true;
          throw e;
        }
      }
    };
  }

  /**
   * Defines methods to report {@link ScanReport} to the engine. This allows us to avoid ambiguous
   * lambdas/anonymous classes as well as reuse the defined default methods.
   */
  private interface ScanReportReporter {

    default void reportError(Exception e) {
      report(Optional.of(e), false /* isFullyConsumed */);
    }

    default void reportCompleteScan() {
      report(Optional.empty(), true /* isFullyConsumed */);
    }

    default void reportIncompleteScan() {
      report(Optional.empty(), false /* isFullyConsumed */);
    }

    /** Given an optional exception, reports a {@link ScanReport} to the engine */
    void report(Optional<Exception> exceptionOpt, boolean isFullyConsumed);
  }
}
