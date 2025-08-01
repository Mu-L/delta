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

import io.delta.kernel.PaginatedScan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.types.StructType;
import java.util.Optional;

/** Implementation of {@link ScanBuilder}. */
public class ScanBuilderImpl implements ScanBuilder {

  private final Path dataPath;
  private final long tableVersion;
  private final Protocol protocol;
  private final Metadata metadata;
  private final StructType snapshotSchema;
  private final LogReplay logReplay;
  private final SnapshotReport snapshotReport;

  private StructType readSchema;
  private Optional<Predicate> predicate;

  public ScanBuilderImpl(
      Path dataPath,
      long tableVersion,
      Protocol protocol,
      Metadata metadata,
      StructType snapshotSchema,
      LogReplay logReplay,
      SnapshotReport snapshotReport) {
    this.dataPath = dataPath;
    this.tableVersion = tableVersion;
    this.protocol = protocol;
    this.metadata = metadata;
    this.snapshotSchema = snapshotSchema;
    this.logReplay = logReplay;
    this.readSchema = snapshotSchema;
    this.predicate = Optional.empty();
    this.snapshotReport = snapshotReport;
  }

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    if (this.predicate.isPresent()) {
      throw new IllegalArgumentException("There already exists a filter in current builder");
    }
    this.predicate = Optional.of(predicate);
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(StructType readSchema) {
    // TODO: Validate that readSchema is a subset of the table schema or that extra fields are
    //  metadata columns.
    this.readSchema = readSchema;
    return this;
  }

  @Override
  public ScanImpl build() {
    return new ScanImpl(
        snapshotSchema,
        readSchema,
        protocol,
        metadata,
        logReplay,
        predicate,
        dataPath,
        snapshotReport);
  }

  @Override
  public PaginatedScan buildPaginated(long pageSize, Optional<Row> pageTokenRowOpt) {
    ScanImpl baseScan = this.build();
    return new PaginatedScanImpl(
        baseScan,
        dataPath.toString(),
        tableVersion,
        pageSize,
        logReplay.getLogSegment(),
        predicate,
        pageTokenRowOpt);
  }
}
