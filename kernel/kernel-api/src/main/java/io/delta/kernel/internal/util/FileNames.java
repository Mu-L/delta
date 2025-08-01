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

package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FileNames {

  private FileNames() {}

  ////////////////////////////////////////////////
  // File name patterns and other static values //
  ////////////////////////////////////////////////

  // TODO: Delete this in favor of ParsedLogCategory.
  public enum DeltaLogFileType {
    COMMIT,
    LOG_COMPACTION,
    CHECKPOINT,
    CHECKSUM
  }

  /** Example: 00000000000000000001.json */
  private static final Pattern DELTA_FILE_PATTERN = Pattern.compile("\\d+\\.json");

  /** Example: 00000000000000000001.00000000000000000009.compacted.json */
  private static final Pattern COMPACTION_FILE_PATTERN =
      Pattern.compile("\\d+\\.\\d+\\.compacted\\.json");

  /** Example: 00000000000000000001.dc0f9f58-a1a0-46fd-971a-bd8b2e9dbb81.json */
  private static final Pattern UUID_DELTA_FILE_REGEX = Pattern.compile("(\\d+)\\.([^\\.]+)\\.json");

  /**
   * Examples:
   *
   * <ul>
   *   <li>Classic V1 - 00000000000000000001.checkpoint.parquet
   *   <li>Multi-part V1 - 00000000000000000001.checkpoint.0000000001.0000000010.parquet
   *   <li>V2 JSON - 00000000000000000001.checkpoint.uuid-1234abcd.json
   *   <li>V2 Parquet - 00000000000000000001.checkpoint.uuid-1234abcd.parquet
   * </ul>
   */
  private static final Pattern CHECKPOINT_FILE_PATTERN =
      Pattern.compile("(\\d+)\\.checkpoint((\\.\\d+\\.\\d+)?\\.parquet|\\.[^.]+\\.(json|parquet))");

  /** Example: 00000000000000000001.checkpoint.parquet */
  private static final Pattern CLASSIC_CHECKPOINT_FILE_PATTERN =
      Pattern.compile("\\d+\\.checkpoint\\.parquet");

  /** Example: 00000000000000000001.crc */
  private static final Pattern CHECK_SUM_FILE_PATTERN = Pattern.compile("(\\d+)\\.crc");

  /**
   * Examples:
   *
   * <ul>
   *   <li>00000000000000000001.checkpoint.dc0f9f58-a1a0-46fd-971a-bd8b2e9dbb81.json
   *   <li>00000000000000000001.checkpoint.dc0f9f58-a1a0-46fd-971a-bd8b2e9dbb81.parquet
   * </ul>
   */
  private static final Pattern V2_CHECKPOINT_FILE_PATTERN =
      Pattern.compile("(\\d+)\\.checkpoint\\.[^.]+\\.(json|parquet)");

  /** Example: 00000000000000000001.checkpoint.0000000020.0000000060.parquet */
  public static final Pattern MULTI_PART_CHECKPOINT_FILE_PATTERN =
      Pattern.compile("(\\d+)\\.checkpoint\\.(\\d+)\\.(\\d+)\\.parquet");

  public static final String STAGED_COMMIT_DIRECTORY = "_staged_commits";

  public static final String SIDECAR_DIRECTORY = "_sidecars";

  public static DeltaLogFileType determineFileType(FileStatus file) {
    final String fileName = file.getPath().toString();

    if (isCommitFile(fileName)) {
      return DeltaLogFileType.COMMIT;
    } else if (isCheckpointFile(fileName)) {
      return DeltaLogFileType.CHECKPOINT;
    } else if (isLogCompactionFile(fileName)) {
      return DeltaLogFileType.LOG_COMPACTION;
    } else if (isChecksumFile(fileName)) {
      return DeltaLogFileType.CHECKSUM;
    } else {
      throw new IllegalStateException("Unexpected file type: " + fileName);
    }
  }

  ////////////////////////
  // Version extractors //
  ////////////////////////

  /**
   * Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
   * file type is seen. These unexpected files should be filtered out to ensure forward
   * compatibility in cases where new file types are added, but without an explicit protocol
   * upgrade.
   */
  public static long getFileVersion(Path path) {
    if (isCheckpointFile(path.getName())) {
      return checkpointVersion(path);
    } else if (isCommitFile(path.getName())) {
      return deltaVersion(path);
    } else if (isChecksumFile(path.getName())) {
      return checksumVersion(path);
    } else {
      throw new IllegalArgumentException(
          String.format("Unexpected file type found in transaction log: %s", path));
    }
  }

  /** Returns the version for the given delta path. */
  public static long deltaVersion(Path path) {
    return Long.parseLong(path.getName().split("\\.")[0]);
  }

  public static long deltaVersion(String path) {
    final int slashIdx = path.lastIndexOf(Path.SEPARATOR);
    final String name = path.substring(slashIdx + 1);
    return Long.parseLong(name.split("\\.")[0]);
  }

  /** Returns the start and end versions for the given compaction path. */
  public static Tuple2<Long, Long> logCompactionVersions(Path path) {
    final String[] split = path.getName().split("\\.");
    return new Tuple2<>(Long.parseLong(split[0]), Long.parseLong(split[1]));
  }

  public static Tuple2<Long, Long> logCompactionVersions(String path) {
    return logCompactionVersions(new Path(path));
  }

  /** Returns the version for the given checkpoint path. */
  public static long checkpointVersion(Path path) {
    return Long.parseLong(path.getName().split("\\.")[0]);
  }

  public static long checkpointVersion(String path) {
    final int slashIdx = path.lastIndexOf(Path.SEPARATOR);
    final String name = path.substring(slashIdx + 1);
    return Long.parseLong(name.split("\\.")[0]);
  }

  public static Tuple2<Integer, Integer> multiPartCheckpointPartAndNumParts(Path path) {
    final String fileName = path.getName();
    final Matcher matcher = MULTI_PART_CHECKPOINT_FILE_PATTERN.matcher(fileName);
    checkArgument(
        matcher.matches(), String.format("Path is not a multi-part checkpoint file: %s", fileName));
    final int partNum = Integer.parseInt(matcher.group(2));
    final int numParts = Integer.parseInt(matcher.group(3));
    return new Tuple2<>(partNum, numParts);
  }

  public static Tuple2<Integer, Integer> multiPartCheckpointPartAndNumParts(String path) {
    return multiPartCheckpointPartAndNumParts(new Path(path));
  }

  ///////////////////////////////////
  // File path and prefix builders //
  ///////////////////////////////////

  /** Returns the delta (json format) path for a given delta file. */
  public static String deltaFile(Path path, long version) {
    return String.format("%s/%020d.json", path, version);
  }

  public static String stagedCommitFile(Path logPath, long version) {
    final Path stagedCommitPath = new Path(logPath, STAGED_COMMIT_DIRECTORY);
    return String.format("%s/%020d.%s.json", stagedCommitPath, version, UUID.randomUUID());
  }

  public static String stagedCommitFile(String logPath, long version) {
    return stagedCommitFile(new Path(logPath), version);
  }

  /** Example: /a/_sidecars/3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet */
  public static String sidecarFile(Path path, String sidecar) {
    return String.format("%s/%s/%s", path.toString(), SIDECAR_DIRECTORY, sidecar);
  }

  /** Returns the path to the checksum file for the given version. */
  public static Path checksumFile(Path path, long version) {
    return new Path(path, String.format("%020d.crc", version));
  }

  public static long checksumVersion(Path path) {
    return Long.parseLong(path.getName().split("\\.")[0]);
  }

  public static long checksumVersion(String path) {
    return checksumVersion(new Path(path));
  }

  /**
   * Returns the prefix of all delta log files for the given version.
   *
   * <p>Intended for use with listFrom to get all files from this version onwards. The returned Path
   * will not exist as a file.
   */
  public static String listingPrefix(Path path, long version) {
    return String.format("%s/%020d.", path, version);
  }

  /**
   * Returns the path for a singular checkpoint up to the given version.
   *
   * <p>In a future protocol version this path will stop being written.
   */
  public static Path checkpointFileSingular(Path path, long version) {
    return new Path(path, String.format("%020d.checkpoint.parquet", version));
  }

  /**
   * Returns the path for a top-level V2 checkpoint file up to the given version with a given UUID
   * and filetype (JSON or Parquet).
   */
  public static Path topLevelV2CheckpointFile(
      Path path, long version, String uuid, String fileType) {
    assert (fileType.equals("json") || fileType.equals("parquet"));
    return new Path(path, String.format("%020d.checkpoint.%s.%s", version, uuid, fileType));
  }

  /** Returns the path for a V2 sidecar file with a given UUID. */
  public static Path v2CheckpointSidecarFile(Path path, String uuid) {
    return new Path(String.format("%s/%s/%s.parquet", path.toString(), SIDECAR_DIRECTORY, uuid));
  }

  public static Path multiPartCheckpointFile(Path path, long version, int part, int numParts) {
    return new Path(
        path, String.format("%020d.checkpoint.%010d.%010d.parquet", version, part, numParts));
  }

  /**
   * Returns the paths for all parts of the checkpoint up to the given version.
   *
   * <p>In a future protocol version we will write this path instead of checkpointFileSingular.
   *
   * <p>Example of the format: 00000000000000004915.checkpoint.0000000020.0000000060.parquet is
   * checkpoint part 20 out of 60 for the snapshot at version 4915. Zero padding is for
   * lexicographic sorting.
   */
  public static List<Path> checkpointFileWithParts(Path path, long version, int numParts) {
    final List<Path> output = new ArrayList<>();
    for (int i = 1; i < numParts + 1; i++) {
      output.add(multiPartCheckpointFile(path, version, i, numParts));
    }
    return output;
  }

  /**
   * Return the path that should be used for a log compaction file.
   *
   * @param logPath path to the delta log location
   * @param startVersion the start version for the log compaction
   * @param endVersion the end version for the log compaction
   */
  public static Path logCompactionPath(Path logPath, long startVersion, long endVersion) {
    String fileName = String.format("%020d.%020d.compacted.json", startVersion, endVersion);
    return new Path(logPath, fileName);
  }

  /////////////////////////////
  // Is <type> file checkers //
  /////////////////////////////

  public static boolean isCheckpointFile(String path) {
    return CHECKPOINT_FILE_PATTERN.matcher(new Path(path).getName()).matches();
  }

  public static boolean isClassicCheckpointFile(String path) {
    return CLASSIC_CHECKPOINT_FILE_PATTERN.matcher(new Path(path).getName()).matches();
  }

  public static boolean isMultiPartCheckpointFile(String path) {
    return MULTI_PART_CHECKPOINT_FILE_PATTERN.matcher(new Path(path).getName()).matches();
  }

  public static boolean isV2CheckpointFile(String path) {
    return V2_CHECKPOINT_FILE_PATTERN.matcher(new Path(path).getName()).matches();
  }

  public static boolean isCommitFile(String path) {
    final String fileName = new Path(path).getName();
    return DELTA_FILE_PATTERN.matcher(fileName).matches()
        || UUID_DELTA_FILE_REGEX.matcher(fileName).matches();
  }

  public static boolean isPublishedDeltaFile(String path) {
    final String fileName = new Path(path).getName();
    return DELTA_FILE_PATTERN.matcher(fileName).matches();
  }

  public static boolean isStagedDeltaFile(String path) {
    final Path p = new Path(path);
    if (!p.getParent().getName().equals(STAGED_COMMIT_DIRECTORY)) {
      return false;
    }
    return UUID_DELTA_FILE_REGEX.matcher(p.getName()).matches();
  }

  public static boolean isLogCompactionFile(String path) {
    final String fileName = new Path(path).getName();
    return COMPACTION_FILE_PATTERN.matcher(fileName).matches();
  }

  public static boolean isChecksumFile(String checksumFilePath) {
    return CHECK_SUM_FILE_PATTERN.matcher(new Path(checksumFilePath).getName()).matches();
  }
}
