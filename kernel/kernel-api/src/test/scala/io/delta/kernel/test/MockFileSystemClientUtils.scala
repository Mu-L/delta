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
package io.delta.kernel.test

import java.util.{Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.kernel.engine._
import io.delta.kernel.internal.MockReadLastCheckpointFileJsonHandler
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

object MockFileSystemClientUtils extends MockFileSystemClientUtils

/**
 * This is an extension to [[BaseMockFileSystemClient]] containing specific mock implementations
 * [[FileSystemClient]] which are shared across multiple test suite.
 *
 * [[MockListFromFileSystemClient]] - mocks the `listFrom` API within [[FileSystemClient]].
 */
trait MockFileSystemClientUtils extends MockEngineUtils {

  val dataPath = new Path("/fake/path/to/table/")
  val logPath = new Path(dataPath, "_delta_log")

  def parsedRatifiedStagedCommit(version: Long): ParsedLogData = {
    ParsedLogData.forFileStatus(stagedCommitFile(version))
  }

  def parsedRatifiedStagedCommits(versions: Seq[Long]): Seq[ParsedLogData] = {
    versions.map(parsedRatifiedStagedCommit)
  }

  /** Staged commit file status where the timestamp = 10*version */
  def stagedCommitFile(v: Long): FileStatus =
    FileStatus.of(FileNames.stagedCommitFile(logPath, v), v, v * 10)

  /** Delta file status where the timestamp = 10*version */
  def deltaFileStatus(v: Long, path: Path = logPath): FileStatus =
    FileStatus.of(FileNames.deltaFile(path, v), v, v * 10)

  /** Compaction file status where the timestamp = 10*startVersion */
  def logCompactionStatus(s: Long, e: Long, path: Path = logPath): FileStatus =
    FileStatus.of(FileNames.logCompactionPath(path, s, e).toString, s, s * 10)

  /** Delta file statuses where the timestamp = 10*version */
  def deltaFileStatuses(deltaVersions: Seq[Long], path: Path = logPath): Seq[FileStatus] = {
    assert(deltaVersions.size == deltaVersions.toSet.size)
    deltaVersions.map(v => deltaFileStatus(v, path))
  }

  /** Compaction file statuses where the timestamp = 10*startVersion */
  def compactedFileStatuses(
      compactedVersions: Seq[(Long, Long)],
      path: Path = logPath): Seq[FileStatus] = {
    compactedVersions.map { case (s, e) =>
      logCompactionStatus(s, e, path)
    }
  }

  /** Checksum file status for given a version */
  def checksumFileStatus(deltaVersion: Long): FileStatus = {
    FileStatus.of(FileNames.checksumFile(logPath, deltaVersion).toString, 10, 10)
  }

  /** Classic checkpoint file status where the timestamp = 10*version */
  def classicCheckpointFileStatus(v: Long): FileStatus = {
    FileStatus.of(FileNames.checkpointFileSingular(logPath, v).toString, v, v * 10)
  }

  /** Checkpoint file statuses where the timestamp = 10*version */
  def singularCheckpointFileStatuses(
      checkpointVersions: Seq[Long],
      path: Path = logPath): Seq[FileStatus] = {
    assert(checkpointVersions.size == checkpointVersions.toSet.size)
    checkpointVersions.map(v =>
      FileStatus.of(FileNames.checkpointFileSingular(path, v).toString, v, v * 10))
  }

  /** Multi-part checkpoint file status where the timestamp = 10*version */
  def multiPartCheckpointFileStatus(version: Long, part: Integer, numParts: Integer): FileStatus = {
    val path = FileNames.multiPartCheckpointFile(logPath, version, part, numParts)
    FileStatus.of(path.toString, version, version * 10)
  }

  /** Checkpoint file statuses where the timestamp = 10*version */
  def multiCheckpointFileStatuses(
      checkpointVersions: Seq[Long],
      numParts: Int): Seq[FileStatus] = {
    assert(checkpointVersions.size == checkpointVersions.toSet.size)
    checkpointVersions.flatMap(v =>
      FileNames.checkpointFileWithParts(logPath, v, numParts).asScala
        .map(p => FileStatus.of(p.toString, v, v * 10)))
  }

  /** Checkpoint file status for a top-level V2 checkpoint file. */
  def v2CheckpointFileStatus(
      version: Long,
      useUUID: Boolean = true,
      fileType: String = "json"): FileStatus = {
    val path = if (useUUID) {
      val uuid = UUID.randomUUID().toString
      FileNames.topLevelV2CheckpointFile(logPath, version, uuid, fileType).toString
    } else {
      FileNames.checkpointFileSingular(logPath, version).toString
    }
    FileStatus.of(path, version, version * 10)
  }

  /**
   * Checkpoint file status for a top-level V2 checkpoint file.
   *
   * @param checkpointVersions List of checkpoint versions, given as Seq(version, whether to use
   *                           UUID naming scheme, number of sidecars).
   * Returns top-level checkpoint file and sidecar files for each checkpoint version.
   */
  def v2CheckpointFileStatuses(
      checkpointVersions: Seq[(Long, Boolean, Int)],
      fileType: String): Seq[(FileStatus, Seq[FileStatus])] = {
    checkpointVersions.map { case (v, useUUID, numSidecars) =>
      val topLevelFile = v2CheckpointFileStatus(v, useUUID, fileType)
      val sidecars = (0 until numSidecars).map { _ =>
        FileStatus.of(
          FileNames.v2CheckpointSidecarFile(logPath, UUID.randomUUID().toString).toString,
          v,
          v * 10)
      }
      (topLevelFile, sidecars)
    }
  }

  /* Create input function for createMockEngine to implement listFrom from a list of
   * file statuses.
   */
  def listFromProvider(files: Seq[FileStatus])(filePath: String): Seq[FileStatus] = {
    files.filter(_.getPath.compareTo(filePath) >= 0).sortBy(_.getPath)
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * the given contents. The contents are filtered depending upon the list from path prefix.
   */
  def createMockFSListFromEngine(
      contents: Seq[FileStatus],
      parquetHandler: ParquetHandler,
      jsonHandler: JsonHandler): Engine = {
    mockEngine(
      fileSystemClient =
        new MockListFromFileSystemClient(listFromProvider(contents)),
      parquetHandler = parquetHandler,
      jsonHandler = jsonHandler)
  }

  def createMockFSAndJsonEngineForLastCheckpoint(
      contents: Seq[FileStatus],
      lastCheckpointVersion: Optional[java.lang.Long]): Engine = {
    mockEngine(
      fileSystemClient = new MockListFromFileSystemClient(listFromProvider(contents)),
      jsonHandler = if (lastCheckpointVersion.isPresent) {
        new MockReadLastCheckpointFileJsonHandler(
          s"$logPath/_last_checkpoint",
          lastCheckpointVersion.get())
      } else {
        null
      })
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * the given list of delta file statuses. When read, each file status will return
   * a single `commitInfo` action with the an inCommitTimestamp set as per
   * `deltaToICTMap`.
   */
  def createMockFSAndJsonEngineForICT(
      contents: Seq[FileStatus],
      deltaToICTMap: Map[Long, Long]): Engine = {
    mockEngine(
      fileSystemClient = new MockListFromFileSystemClient(listFromProvider(contents)),
      jsonHandler = new MockReadICTFileJsonHandler(deltaToICTMap))
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * the given contents. The contents are filtered depending upon the list from path prefix.
   */
  def createMockFSListFromEngine(contents: Seq[FileStatus]): Engine = {
    mockEngine(fileSystemClient =
      new MockListFromFileSystemClient(listFromProvider(contents)))
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * [[MockListFromFileSystemClient]].
   */
  def createMockFSListFromEngine(listFromProvider: String => Seq[FileStatus]): Engine = {
    mockEngine(fileSystemClient = new MockListFromFileSystemClient(listFromProvider))
  }
}

/**
 * A mock [[FileSystemClient]] that answers `listFrom` calls from a given content provider.
 *
 * It also maintains metrics on number of times `listFrom` is called and arguments for each call.
 */
class MockListFromFileSystemClient(listFromProvider: String => Seq[FileStatus])
    extends BaseMockFileSystemClient {
  private var listFromCalls: Seq[String] = Seq.empty

  override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
    listFromCalls = listFromCalls :+ filePath
    toCloseableIterator(listFromProvider(filePath).iterator.asJava)
  }

  override def resolvePath(path: String): String = path

  def getListFromCalls: Seq[String] = listFromCalls
}

/**
 * A mock [[FileSystemClient]] that answers `listFrom` calls from a given content provider and
 * implements the identity function for `resolvePath` calls.
 *
 * It also maintains metrics on number of times `listFrom` is called and arguments for each call.
 */
class MockListFromResolvePathFileSystemClient(listFromProvider: String => Seq[FileStatus])
    extends BaseMockFileSystemClient {
  private var listFromCalls: Seq[String] = Seq.empty

  override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
    listFromCalls = listFromCalls :+ filePath
    toCloseableIterator(listFromProvider(filePath).iterator.asJava)
  }

  override def resolvePath(path: String): String = path

  def getListFromCalls: Seq[String] = listFromCalls
}

/**
 * A mock [[FileSystemClient]] that answers `listFrom` call from the given list of file statuses
 * and tracks the delete calls.
 * @param listContents List of file statuses to be returned by `listFrom` call.
 */
class MockListFromDeleteFileSystemClient(listContents: Seq[FileStatus])
    extends BaseMockFileSystemClient {
  private val listOfFiles: Seq[String] = listContents.map(_.getPath).toSeq
  private var isListFromAlreadyCalled = false
  private var deleteCalls: Seq[String] = Seq.empty

  override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
    assert(!isListFromAlreadyCalled, "listFrom should be called only once")
    isListFromAlreadyCalled = true
    toCloseableIterator(listContents.sortBy(_.getPath).asJava.iterator())
  }

  override def delete(path: String): Boolean = {
    deleteCalls = deleteCalls :+ path
    listOfFiles.contains(path)
  }

  def getDeleteCalls: Seq[String] = deleteCalls
}
