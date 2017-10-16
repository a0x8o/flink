/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.legacy.checkpoints;

import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummary;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.AbstractExecutionGraphRequestHandler;
<<<<<<< HEAD
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.MinMaxAvgStatistics;
=======
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphHolder;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Handler that returns checkpoint statistics for a job.
 */
public class CheckpointStatsHandler extends AbstractExecutionGraphRequestHandler {

	private static final String CHECKPOINT_STATS_REST_PATH = "/jobs/:jobid/checkpoints";

<<<<<<< HEAD
	public CheckpointStatsHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
=======
	public CheckpointStatsHandler(ExecutionGraphHolder executionGraphHolder, Executor executor) {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		super(executionGraphHolder, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{CHECKPOINT_STATS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createCheckpointStatsJson(graph);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create checkpoint stats json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the CheckpointStatsJsonHandler.
	 */
	public static class CheckpointStatsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createCheckpointStatsJson(graph);
			String path = CHECKPOINT_STATS_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}

	private static String createCheckpointStatsJson(AccessExecutionGraph graph) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
		if (snapshot == null) {
			return "{}";
		}

		gen.writeStartObject();

		// Counts
		writeCounts(gen, snapshot.getCounts());

		// Summary
		writeSummary(gen, snapshot.getSummaryStats());

		CheckpointStatsHistory history = snapshot.getHistory();

		// Latest
		writeLatestCheckpoints(
			gen,
			history.getLatestCompletedCheckpoint(),
			history.getLatestSavepoint(),
			history.getLatestFailedCheckpoint(),
			snapshot.getLatestRestoredCheckpoint());

		// History
		writeHistory(gen, snapshot.getHistory());

		gen.writeEndObject();
		gen.close();

		return writer.toString();
	}

	private static void writeCounts(JsonGenerator gen, CheckpointStatsCounts counts) throws IOException {
<<<<<<< HEAD
		gen.writeObjectFieldStart(CheckpointingStatistics.FIELD_NAME_COUNTS);
		gen.writeNumberField(CheckpointingStatistics.Counts.FIELD_NAME_RESTORED_CHECKPOINTS, counts.getNumberOfRestoredCheckpoints());
		gen.writeNumberField(CheckpointingStatistics.Counts.FIELD_NAME_TOTAL_CHECKPOINTS, counts.getTotalNumberOfCheckpoints());
		gen.writeNumberField(CheckpointingStatistics.Counts.FIELD_NAME_IN_PROGRESS_CHECKPOINTS, counts.getNumberOfInProgressCheckpoints());
		gen.writeNumberField(CheckpointingStatistics.Counts.FIELD_NAME_COMPLETED_CHECKPOINTS, counts.getNumberOfCompletedCheckpoints());
		gen.writeNumberField(CheckpointingStatistics.Counts.FIELD_NAME_FAILED_CHECKPOINTS, counts.getNumberOfFailedCheckpoints());
=======
		gen.writeObjectFieldStart("counts");
		gen.writeNumberField("restored", counts.getNumberOfRestoredCheckpoints());
		gen.writeNumberField("total", counts.getTotalNumberOfCheckpoints());
		gen.writeNumberField("in_progress", counts.getNumberOfInProgressCheckpoints());
		gen.writeNumberField("completed", counts.getNumberOfCompletedCheckpoints());
		gen.writeNumberField("failed", counts.getNumberOfFailedCheckpoints());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		gen.writeEndObject();
	}

	private static void writeSummary(
		JsonGenerator gen,
		CompletedCheckpointStatsSummary summary) throws IOException {
<<<<<<< HEAD
		gen.writeObjectFieldStart(CheckpointingStatistics.FIELD_NAME_SUMMARY);
		gen.writeObjectFieldStart(CheckpointingStatistics.Summary.FIELD_NAME_STATE_SIZE);
		writeMinMaxAvg(gen, summary.getStateSizeStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart(CheckpointingStatistics.Summary.FIELD_NAME_DURATION);
		writeMinMaxAvg(gen, summary.getEndToEndDurationStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart(CheckpointingStatistics.Summary.FIELD_NAME_ALIGNMENT_BUFFERED);
=======
		gen.writeObjectFieldStart("summary");
		gen.writeObjectFieldStart("state_size");
		writeMinMaxAvg(gen, summary.getStateSizeStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart("end_to_end_duration");
		writeMinMaxAvg(gen, summary.getEndToEndDurationStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart("alignment_buffered");
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		writeMinMaxAvg(gen, summary.getAlignmentBufferedStats());
		gen.writeEndObject();
		gen.writeEndObject();
	}

	static void writeMinMaxAvg(JsonGenerator gen, MinMaxAvgStats minMaxAvg) throws IOException {
<<<<<<< HEAD
		gen.writeNumberField(MinMaxAvgStatistics.FIELD_NAME_MINIMUM, minMaxAvg.getMinimum());
		gen.writeNumberField(MinMaxAvgStatistics.FIELD_NAME_MAXIMUM, minMaxAvg.getMaximum());
		gen.writeNumberField(MinMaxAvgStatistics.FIELD_NAME_AVERAGE, minMaxAvg.getAverage());
=======
		gen.writeNumberField("min", minMaxAvg.getMinimum());
		gen.writeNumberField("max", minMaxAvg.getMaximum());
		gen.writeNumberField("avg", minMaxAvg.getAverage());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	private static void writeLatestCheckpoints(
		JsonGenerator gen,
		@Nullable CompletedCheckpointStats completed,
		@Nullable CompletedCheckpointStats savepoint,
		@Nullable FailedCheckpointStats failed,
		@Nullable RestoredCheckpointStats restored) throws IOException {

<<<<<<< HEAD
		gen.writeObjectFieldStart(CheckpointingStatistics.FIELD_NAME_LATEST_CHECKPOINTS);
		// Completed checkpoint
		if (completed != null) {
			gen.writeObjectFieldStart(CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_COMPLETED);
=======
		gen.writeObjectFieldStart("latest");
		// Completed checkpoint
		if (completed != null) {
			gen.writeObjectFieldStart("completed");
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			writeCheckpoint(gen, completed);

			String externalPath = completed.getExternalPath();
			if (externalPath != null) {
<<<<<<< HEAD
				gen.writeStringField(CheckpointStatistics.CompletedCheckpointStatistics.FIELD_NAME_EXTERNAL_PATH, completed.getExternalPath());
=======
				gen.writeStringField("external_path", completed.getExternalPath());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			}

			gen.writeEndObject();
		}

		// Completed savepoint
		if (savepoint != null) {
<<<<<<< HEAD
			gen.writeObjectFieldStart(CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_SAVEPOINT);
=======
			gen.writeObjectFieldStart("savepoint");
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			writeCheckpoint(gen, savepoint);

			String externalPath = savepoint.getExternalPath();
			if (externalPath != null) {
<<<<<<< HEAD
				gen.writeStringField(CheckpointStatistics.CompletedCheckpointStatistics.FIELD_NAME_EXTERNAL_PATH, savepoint.getExternalPath());
=======
				gen.writeStringField("external_path", savepoint.getExternalPath());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			}
			gen.writeEndObject();
		}

		// Failed checkpoint
		if (failed != null) {
<<<<<<< HEAD
			gen.writeObjectFieldStart(CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_FAILED);
			writeCheckpoint(gen, failed);

			gen.writeNumberField(CheckpointStatistics.FailedCheckpointStatistics.FIELD_NAME_FAILURE_TIMESTAMP, failed.getFailureTimestamp());
			String failureMsg = failed.getFailureMessage();
			if (failureMsg != null) {
				gen.writeStringField(CheckpointStatistics.FailedCheckpointStatistics.FIELD_NAME_FAILURE_MESSAGE, failureMsg);
=======
			gen.writeObjectFieldStart("failed");
			writeCheckpoint(gen, failed);

			gen.writeNumberField("failure_timestamp", failed.getFailureTimestamp());
			String failureMsg = failed.getFailureMessage();
			if (failureMsg != null) {
				gen.writeStringField("failure_message", failureMsg);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			}
			gen.writeEndObject();
		}

		// Restored checkpoint
		if (restored != null) {
<<<<<<< HEAD
			gen.writeObjectFieldStart(CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_RESTORED);
			gen.writeNumberField(CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_ID, restored.getCheckpointId());
			gen.writeNumberField(CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_RESTORE_TIMESTAMP, restored.getRestoreTimestamp());
			gen.writeBooleanField(CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_IS_SAVEPOINT, restored.getProperties().isSavepoint());

			String externalPath = restored.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField(CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_EXTERNAL_PATH, externalPath);
=======
			gen.writeObjectFieldStart("restored");
			gen.writeNumberField("id", restored.getCheckpointId());
			gen.writeNumberField("restore_timestamp", restored.getRestoreTimestamp());
			gen.writeBooleanField("is_savepoint", restored.getProperties().isSavepoint());

			String externalPath = restored.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField("external_path", externalPath);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			}
			gen.writeEndObject();
		}
		gen.writeEndObject();
	}

	private static void writeCheckpoint(JsonGenerator gen, AbstractCheckpointStats checkpoint) throws IOException {
<<<<<<< HEAD
		gen.writeNumberField(CheckpointStatistics.FIELD_NAME_ID, checkpoint.getCheckpointId());
		gen.writeNumberField(CheckpointStatistics.FIELD_NAME_TRIGGER_TIMESTAMP, checkpoint.getTriggerTimestamp());
		gen.writeNumberField(CheckpointStatistics.FIELD_NAME_LATEST_ACK_TIMESTAMP, checkpoint.getLatestAckTimestamp());
		gen.writeNumberField(CheckpointStatistics.FIELD_NAME_STATE_SIZE, checkpoint.getStateSize());
		gen.writeNumberField(CheckpointStatistics.FIELD_NAME_DURATION, checkpoint.getEndToEndDuration());
		gen.writeNumberField(CheckpointStatistics.FIELD_NAME_ALIGNMENT_BUFFERED, checkpoint.getAlignmentBuffered());
=======
		gen.writeNumberField("id", checkpoint.getCheckpointId());
		gen.writeNumberField("trigger_timestamp", checkpoint.getTriggerTimestamp());
		gen.writeNumberField("latest_ack_timestamp", checkpoint.getLatestAckTimestamp());
		gen.writeNumberField("state_size", checkpoint.getStateSize());
		gen.writeNumberField("end_to_end_duration", checkpoint.getEndToEndDuration());
		gen.writeNumberField("alignment_buffered", checkpoint.getAlignmentBuffered());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

	}

	private static void writeHistory(JsonGenerator gen, CheckpointStatsHistory history) throws IOException {
<<<<<<< HEAD
		gen.writeArrayFieldStart(CheckpointingStatistics.FIELD_NAME_HISTORY);
		for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
			gen.writeStartObject();
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_ID, checkpoint.getCheckpointId());
			gen.writeStringField(CheckpointStatistics.FIELD_NAME_STATUS, checkpoint.getStatus().toString());
			gen.writeBooleanField(CheckpointStatistics.FIELD_NAME_IS_SAVEPOINT, checkpoint.getProperties().isSavepoint());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_TRIGGER_TIMESTAMP, checkpoint.getTriggerTimestamp());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_LATEST_ACK_TIMESTAMP, checkpoint.getLatestAckTimestamp());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_STATE_SIZE, checkpoint.getStateSize());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_DURATION, checkpoint.getEndToEndDuration());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_ALIGNMENT_BUFFERED, checkpoint.getAlignmentBuffered());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_NUM_SUBTASKS, checkpoint.getNumberOfSubtasks());
			gen.writeNumberField(CheckpointStatistics.FIELD_NAME_NUM_ACK_SUBTASKS, checkpoint.getNumberOfAcknowledgedSubtasks());
=======
		gen.writeArrayFieldStart("history");
		for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
			gen.writeStartObject();
			gen.writeNumberField("id", checkpoint.getCheckpointId());
			gen.writeStringField("status", checkpoint.getStatus().toString());
			gen.writeBooleanField("is_savepoint", checkpoint.getProperties().isSavepoint());
			gen.writeNumberField("trigger_timestamp", checkpoint.getTriggerTimestamp());
			gen.writeNumberField("latest_ack_timestamp", checkpoint.getLatestAckTimestamp());
			gen.writeNumberField("state_size", checkpoint.getStateSize());
			gen.writeNumberField("end_to_end_duration", checkpoint.getEndToEndDuration());
			gen.writeNumberField("alignment_buffered", checkpoint.getAlignmentBuffered());
			gen.writeNumberField("num_subtasks", checkpoint.getNumberOfSubtasks());
			gen.writeNumberField("num_acknowledged_subtasks", checkpoint.getNumberOfAcknowledgedSubtasks());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

			if (checkpoint.getStatus().isCompleted()) {
				// --- Completed ---
				CompletedCheckpointStats completed = (CompletedCheckpointStats) checkpoint;

				String externalPath = completed.getExternalPath();
				if (externalPath != null) {
<<<<<<< HEAD
					gen.writeStringField(CheckpointStatistics.CompletedCheckpointStatistics.FIELD_NAME_EXTERNAL_PATH, externalPath);
				}

				gen.writeBooleanField(CheckpointStatistics.CompletedCheckpointStatistics.FIELD_NAME_DISCARDED, completed.isDiscarded());
=======
					gen.writeStringField("external_path", externalPath);
				}

				gen.writeBooleanField("discarded", completed.isDiscarded());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			}
			else if (checkpoint.getStatus().isFailed()) {
				// --- Failed ---
				FailedCheckpointStats failed = (FailedCheckpointStats) checkpoint;

<<<<<<< HEAD
				gen.writeNumberField(CheckpointStatistics.FailedCheckpointStatistics.FIELD_NAME_FAILURE_TIMESTAMP, failed.getFailureTimestamp());

				String failureMsg = failed.getFailureMessage();
				if (failureMsg != null) {
					gen.writeStringField(CheckpointStatistics.FailedCheckpointStatistics.FIELD_NAME_FAILURE_MESSAGE, failureMsg);
=======
				gen.writeNumberField("failure_timestamp", failed.getFailureTimestamp());

				String failureMsg = failed.getFailureMessage();
				if (failureMsg != null) {
					gen.writeStringField("failure_message", failureMsg);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
				}
			}

			gen.writeEndObject();
		}
		gen.writeEndArray();
	}
}
