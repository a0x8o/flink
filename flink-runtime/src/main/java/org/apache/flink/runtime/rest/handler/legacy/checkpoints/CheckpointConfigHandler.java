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

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
<<<<<<< HEAD
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.rest.handler.legacy.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
=======
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.rest.handler.legacy.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphHolder;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Handler that returns a job's snapshotting settings.
 */
public class CheckpointConfigHandler extends AbstractExecutionGraphRequestHandler {

	private static final String CHECKPOINT_CONFIG_REST_PATH = "/jobs/:jobid/checkpoints/config";

<<<<<<< HEAD
	public CheckpointConfigHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
=======
	public CheckpointConfigHandler(ExecutionGraphHolder executionGraphHolder, Executor executor) {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		super(executionGraphHolder, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{CHECKPOINT_CONFIG_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createCheckpointConfigJson(graph);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create checkpoint config json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the CheckpointConfigHandler.
	 */
	public static class CheckpointConfigJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createCheckpointConfigJson(graph);
			String path = CHECKPOINT_CONFIG_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}

	private static String createCheckpointConfigJson(AccessExecutionGraph graph) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
<<<<<<< HEAD
		CheckpointCoordinatorConfiguration jobCheckpointingConfiguration = graph.getCheckpointCoordinatorConfiguration();

		if (jobCheckpointingConfiguration == null) {
=======
		JobCheckpointingSettings settings = graph.getJobCheckpointingSettings();

		if (settings == null) {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			return "{}";
		}

		gen.writeStartObject();
		{
<<<<<<< HEAD
			gen.writeStringField(CheckpointConfigInfo.FIELD_NAME_PROCESSING_MODE, jobCheckpointingConfiguration.isExactlyOnce() ? "exactly_once" : "at_least_once");
			gen.writeNumberField(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_INTERVAL, jobCheckpointingConfiguration.getCheckpointInterval());
			gen.writeNumberField(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_TIMEOUT, jobCheckpointingConfiguration.getCheckpointTimeout());
			gen.writeNumberField(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_MIN_PAUSE, jobCheckpointingConfiguration.getMinPauseBetweenCheckpoints());
			gen.writeNumberField(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_MAX_CONCURRENT, jobCheckpointingConfiguration.getMaxConcurrentCheckpoints());

			ExternalizedCheckpointSettings externalization = jobCheckpointingConfiguration.getExternalizedCheckpointSettings();
			gen.writeObjectFieldStart(CheckpointConfigInfo.FIELD_NAME_EXTERNALIZED_CHECKPOINT_CONFIG);
			{
				if (externalization.externalizeCheckpoints()) {
					gen.writeBooleanField(CheckpointConfigInfo.ExternalizedCheckpointInfo.FIELD_NAME_ENABLED, true);
					gen.writeBooleanField(CheckpointConfigInfo.ExternalizedCheckpointInfo.FIELD_NAME_DELETE_ON_CANCELLATION, externalization.deleteOnCancellation());
				} else {
					gen.writeBooleanField(CheckpointConfigInfo.ExternalizedCheckpointInfo.FIELD_NAME_ENABLED, false);
=======
			gen.writeStringField("mode", settings.isExactlyOnce() ? "exactly_once" : "at_least_once");
			gen.writeNumberField("interval", settings.getCheckpointInterval());
			gen.writeNumberField("timeout", settings.getCheckpointTimeout());
			gen.writeNumberField("min_pause", settings.getMinPauseBetweenCheckpoints());
			gen.writeNumberField("max_concurrent", settings.getMaxConcurrentCheckpoints());

			ExternalizedCheckpointSettings externalization = settings.getExternalizedCheckpointSettings();
			gen.writeObjectFieldStart("externalization");
			{
				if (externalization.externalizeCheckpoints()) {
					gen.writeBooleanField("enabled", true);
					gen.writeBooleanField("delete_on_cancellation", externalization.deleteOnCancellation());
				} else {
					gen.writeBooleanField("enabled", false);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
				}
			}
			gen.writeEndObject();

		}
		gen.writeEndObject();

		gen.close();

		return writer.toString();
	}
}
