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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
<<<<<<< HEAD
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
=======
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphHolder;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the CheckpointConfigHandler.
 */
public class CheckpointConfigHandlerTest {

	@Test
	public void testArchiver() throws IOException {
		JsonArchivist archivist = new CheckpointConfigHandler.CheckpointConfigJsonArchivist();
		GraphAndSettings graphAndSettings = createGraphAndSettings(true, true);

		AccessExecutionGraph graph = graphAndSettings.graph;
		when(graph.getJobID()).thenReturn(new JobID());
<<<<<<< HEAD
		CheckpointCoordinatorConfiguration chkConfig = graphAndSettings.jobCheckpointingConfiguration;
=======
		JobCheckpointingSettings settings = graphAndSettings.snapshottingSettings;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		ExternalizedCheckpointSettings externalizedSettings = graphAndSettings.externalizedSettings;

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(graph);
		Assert.assertEquals(1, archives.size());
		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + graph.getJobID() + "/checkpoints/config", archive.getPath());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(archive.getJson());

		Assert.assertEquals("exactly_once", rootNode.get("mode").asText());
<<<<<<< HEAD
		Assert.assertEquals(chkConfig.getCheckpointInterval(), rootNode.get("interval").asLong());
		Assert.assertEquals(chkConfig.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		Assert.assertEquals(chkConfig.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		Assert.assertEquals(chkConfig.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());
=======
		Assert.assertEquals(settings.getCheckpointInterval(), rootNode.get("interval").asLong());
		Assert.assertEquals(settings.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		Assert.assertEquals(settings.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		Assert.assertEquals(settings.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		JsonNode externalizedNode = rootNode.get("externalization");
		Assert.assertNotNull(externalizedNode);
		Assert.assertEquals(externalizedSettings.externalizeCheckpoints(), externalizedNode.get("enabled").asBoolean());
		Assert.assertEquals(externalizedSettings.deleteOnCancellation(), externalizedNode.get("delete_on_cancellation").asBoolean());

	}

	@Test
	public void testGetPaths() {
<<<<<<< HEAD
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
=======
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class), Executors.directExecutor());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/checkpoints/config", paths[0]);
	}

	/**
	 * Tests a simple config.
	 */
	@Test
	public void testSimpleConfig() throws Exception {
		GraphAndSettings graphAndSettings = createGraphAndSettings(false, true);

		AccessExecutionGraph graph = graphAndSettings.graph;
<<<<<<< HEAD
		CheckpointCoordinatorConfiguration chkConfig = graphAndSettings.jobCheckpointingConfiguration;

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
=======
		JobCheckpointingSettings settings = graphAndSettings.snapshottingSettings;

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class), Executors.directExecutor());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		assertEquals("exactly_once", rootNode.get("mode").asText());
<<<<<<< HEAD
		assertEquals(chkConfig.getCheckpointInterval(), rootNode.get("interval").asLong());
		assertEquals(chkConfig.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		assertEquals(chkConfig.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		assertEquals(chkConfig.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());
=======
		assertEquals(settings.getCheckpointInterval(), rootNode.get("interval").asLong());
		assertEquals(settings.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		assertEquals(settings.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		assertEquals(settings.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		JsonNode externalizedNode = rootNode.get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(false, externalizedNode.get("enabled").asBoolean());
	}

	/**
	 * Tests the that the isExactlyOnce flag is respected.
	 */
	@Test
	public void testAtLeastOnce() throws Exception {
		GraphAndSettings graphAndSettings = createGraphAndSettings(false, false);

		AccessExecutionGraph graph = graphAndSettings.graph;

<<<<<<< HEAD
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
=======
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class), Executors.directExecutor());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		assertEquals("at_least_once", rootNode.get("mode").asText());
	}

	/**
	 * Tests that the externalized checkpoint settings are forwarded.
	 */
	@Test
	public void testEnabledExternalizedCheckpointSettings() throws Exception {
		GraphAndSettings graphAndSettings = createGraphAndSettings(true, false);

		AccessExecutionGraph graph = graphAndSettings.graph;
		ExternalizedCheckpointSettings externalizedSettings = graphAndSettings.externalizedSettings;

<<<<<<< HEAD
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
=======
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class), Executors.directExecutor());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode externalizedNode = mapper.readTree(json).get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(externalizedSettings.externalizeCheckpoints(), externalizedNode.get("enabled").asBoolean());
		assertEquals(externalizedSettings.deleteOnCancellation(), externalizedNode.get("delete_on_cancellation").asBoolean());
	}

	private static GraphAndSettings createGraphAndSettings(boolean externalized, boolean exactlyOnce) {
		long interval = 18231823L;
		long timeout = 996979L;
		long minPause = 119191919L;
		int maxConcurrent = 12929329;
		ExternalizedCheckpointSettings externalizedSetting = externalized
			? ExternalizedCheckpointSettings.externalizeCheckpoints(true)
			: ExternalizedCheckpointSettings.none();

<<<<<<< HEAD
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
=======
		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			interval,
			timeout,
			minPause,
			maxConcurrent,
			externalizedSetting,
<<<<<<< HEAD
			exactlyOnce);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointCoordinatorConfiguration()).thenReturn(chkConfig);

		return new GraphAndSettings(graph, chkConfig, externalizedSetting);
=======
			null,
			exactlyOnce);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getJobCheckpointingSettings()).thenReturn(settings);

		return new GraphAndSettings(graph, settings, externalizedSetting);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	private static class GraphAndSettings {
		public final AccessExecutionGraph graph;
<<<<<<< HEAD
		public final CheckpointCoordinatorConfiguration jobCheckpointingConfiguration;
=======
		public final JobCheckpointingSettings snapshottingSettings;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		public final ExternalizedCheckpointSettings externalizedSettings;

		public GraphAndSettings(
				AccessExecutionGraph graph,
<<<<<<< HEAD
				CheckpointCoordinatorConfiguration jobCheckpointingConfiguration,
				ExternalizedCheckpointSettings externalizedSettings) {
			this.graph = graph;
			this.jobCheckpointingConfiguration = jobCheckpointingConfiguration;
=======
				JobCheckpointingSettings snapshottingSettings,
				ExternalizedCheckpointSettings externalizedSettings) {
			this.graph = graph;
			this.snapshottingSettings = snapshottingSettings;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			this.externalizedSettings = externalizedSettings;
		}
	}
}
