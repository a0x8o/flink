/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

/**
 * The base for the Kafka tests. It brings up:
 * <ul>
 *     <li>A ZooKeeper mini cluster</li>
 *     <li>Three Kafka Brokers (mini clusters)</li>
 *     <li>A Flink mini cluster</li>
 * </ul>
 *
 * <p>Code in this test is based on the following GitHub repository:
 * <a href="https://github.com/sakserv/hadoop-mini-clusters">
 *   https://github.com/sakserv/hadoop-mini-clusters</a> (ASL licensed),
 * as per commit <i>bc6b2b2d5f6424d5f377aa6c0871e82a956462ef</i></p>
 */
@SuppressWarnings("serial")
public abstract class KafkaTestBase extends TestLogger {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);

	protected static final int NUMBER_OF_KAFKA_SERVERS = 3;

	protected static final int NUM_TMS = 1;

	protected static final int TM_SLOTS = 8;

	protected static final int PARALLELISM = NUM_TMS * TM_SLOTS;

	protected static String brokerConnectionStrings;

	protected static Properties standardProps;

	protected static LocalFlinkMiniCluster flink;

	protected static FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

	protected static KafkaTestEnvironment kafkaServer;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	protected static Properties secureProps = new Properties();

	// ------------------------------------------------------------------------
	//  Setup and teardown of the mini clusters
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void prepare() throws ClassNotFoundException {
		prepare(true);
	}

	public static void prepare(boolean hideKafkaBehindProxy) throws ClassNotFoundException {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting KafkaTestBase ");
		LOG.info("-------------------------------------------------------------------------");

		startClusters(false, hideKafkaBehindProxy);

		TestStreamEnvironment.setAsContext(flink, PARALLELISM);
	}

	@AfterClass
	public static void shutDownServices() throws Exception {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shut down KafkaTestBase ");
		LOG.info("-------------------------------------------------------------------------");

		TestStreamEnvironment.unsetAsContext();

		shutdownClusters();

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    KafkaTestBase finished");
		LOG.info("-------------------------------------------------------------------------");
	}

	protected static Configuration getFlinkConfiguration() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, TM_SLOTS);
		flinkConfig.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 16L);
		flinkConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");
		flinkConfig.setString(MetricOptions.REPORTERS_LIST, "my_reporter");
		flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
		return flinkConfig;
	}

	protected static void startClusters(boolean secureMode, boolean hideKafkaBehindProxy) throws ClassNotFoundException {

		// dynamically load the implementation for the test
		Class<?> clazz = Class.forName("org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl");
		kafkaServer = (KafkaTestEnvironment) InstantiationUtil.instantiate(clazz);

		LOG.info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.getVersion());

		kafkaServer.prepare(kafkaServer.createConfig()
			.setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS)
			.setSecureMode(secureMode)
			.setHideKafkaBehindProxy(hideKafkaBehindProxy));

		standardProps = kafkaServer.getStandardProperties();

		brokerConnectionStrings = kafkaServer.getBrokerConnectionString();

		if (secureMode) {
			if (!kafkaServer.isSecureRunSupported()) {
				throw new IllegalStateException(
					"Attempting to test in secure mode but secure mode not supported by the KafkaTestEnvironment.");
			}
			secureProps = kafkaServer.getSecureProperties();
		}

		// start also a re-usable Flink mini cluster
		flink = new LocalFlinkMiniCluster(getFlinkConfiguration(), false);
		flink.start();
	}

	protected static void shutdownClusters() throws Exception {

		if (flink != null) {
			flink.shutdown();
		}

		if (secureProps != null) {
			secureProps.clear();
		}

		kafkaServer.shutdown();

	}

	// ------------------------------------------------------------------------
	//  Execution utilities
	// ------------------------------------------------------------------------

	protected static void tryExecutePropagateExceptions(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			see.execute(name);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					throw root;
				}
				else {
					cause = cause.getCause();
				}
			}
		}
	}

	protected static void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
		kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor);
	}

	protected static void deleteTestTopic(String topic) {
		kafkaServer.deleteTestTopic(topic);
	}

}
