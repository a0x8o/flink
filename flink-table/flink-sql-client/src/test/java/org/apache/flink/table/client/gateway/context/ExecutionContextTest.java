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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.python.PythonFunctionFactory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.CatalogEntry;
import org.apache.flink.table.client.gateway.local.DependencyTest;
import org.apache.flink.table.client.gateway.utils.DummyTableSourceFactory;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.functions.python.PythonScalarFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.StringUtils;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

/** Test for {@link ExecutionContext}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PythonFunctionFactory.class)
@PowerMockIgnore({
    "org.apache.hive.*",
    "org.apache.hadoop.*",
    "com.sun.org.apache.xerces.*",
    "javax.xml.*",
    "org.xml.*",
    "javax.management.*",
    "org.w3c.dom.*",
    "org.datanucleus.*"
})
public class ExecutionContextTest {

    private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
    public static final String MODULES_ENVIRONMENT_FILE = "test-sql-client-modules.yaml";
    public static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-catalogs.yaml";
    private static final String STREAMING_ENVIRONMENT_FILE = "test-sql-client-streaming.yaml";
    private static final String CONFIGURATION_ENVIRONMENT_FILE =
            "test-sql-client-configuration.yaml";
    private static final String FUNCTION_ENVIRONMENT_FILE = "test-sql-client-python-functions.yaml";
    private static final String EXECUTION_ENVIRONMENT_FILE = "test-sql-client-execution.yaml";

    private SessionContext sessionContext;

    @Test
    public void testExecutionConfig() throws Exception {
        final ExecutionContext context = createDefaultExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();
        final TableConfig tableConfig = tableEnv.getConfig();

        assertEquals(1_000, tableConfig.getMinIdleStateRetentionTime());
        assertEquals(1_000 * 3 / 2, tableConfig.getMaxIdleStateRetentionTime());
        Configuration conf = tableConfig.getConfiguration();

        assertEquals(1, conf.getInteger(CoreOptions.DEFAULT_PARALLELISM));
        assertEquals(16, conf.getInteger(PipelineOptions.MAX_PARALLELISM));

        assertEquals(Duration.ofMillis(99), conf.get(PipelineOptions.AUTO_WATERMARK_INTERVAL));

        assertEquals("failure-rate", conf.getString(RestartStrategyOptions.RESTART_STRATEGY));
        assertEquals(
                10,
                conf.getInteger(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL));
        assertEquals(
                Duration.ofMillis(99_000),
                conf.get(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL));
        assertEquals(
                Duration.ofMillis(1_000),
                conf.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY));
    }

    @Test
    public void testDefaultExecutionConfig() throws Exception {
        final ExecutionContext context = createExecutionExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();
        final TableConfig tableConfig = tableEnv.getConfig();

        assertEquals(1_000, tableConfig.getMinIdleStateRetentionTime());
        assertEquals(1_000 * 3 / 2, tableConfig.getMaxIdleStateRetentionTime());
        Configuration conf = tableConfig.getConfiguration();

        assertEquals(1, conf.getInteger(CoreOptions.DEFAULT_PARALLELISM));
        assertEquals(16, conf.getInteger(PipelineOptions.MAX_PARALLELISM));

        assertEquals(Duration.ofMillis(99), conf.get(PipelineOptions.AUTO_WATERMARK_INTERVAL));

        assertNull(conf.getString(RestartStrategyOptions.RESTART_STRATEGY));
        assertEquals(
                1,
                conf.getInteger(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL));
        assertEquals(
                Duration.ofMinutes(1),
                conf.get(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL));
        assertEquals(
                Duration.ofSeconds(1),
                conf.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY));
    }

    @Test
    public void testModules() throws Exception {
        final ExecutionContext context = createModuleExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        Set<String> allModules = new HashSet<>(Arrays.asList(tableEnv.listModules()));
        assertEquals(4, allModules.size());
        assertEquals(
                new HashSet<>(Arrays.asList("core", "mymodule", "myhive", "myhive2")), allModules);
    }

    @Test
    @Category(FailsOnJava11.class)
    public void testCatalogs() throws Exception {
        final String inmemoryCatalog = "inmemorycatalog";
        final String hiveCatalog = "hivecatalog";
        final String hiveDefaultVersionCatalog = "hivedefaultversion";

        final ExecutionContext context = createCatalogExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        assertEquals(inmemoryCatalog, tableEnv.getCurrentCatalog());
        assertEquals("mydatabase", tableEnv.getCurrentDatabase());

        Catalog catalog = tableEnv.getCatalog(hiveCatalog).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof HiveCatalog);
        assertEquals("2.3.4", ((HiveCatalog) catalog).getHiveVersion());

        catalog = tableEnv.getCatalog(hiveDefaultVersionCatalog).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof HiveCatalog);
        // make sure we have assigned a default hive version
        assertFalse(StringUtils.isNullOrWhitespaceOnly(((HiveCatalog) catalog).getHiveVersion()));

        tableEnv.useCatalog(hiveCatalog);

        assertEquals(hiveCatalog, tableEnv.getCurrentCatalog());

        Set<String> allCatalogs = new HashSet<>(Arrays.asList(tableEnv.listCatalogs()));
        assertEquals(6, allCatalogs.size());
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "default_catalog",
                                inmemoryCatalog,
                                hiveCatalog,
                                hiveDefaultVersionCatalog,
                                "catalog1",
                                "catalog2")),
                allCatalogs);

        sessionContext.close();
    }

    @Test
    @Category(FailsOnJava11.class)
    public void testDatabases() throws Exception {
        final String hiveCatalog = "hivecatalog";

        final ExecutionContext context = createCatalogExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        assertEquals(1, tableEnv.listDatabases().length);
        assertEquals("mydatabase", tableEnv.listDatabases()[0]);

        tableEnv.useCatalog(hiveCatalog);

        assertEquals(2, tableEnv.listDatabases().length);
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                HiveCatalog.DEFAULT_DB,
                                DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE)),
                new HashSet<>(Arrays.asList(tableEnv.listDatabases())));

        tableEnv.useCatalog(hiveCatalog);

        assertEquals(HiveCatalog.DEFAULT_DB, tableEnv.getCurrentDatabase());

        tableEnv.useDatabase(DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE);

        assertEquals(
                DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE,
                tableEnv.getCurrentDatabase());

        sessionContext.close();
    }

    @Test
    public void testFunctions() throws Exception {
        final ExecutionContext context = createDefaultExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();
        final String[] expected = new String[] {"scalarudf", "tableudf", "aggregateudf"};
        final String[] actual = tableEnv.listUserDefinedFunctions();
        Arrays.sort(expected);
        Arrays.sort(actual);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testPythonFunction() throws Exception {
        PythonFunctionFactory testFunctionFactory =
                (moduleName, objectName) ->
                        new PythonScalarFunction(null, null, null, null, null, false, false, null);
        PowerMockito.mockStatic(PythonFunctionFactory.class);
        when(PythonFunctionFactory.getPythonFunction(
                        anyString(), any(ReadableConfig.class), any(ClassLoader.class)))
                .thenCallRealMethod();
        when(PythonFunctionFactory.createPythonFunctionFactory(any(ReadableConfig.class)))
                .thenReturn(testFunctionFactory);
        ExecutionContext context = createPythonFunctionExecutionContext();
        final String[] expected = new String[] {"pythonudf"};
        final String[] actual = context.getTableEnvironment().listUserDefinedFunctions();
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testTables() throws Exception {
        final ExecutionContext context = createDefaultExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        assertArrayEquals(
                new String[] {
                    "TableNumber1", "TableNumber2", "TableSourceSink", "TestView1", "TestView2"
                },
                tableEnv.listTables());
    }

    @Test
    public void testTemporalTables() throws Exception {
        final ExecutionContext context = createStreamingExecutionContext();
        final StreamTableEnvironment tableEnv = context.getTableEnvironment();

        assertArrayEquals(
                new String[] {
                    "EnrichmentSource", "HistorySource", "HistoryView", "TemporalTableUsage"
                },
                tableEnv.listTables());

        assertArrayEquals(
                new String[] {"sourcetemporaltable", "viewtemporaltable"},
                tableEnv.listUserDefinedFunctions());

        assertEquals(
                Arrays.asList(
                        "integerField",
                        "stringField",
                        "rowtimeField",
                        "integerField0",
                        "stringField0",
                        "rowtimeField0"),
                tableEnv.from("TemporalTableUsage").getResolvedSchema().getColumnNames());

        // Please delete this test after removing registerTableSourceInternal in SQL-CLI.
        ResolvedSchema schema = tableEnv.from("EnrichmentSource").getResolvedSchema();
        LogicalType timestampType = schema.getColumnDataTypes().get(2).getLogicalType();
        assertTrue(timestampType instanceof TimestampType);
        assertEquals(TimestampKind.ROWTIME, ((TimestampType) timestampType).getKind());
    }

    @Test
    public void testConfiguration() throws Exception {
        final ExecutionContext context = createConfigurationExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        Configuration conf = tableEnv.getConfig().getConfiguration();
        assertEquals(100, conf.getInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT));
        assertTrue(conf.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED));
        assertEquals(
                "128 kb",
                conf.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                        .toString());

        assertTrue(conf.getBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED));

        // these options are not modified and should be equal to their default value
        assertEquals(
                ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED.defaultValue(),
                conf.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
        assertEquals(
                ExecutionOptions.SHUFFLE_MODE.defaultValue(),
                conf.get(ExecutionOptions.SHUFFLE_MODE));
        assertEquals(
                OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD
                        .defaultValue()
                        .longValue(),
                conf.getLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD));
    }

    @Test
    public void testInitCatalogs() throws Exception {
        final Map<String, String> replaceVars = createDefaultReplaceVars();
        Environment env = EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars);

        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("name", "test");
        catalogProps.put("type", "test_cl_catalog");
        env.getCatalogs().clear();
        env.getCatalogs().put("test", CatalogEntry.create(catalogProps));

        createExecutionContext(env);
    }

    private ExecutionContext createExecutionContext(String file, Map<String, String> replaceVars)
            throws Exception {
        final Environment env = EnvironmentFileUtil.parseModified(file, replaceVars);
        return createExecutionContext(env);
    }

    private ExecutionContext createExecutionContext(Environment env) throws Exception {
        final Configuration configuration = new Configuration();
        DefaultContext defaultContext =
                new DefaultContext(
                        env,
                        new ArrayList<>(),
                        configuration,
                        Collections.singletonList(new DefaultCLI()));
        sessionContext = SessionContext.create(defaultContext, "test-session");
        return sessionContext.getExecutionContext();
    }

    private Map<String, String> createDefaultReplaceVars() {
        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
        return replaceVars;
    }

    static Map<String, String> createModuleReplaceVars() {
        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        return replaceVars;
    }

    private ExecutionContext createDefaultExecutionContext() throws Exception {
        final Map<String, String> replaceVars = createDefaultReplaceVars();
        return createExecutionContext(DEFAULTS_ENVIRONMENT_FILE, replaceVars);
    }

    private ExecutionContext createModuleExecutionContext() throws Exception {
        return createExecutionContext(MODULES_ENVIRONMENT_FILE, createModuleReplaceVars());
    }

    private ExecutionContext createCatalogExecutionContext() throws Exception {
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        return createExecutionContext(CATALOGS_ENVIRONMENT_FILE, replaceVars);
    }

    private ExecutionContext createStreamingExecutionContext() throws Exception {
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_CONNECTOR_TYPE", DummyTableSourceFactory.CONNECTOR_TYPE_VALUE);
        replaceVars.put("$VAR_CONNECTOR_PROPERTY", DummyTableSourceFactory.TEST_PROPERTY);
        replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "");
        return createExecutionContext(STREAMING_ENVIRONMENT_FILE, replaceVars);
    }

    private ExecutionContext createExecutionExecutionContext() throws Exception {
        final Map<String, String> replaceVars = createDefaultReplaceVars();
        return createExecutionContext(EXECUTION_ENVIRONMENT_FILE, replaceVars);
    }

    private ExecutionContext createConfigurationExecutionContext() throws Exception {
        return createExecutionContext(CONFIGURATION_ENVIRONMENT_FILE, new HashMap<>());
    }

    private ExecutionContext createPythonFunctionExecutionContext() throws Exception {
        return createExecutionContext(FUNCTION_ENVIRONMENT_FILE, new HashMap<>());
    }

    // a catalog that requires the thread context class loader to be a user code classloader during
    // construction and opening
    private static class TestClassLoaderCatalog extends GenericInMemoryCatalog {

        private static final Class parentFirstCL =
                FlinkUserCodeClassLoaders.parentFirst(
                                new URL[0],
                                TestClassLoaderCatalog.class.getClassLoader(),
                                NOOP_EXCEPTION_HANDLER,
                                true)
                        .getClass();
        private static final Class childFirstCL =
                FlinkUserCodeClassLoaders.childFirst(
                                new URL[0],
                                TestClassLoaderCatalog.class.getClassLoader(),
                                new String[0],
                                NOOP_EXCEPTION_HANDLER,
                                true)
                        .getClass();

        TestClassLoaderCatalog(String name) {
            super(name);
            verifyUserClassLoader();
        }

        @Override
        public void open() {
            verifyUserClassLoader();
            super.open();
        }

        private void verifyUserClassLoader() {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            assertTrue(
                    parentFirstCL.isInstance(contextLoader)
                            || childFirstCL.isInstance(contextLoader));
        }
    }

    /** Factory to create TestClassLoaderCatalog. */
    public static class TestClassLoaderCatalogFactory implements CatalogFactory {

        @Override
        public String factoryIdentifier() {
            return "test_cl_catalog";
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.emptySet();
        }

        @Override
        public Catalog createCatalog(Context context) {
            return new TestClassLoaderCatalog("test_cl");
        }
    }
}
