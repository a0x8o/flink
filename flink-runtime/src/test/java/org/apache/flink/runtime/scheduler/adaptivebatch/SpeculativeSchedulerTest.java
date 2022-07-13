/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultExecutionOperations;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.TestExecutionOperationsDecorator;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.completeCancellingForAllVertices;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.scheduler.DefaultSchedulerTest.createFailedTaskExecutionState;
import static org.apache.flink.runtime.scheduler.DefaultSchedulerTest.singleNonParallelJobVertexJobGraph;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpeculativeScheduler}. */
class SpeculativeSchedulerTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ScheduledExecutorService futureExecutor;

    private ManuallyTriggeredScheduledExecutor taskRestartExecutor;
    private TestExecutionOperationsDecorator testExecutionOperations;
    private TestBlocklistOperations testBlocklistOperations;
    private TestRestartBackoffTimeStrategy restartStrategy;

    @BeforeEach
    void setUp() {
        futureExecutor = new DirectScheduledExecutorService();

        taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();
        testExecutionOperations =
                new TestExecutionOperationsDecorator(new DefaultExecutionOperations());
        testBlocklistOperations = new TestBlocklistOperations();
        restartStrategy = new TestRestartBackoffTimeStrategy(true, 0);
    }

    @AfterEach
    void tearDown() {
        if (futureExecutor != null) {
            ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, futureExecutor);
        }
    }

    @Test
    void testStartScheduling() {
        createSchedulerAndStartScheduling();
        final List<ExecutionAttemptID> deployedExecutions =
                testExecutionOperations.getDeployedExecutions();
        assertThat(deployedExecutions).hasSize(1);
    }

    @Test
    void testNotifySlowTasks() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(1);

        notifySlowTask(scheduler, attempt1);

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(2);
        assertThat(testBlocklistOperations.getAllBlockedNodeIds())
                .containsExactly(attempt1.getAssignedResourceLocation().getNodeId());
    }

    @Test
    void testNotifyDuplicatedSlowTasks() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(2);

        // notify the execution as a slow task again
        notifySlowTask(scheduler, attempt1);

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(2);

        // fail attempt2 to make room for a new speculative execution
        final Execution attempt2 = getExecution(ev, 1);
        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attempt2.getAttemptId()));

        // notify the execution as a slow task again
        notifySlowTask(scheduler, attempt1);

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(3);
    }

    @Test
    void testRestartVertexIfAllSpeculativeExecutionFailed() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(2);

        final ExecutionAttemptID attemptId1 = attempt1.getAttemptId();
        final ExecutionAttemptID attemptId2 = getExecution(ev, 1).getAttemptId();

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId1));
        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId2));
        taskRestartExecutor.triggerScheduledTasks();

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(3);
    }

    @Test
    void testNoRestartIfNotAllSpeculativeExecutionFailed() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);
        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attempt1.getAttemptId()));
        taskRestartExecutor.triggerScheduledTasks();

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(2);
    }

    @Test
    void testRestartVertexIfPartitionExceptionHappened() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);
        final Execution attempt2 = getExecution(ev, 1);
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attempt1.getAttemptId(),
                        ExecutionState.FAILED,
                        new PartitionNotFoundException(new ResultPartitionID())));

        assertThat(attempt2.getState()).isEqualTo(ExecutionState.CANCELING);

        completeCancellingForAllVertices(scheduler.getExecutionGraph());
        taskRestartExecutor.triggerScheduledTasks();

        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(3);
    }

    @Test
    void testCancelOtherCurrentExecutionsWhenAnyExecutionFinished() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);
        final Execution attempt2 = getExecution(ev, 1);
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(attempt1.getAttemptId(), ExecutionState.FINISHED));

        assertThat(attempt2.getState()).isEqualTo(ExecutionState.CANCELING);
    }

    @Test
    void testExceptionHistoryIfPartitionExceptionHappened() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);

        // A partition exception can result in a restart of the whole execution vertex.
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attempt1.getAttemptId(),
                        ExecutionState.FAILED,
                        new PartitionNotFoundException(new ResultPartitionID())));

        completeCancellingForAllVertices(scheduler.getExecutionGraph());
        taskRestartExecutor.triggerScheduledTasks();

        assertThat(scheduler.getExceptionHistory()).hasSize(1);

        final RootExceptionHistoryEntry entry = scheduler.getExceptionHistory().iterator().next();
        // the current execution attempt before the restarting should be attempt2 but the failure
        // root exception should be attempt1
        assertThat(entry.getFailingTaskName()).isEqualTo(attempt1.getVertexWithAttempt());
    }

    @Test
    void testLocalExecutionAttemptFailureIsCorrectlyRecorded() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);

        // the execution vertex will not be restarted if we only fails attempt1, but it still should
        // be recorded in the execution graph and in exception history
        final TaskExecutionState failedState =
                createFailedTaskExecutionState(attempt1.getAttemptId());
        scheduler.updateTaskExecutionState(failedState);

        final ClassLoader classLoader = SpeculativeSchedulerTest.class.getClassLoader();
        assertThat(scheduler.getExecutionGraph().getFailureInfo()).isNotNull();
        assertThat(scheduler.getExecutionGraph().getFailureInfo().getExceptionAsString())
                .contains(failedState.getError(classLoader).getMessage());

        assertThat(scheduler.getExceptionHistory()).hasSize(1);

        final RootExceptionHistoryEntry entry = scheduler.getExceptionHistory().iterator().next();
        assertThat(entry.getFailingTaskName()).isEqualTo(attempt1.getVertexWithAttempt());
    }

    @Test
    void testUnrecoverableLocalExecutionAttemptFailureWillFailJob() {
        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);

        final TaskExecutionState failedState =
                new TaskExecutionState(
                        attempt1.getAttemptId(),
                        ExecutionState.FAILED,
                        new SuppressRestartsException(
                                new Exception("Forced failure for testing.")));
        scheduler.updateTaskExecutionState(failedState);

        assertThat(scheduler.getExecutionGraph().getState()).isEqualTo(JobStatus.FAILING);
    }

    @Test
    void testLocalExecutionAttemptFailureAndForbiddenRestartWillFailJob() {
        restartStrategy.setCanRestart(false);

        final SpeculativeScheduler scheduler = createSchedulerAndStartScheduling();
        final ExecutionVertex ev = getOnlyExecutionVertex(scheduler);
        final Execution attempt1 = ev.getCurrentExecutionAttempt();

        notifySlowTask(scheduler, attempt1);

        final TaskExecutionState failedState =
                createFailedTaskExecutionState(attempt1.getAttemptId());
        scheduler.updateTaskExecutionState(failedState);

        assertThat(scheduler.getExecutionGraph().getState()).isEqualTo(JobStatus.FAILING);
    }

    @Test
    public void testSpeculativeExecutionCombinedWithAdaptiveScheduling() throws Exception {
        final JobVertex source = createNoOpVertex("source", 1);
        final JobVertex sink = createNoOpVertex("sink", -1);
        sink.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(source, sink);

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forMainThread();
        final SpeculativeScheduler scheduler =
                createSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setVertexParallelismDecider(ignore -> 3)
                        .buildSpeculativeScheduler();
        mainThreadExecutor.execute(scheduler::startScheduling);

        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        final ExecutionJobVertex sourceExecutionJobVertex = graph.getJobVertex(source.getID());
        final ExecutionJobVertex sinkExecutionJobVertex = graph.getJobVertex(sink.getID());

        final ExecutionVertex sourceExecutionVertex = sourceExecutionJobVertex.getTaskVertices()[0];
        assertThat(sourceExecutionVertex.getCurrentExecutions()).hasSize(1);

        // trigger source vertex speculation
        final Execution sourceAttempt1 = sourceExecutionVertex.getCurrentExecutionAttempt();
        notifySlowTask(scheduler, sourceAttempt1);
        assertThat(sourceExecutionVertex.getCurrentExecutions()).hasSize(2);

        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(-1);

        // Finishing any source execution attempt will finish the source execution vertex, and then
        // finish the job vertex.
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        sourceAttempt1.getAttemptId(),
                        ExecutionState.FINISHED,
                        null,
                        null,
                        new IOMetrics(0, 0, 0, 0, 0, 0, 0)));
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(3);

        // trigger sink vertex speculation
        final ExecutionVertex sinkExecutionVertex = sinkExecutionJobVertex.getTaskVertices()[0];
        final Execution sinkAttempt1 = sinkExecutionVertex.getCurrentExecutionAttempt();
        notifySlowTask(scheduler, sinkAttempt1);
        assertThat(sinkExecutionVertex.getCurrentExecutions()).hasSize(2);
    }

    private static Execution getExecution(ExecutionVertex executionVertex, int attemptNumber) {
        return executionVertex.getCurrentExecutions().stream()
                .filter(e -> e.getAttemptNumber() == attemptNumber)
                .findFirst()
                .get();
    }

    private static ExecutionVertex getOnlyExecutionVertex(SpeculativeScheduler scheduler) {
        return Iterables.getOnlyElement(scheduler.getExecutionGraph().getAllExecutionVertices());
    }

    private SpeculativeScheduler createSchedulerAndStartScheduling() {
        return createSchedulerAndStartScheduling(singleNonParallelJobVertexJobGraph());
    }

    private SpeculativeScheduler createSchedulerAndStartScheduling(final JobGraph jobGraph) {
        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forMainThread();

        try {
            final SpeculativeScheduler scheduler = createScheduler(jobGraph, mainThreadExecutor);
            mainThreadExecutor.execute(scheduler::startScheduling);
            return scheduler;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SpeculativeScheduler createScheduler(
            final JobGraph jobGraph, final ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        return createSchedulerBuilder(jobGraph, mainThreadExecutor).buildSpeculativeScheduler();
    }

    private DefaultSchedulerBuilder createSchedulerBuilder(
            final JobGraph jobGraph, final ComponentMainThreadExecutor mainThreadExecutor) {
        return new DefaultSchedulerBuilder(
                        jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                .setBlocklistOperations(testBlocklistOperations)
                .setExecutionOperations(testExecutionOperations)
                .setFutureExecutor(futureExecutor)
                .setDelayExecutor(taskRestartExecutor)
                .setRestartBackoffTimeStrategy(restartStrategy);
    }

    private static void notifySlowTask(
            final SpeculativeScheduler scheduler, final Execution slowTask) {
        scheduler.notifySlowTasks(
                ImmutableMap.of(
                        slowTask.getVertex().getID(),
                        Collections.singleton(slowTask.getAttemptId())));
    }

    private static class TestBlocklistOperations implements BlocklistOperations {
        private final List<BlockedNode> blockedNodes = new ArrayList<>();

        @Override
        public void addNewBlockedNodes(Collection<BlockedNode> newNodes) {
            blockedNodes.addAll(newNodes);
        }

        public Set<String> getAllBlockedNodeIds() {
            return blockedNodes.stream().map(BlockedNode::getNodeId).collect(Collectors.toSet());
        }
    }
}
