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

package org.apache.flink.runtime.scheduler.adaptivebatch.forwardgroup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Common utils for computing forward groups. */
public class ForwardGroupComputeUtil {

    public static Map<JobVertexID, ForwardGroup>
            computeForwardGroupsAndSetVertexParallelismsIfNecessary(
                    final Iterable<JobVertex> topologicallySortedVertices) {
        final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId =
                computeForwardGroups(topologicallySortedVertices);
        // set parallelism for vertices in parallelism-decided forward groups
        topologicallySortedVertices.forEach(
                jobVertex -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    if (jobVertex.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT
                            && forwardGroup != null
                            && forwardGroup.isParallelismDecided()) {
                        jobVertex.setParallelism(forwardGroup.getParallelism());
                    }
                });
        return forwardGroupsByJobVertexId;
    }

    static Map<JobVertexID, ForwardGroup> computeForwardGroups(
            final Iterable<JobVertex> topologicallySortedVertices) {

        final Map<JobVertex, Set<JobVertex>> vertexToGroup = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (JobVertex vertex : topologicallySortedVertices) {
            Set<JobVertex> currentGroup = new HashSet<>();
            currentGroup.add(vertex);
            vertexToGroup.put(vertex, currentGroup);

            for (JobEdge input : getForwardInputs(vertex)) {
                final JobVertex producerVertex = input.getSource().getProducer();
                final Set<JobVertex> producerGroup = vertexToGroup.get(producerVertex);

                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getID()
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + vertex.getID()
                                    + ". This should be a forward group building bug.");
                }

                if (currentGroup != producerGroup) {
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, vertexToGroup);
                }
            }
        }

        final Map<JobVertexID, ForwardGroup> ret = new HashMap<>();
        for (Set<JobVertex> vertexGroup :
                VertexGroupComputeUtil.uniqueVertexGroups(vertexToGroup)) {
            if (vertexGroup.size() > 1) {
                ForwardGroup forwardGroup = new ForwardGroup(vertexGroup);
                for (JobVertexID jobVertexId : forwardGroup.getJobVertexIds()) {
                    ret.put(jobVertexId, forwardGroup);
                }
            }
        }
        return ret;
    }

    static Iterable<JobEdge> getForwardInputs(JobVertex jobVertex) {
        return jobVertex.getInputs().stream()
                .filter(JobEdge::isForward)
                .collect(Collectors.toSet());
    }
}
