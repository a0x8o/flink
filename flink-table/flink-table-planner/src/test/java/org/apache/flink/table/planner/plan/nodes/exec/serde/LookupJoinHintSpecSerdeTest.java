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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintSpecTest;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;

/** Serde tests for {@link LookupJoinHintSpec}. */
public class LookupJoinHintSpecSerdeTest {

    @Test
    void testJoinSpecSerde() throws IOException {
        LookupJoinHintSpec lookupJoinHintSpec =
                LookupJoinHintSpec.fromJoinHint(LookupJoinHintSpecTest.completeLookupHint);
        testJsonRoundTrip(lookupJoinHintSpec, LookupJoinHintSpec.class);

        lookupJoinHintSpec =
                LookupJoinHintSpec.fromJoinHint(LookupJoinHintSpecTest.lookupHintWithAsync);
        testJsonRoundTrip(lookupJoinHintSpec, LookupJoinHintSpec.class);

        lookupJoinHintSpec =
                LookupJoinHintSpec.fromJoinHint(LookupJoinHintSpecTest.lookupHintWithRetry);
        testJsonRoundTrip(lookupJoinHintSpec, LookupJoinHintSpec.class);

        lookupJoinHintSpec =
                LookupJoinHintSpec.fromJoinHint(LookupJoinHintSpecTest.lookupHintWithTableOnly);
        testJsonRoundTrip(lookupJoinHintSpec, LookupJoinHintSpec.class);
    }
}
