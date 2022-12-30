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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.security.token.DelegationTokenContainer;
import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delegation token updater functionality. */
@Internal
public final class HadoopDelegationTokenUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopDelegationTokenUpdater.class);

    private HadoopDelegationTokenUpdater() {}

    /** Updates delegation tokens for the current user. */
    public static void addCurrentUserCredentials(byte[] containerBytes) throws Exception {
        if (containerBytes == null || containerBytes.length == 0) {
            throw new IllegalArgumentException("Illegal container tried to be processed");
        }
        DelegationTokenContainer container =
                InstantiationUtil.deserializeObject(
                        containerBytes, HadoopDelegationTokenUpdater.class.getClassLoader());
        Credentials credentials = new Credentials();
        for (byte[] v : container.getTokens().values()) {
            credentials.addAll(HadoopDelegationTokenConverter.deserialize(v));
        }
        LOG.info("Updating delegation tokens for current user");
        dumpAllTokens(credentials);
        UserGroupInformation.getCurrentUser().addCredentials(credentials);
        LOG.info("Updated delegation tokens for current user successfully");
    }

    public static void dumpAllTokens(Credentials credentials) {
        credentials
                .getAllTokens()
                .forEach(
                        token ->
                                LOG.info(
                                        "Token Service:{} Identifier:{}",
                                        token.getService(),
                                        token.getIdentifier()));
    }
}
