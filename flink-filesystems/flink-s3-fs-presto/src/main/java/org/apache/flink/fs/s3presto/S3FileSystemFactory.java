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

package org.apache.flink.fs.s3presto;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.runtime.util.HadoopConfigLoader;
import org.apache.flink.util.FlinkRuntimeException;

import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import org.apache.hadoop.fs.FileSystem;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

/** Simple factory for the S3 file system. */
public class S3FileSystemFactory extends AbstractS3FileSystemFactory {

    private static final String[] FLINK_CONFIG_PREFIXES = {"s3.", "presto.s3."};

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"presto.s3.access.key", "presto.s3.access-key"},
        {"presto.s3.secret.key", "presto.s3.secret-key"},
        {"presto.s3.path.style.access", "presto.s3.path-style-access"}
    };

    public S3FileSystemFactory() {
        super("Presto S3 File System", createHadoopConfigLoader());
    }

    @Override
    public String getScheme() {
        return "s3";
    }

    @VisibleForTesting
    static HadoopConfigLoader createHadoopConfigLoader() {
        return new HadoopConfigLoader(
                FLINK_CONFIG_PREFIXES,
                MIRRORED_CONFIG_KEYS,
                "presto.s3.",
                Collections.emptySet(),
                Collections.emptySet(),
                "");
    }

    @Override
    protected org.apache.flink.core.fs.FileSystem createFlinkFileSystem(
            FileSystem fs,
            @Nullable FlinkS3FileSystem.S5CmdConfiguration s5CmdConfiguration,
            String localTmpDirectory,
            String entropyInjectionKey,
            int numEntropyChars,
            S3AccessHelper s3AccessHelper,
            long s3minPartSize,
            int maxConcurrentUploads) {
        return new FlinkS3PrestoFileSystem(
                fs,
                s5CmdConfiguration,
                localTmpDirectory,
                entropyInjectionKey,
                numEntropyChars,
                s3AccessHelper,
                s3minPartSize,
                maxConcurrentUploads);
    }

    @Override
    protected org.apache.hadoop.fs.FileSystem createHadoopFileSystem() {
        return new PrestoS3FileSystem();
    }

    @Override
    protected URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
        final String scheme = fsUri.getScheme();
        final String authority = fsUri.getAuthority();
        final URI initUri;

        if (scheme == null && authority == null) {
            initUri = createURI("s3://s3.amazonaws.com");
        } else if (scheme != null && authority == null) {
            initUri = createURI(scheme + "://s3.amazonaws.com");
        } else {
            initUri = fsUri;
        }
        return initUri;
    }

    @Nullable
    @Override
    protected S3AccessHelper getS3AccessHelper(FileSystem fs) {
        return null;
    }

    private URI createURI(String str) {
        try {
            return new URI(str);
        } catch (URISyntaxException e) {
            throw new FlinkRuntimeException("Error in s3 aws URI - " + str, e);
        }
    }
}
