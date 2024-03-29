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

package org.apache.flink.connector.file.table.batch.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.stream.compact.CompactContext;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.connector.file.table.stream.compact.CompactWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for compacting files. */
public class CompactFileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompactFileUtils.class);

    /**
     * Do Compaction: - Target file exists, do nothing. Otherwise, it'll read the input files and
     * write the target file to achieve compaction purpose.
     */
    public static @Nullable <T> Path doCompact(
            FileSystem fileSystem,
            String partition,
            List<Path> paths,
            Path target,
            Configuration config,
            CompactReader.Factory<T> readerFactory,
            CompactWriter.Factory<T> writerFactory)
            throws IOException {
        if (paths.size() == 0) {
            return null;
        }

        if (fileSystem.exists(target)) {
            return target;
        }

        checkExist(fileSystem, paths);

        long startMillis = System.currentTimeMillis();

        Map<Path, Long> inputMap = new HashMap<>();
        for (Path path : paths) {
            inputMap.put(path, fileSystem.getFileStatus(path).getLen());
        }

        doMultiFilesCompact(
                partition, paths, target, config, fileSystem, readerFactory, writerFactory);
        Map<Path, Long> targetMap = new HashMap<>();
        targetMap.put(target, fileSystem.getFileStatus(target).getLen());
        double costSeconds = ((double) (System.currentTimeMillis() - startMillis)) / 1000;
        LOG.info(
                "Compaction time cost is '{}S', output per file as following format: name=size(byte), target file is '{}', input files are '{}'",
                costSeconds,
                targetMap,
                inputMap);
        return target;
    }

    private static <T> void doMultiFilesCompact(
            String partition,
            List<Path> files,
            Path dst,
            Configuration config,
            FileSystem fileSystem,
            CompactReader.Factory<T> readerFactory,
            CompactWriter.Factory<T> writerFactory)
            throws IOException {
        CompactWriter<T> writer =
                writerFactory.create(CompactContext.create(config, fileSystem, partition, dst));

        for (Path path : files) {
            try (CompactReader<T> reader =
                    readerFactory.create(
                            CompactContext.create(config, fileSystem, partition, path))) {
                T record;
                while ((record = reader.read()) != null) {
                    writer.write(record);
                }
            }
        }

        // commit immediately
        writer.commit();
    }

    private static void checkExist(FileSystem fileSystem, List<Path> candidates)
            throws IOException {
        for (Path path : candidates) {
            if (!fileSystem.exists(path)) {
                throw new IOException("Compaction file not exist: " + path);
            }
        }
    }
}
