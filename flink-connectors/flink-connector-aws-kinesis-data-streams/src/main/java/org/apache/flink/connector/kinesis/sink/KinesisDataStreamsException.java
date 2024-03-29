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

package org.apache.flink.connector.kinesis.sink;

/**
 * A {@link RuntimeException} wrapper indicating the exception was thrown from the Kinesis Data
 * Streams Sink.
 */
class KinesisDataStreamsException extends RuntimeException {

    public KinesisDataStreamsException(final String message) {
        super(message);
    }

    public KinesisDataStreamsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * When the flag {@code failOnError} is set in {@link KinesisDataStreamsSinkWriter}, this
     * exception is raised as soon as any exception occurs when KDS is written to.
     */
    static class KinesisDataStreamsFailFastException extends KinesisDataStreamsException {

        private static final String ERROR_MESSAGE =
                "Encountered an exception while persisting records, not retrying due to {failOnError} being set.";

        public KinesisDataStreamsFailFastException() {
            super(ERROR_MESSAGE);
        }

        public KinesisDataStreamsFailFastException(final Throwable cause) {
            super(ERROR_MESSAGE, cause);
        }
    }
}
