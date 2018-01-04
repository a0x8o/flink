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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.TestLogger;

<<<<<<< HEAD
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
=======
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.junit.Assert;
import org.junit.Test;

/**
 * Test base for verifying that marshalling / unmarshalling REST {@link ResponseBody}s work properly.
 */
public abstract class RestResponseMarshallingTestBase<R extends ResponseBody> extends TestLogger {

	/**
	 * Returns the class of the test response.
	 *
	 * @return class of the test response type
	 */
	protected abstract Class<R> getTestResponseClass();

	/**
	 * Returns an instance of a response to be tested.
	 *
	 * @return instance of the expected test response
	 */
<<<<<<< HEAD
	protected abstract R getTestResponseInstance() throws Exception;
=======
	protected abstract R getTestResponseInstance();
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

	/**
	 * Tests that we can marshal and unmarshal the response.
	 */
	@Test
<<<<<<< HEAD
	public void testJsonMarshalling() throws Exception {
=======
	public void testJsonMarshalling() throws JsonProcessingException {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		final R expected = getTestResponseInstance();

		ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
		JsonNode json = objectMapper.valueToTree(expected);

		final R unmarshalled = objectMapper.treeToValue(json, getTestResponseClass());
		Assert.assertEquals(expected, unmarshalled);
	}

}
