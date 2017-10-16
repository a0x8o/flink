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

package org.apache.flink.graph.drivers;

import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link EdgeList}.
 */
@RunWith(Parameterized.class)
public class EdgeListITCase extends NonTransformableDriverBaseITCase {

	public EdgeListITCase(String idType, TestExecutionMode mode) {
		super(idType, mode);
	}

	private String[] parameters(String input, String output, String... additionalParameters) {
		String[] parameters = new String[] {
			"--algorithm", "EdgeList",
			"--input", input, "--type", idType,
			"--output", output};

		return ArrayUtils.addAll(parameters, additionalParameters);
	}

	@Test
	public void testLongDescription() throws Exception {
		String expected = regexSubstring(new EdgeList().getLongDescription());

		expectedOutputFromException(
			new String[]{"--algorithm", "EdgeList"},
			expected,
			ProgramParametrizationException.class);
	}
<<<<<<< HEAD

	@Test
	public void testHashWithCirculantGraph() throws Exception {
		expectedChecksum(
			parameters("CirculantGraph", "hash", "--vertex_count", "42", "--range0", "13:4"),
			168, 0x000000000001ae80);
=======

	// CirculantGraph

	private String[] getCirculantGraphParameters(String output) {
		return parameters("CirculantGraph", output, "--vertex_count", "42", "--range0", "13:4");
	}

	@Test
	public void testHashWithCirculantGraph() throws Exception {
		expectedChecksum(getCirculantGraphParameters("hash"), 168, 0x000000000001ae80);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithCirculantGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getCirculantGraphParameters("print"), new Checksum(168, 0x0000004bdcc52cbcL));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithCompleteGraph() throws Exception {
		expectedChecksum(
			parameters("CompleteGraph", "hash", "--vertex_count", "42"),
			1722, 0x0000000000113ca0L);
=======
	public void testParallelismWithCirculantGraph() throws Exception {
		TestUtils.verifyParallelism(getCirculantGraphParameters("print"));
	}

	// CompleteGraph

	private String[] getCompleteGraphParameters(String output) {
		return parameters("CompleteGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithCompleteGraph() throws Exception {
		expectedChecksum(getCompleteGraphParameters("hash"), 1722, 0x0000000000113ca0L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithCompleteGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getCompleteGraphParameters("print"), new Checksum(1722, 0x0000031109a0c398L));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithCycleGraph() throws Exception {
		expectedChecksum(
			parameters("CycleGraph", "hash", "--vertex_count", "42"),
			84, 0x000000000000d740L);
=======
	public void testParallelismWithCompleteGraph() throws Exception {
		TestUtils.verifyParallelism(getCompleteGraphParameters("print"));
	}

	// CycleGraph

	private String[] getCycleGraphParameters(String output) {
		return parameters("CycleGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithCycleGraph() throws Exception {
		expectedChecksum(getCycleGraphParameters("hash"), 84, 0x000000000000d740L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithCycleGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getCycleGraphParameters("print"), new Checksum(84, 0x000000272a136fcaL));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithEchoGraph() throws Exception {
		expectedChecksum(
			parameters("EchoGraph", "hash", "--vertex_count", "42", "--vertex_degree", "13"),
			546, 0x0000000000057720L);
=======
	public void testParallelismWithCycleGraph() throws Exception {
		TestUtils.verifyParallelism(getCycleGraphParameters("print"));
	}

	// EchoGraph

	private String[] getEchoGraphParameters(String output) {
		return parameters("EchoGraph", output, "--vertex_count", "42", "--vertex_degree", "13");
	}

	@Test
	public void testHashWithEchoGraph() throws Exception {
		expectedChecksum(getEchoGraphParameters("hash"), 546, 0x0000000000057720L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithEchoGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getEchoGraphParameters("print"), new Checksum(546, 0x000000f7190b8fcaL));
	}

	@Test
	public void testParallelismWithEchoGraph() throws Exception {
		TestUtils.verifyParallelism(getEchoGraphParameters("print"));
	}

	// EmptyGraph

	private String[] getEmptyGraphParameters(String output) {
		return parameters("EmptyGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithEmptyGraph() throws Exception {
		expectedChecksum(getEmptyGraphParameters("hash"), 0, 0x0000000000000000L);
	}

	@Test
<<<<<<< HEAD
	public void testHashWithGridGraph() throws Exception {
		expectedChecksum(
			parameters("GridGraph", "hash", "--dim0", "2:true", "--dim1", "3:false", "--dim2", "5:true"),
			130, 0x000000000000eba0L);
=======
	public void testPrintWithEmptyGraph() throws Exception {
		expectedOutputChecksum(getEmptyGraphParameters("print"), new Checksum(0, 0x0000000000000000L));
	}

	@Test
	public void testParallelismWithEmptyGraph() throws Exception {
		TestUtils.verifyParallelism(getEmptyGraphParameters("print"));
	}

	// GridGraph

	private String[] getGridGraphParameters(String output) {
		return parameters("GridGraph", output, "--dim0", "2:true", "--dim1", "3:false", "--dim2", "5:true");
	}

	@Test
	public void testHashWithGridGraph() throws Exception {
		expectedChecksum(getGridGraphParameters("hash"), 130, 0x000000000000eba0L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithGridGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getGridGraphParameters("print"), new Checksum(130, 0x00000033237d24eeL));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithHypercubeGraph() throws Exception {
		expectedChecksum(
			parameters("HypercubeGraph", "hash", "--dimensions", "7"),
			896, 0x00000000001bc800L);
=======
	public void testParallelismWithGridGraph() throws Exception {
		TestUtils.verifyParallelism(getGridGraphParameters("print"));
	}

	// HypercubeGraph

	private String[] getHypercubeGraphParameters(String output) {
		return parameters("HypercubeGraph", output, "--dimensions", "7");
	}

	@Test
	public void testHashWithHypercubeGraph() throws Exception {
		expectedChecksum(getHypercubeGraphParameters("hash"), 896, 0x00000000001bc800L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithHypercubeGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getHypercubeGraphParameters("print"), new Checksum(896, 0x000001f243ee33b2L));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithPathGraph() throws Exception {
		expectedChecksum(
			parameters("PathGraph", "hash", "--vertex_count", "42"),
			82, 0x000000000000d220L);
=======
	public void testParallelismWithHypercubeGraph() throws Exception {
		TestUtils.verifyParallelism(getHypercubeGraphParameters("print"));
	}

	// PathGraph

	private String[] getPathGraphParameters(String output) {
		return parameters("PathGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithPathGraph() throws Exception {
		expectedChecksum(getPathGraphParameters("hash"), 82, 0x000000000000d220L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithPathGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getPathGraphParameters("print"), new Checksum(82, 0x000000269be2d4c2L));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithRMatGraph() throws Exception {
		expectedChecksum(
			parameters("RMatGraph", "hash", "--scale", "7"),
			2048, 0x00000000001ee529);
=======
	public void testParallelismWithPathGraph() throws Exception {
		TestUtils.verifyParallelism(getPathGraphParameters("print"));
	}

	// RMatGraph

	private String[] getRMatGraphParameters(String output, String simplify) {
		if (simplify == null) {
			return parameters("RMatGraph", output, "--scale", "7");
		} else {
			return parameters("RMatGraph", output, "--scale", "7", "--simplify", simplify);
		}
	}

	@Test
	public void testHashWithRMatGraph() throws Exception {
		expectedChecksum(getRMatGraphParameters("hash", null), 2048, 0x00000000001ee529);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getRMatGraphParameters("print", null), new Checksum(2048, 0x000002f737939f05L));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithDirectedRMatGraph() throws Exception {
		expectedChecksum(
			parameters("RMatGraph", "hash", "--scale", "7", "--simplify", "directed"),
			1168, 0x00000000001579bdL);
=======
	public void testParallelismWithRMatGraph() throws Exception {
		TestUtils.verifyParallelism(getRMatGraphParameters("print", null));
	}

	@Test
	public void testHashWithDirectedRMatGraph() throws Exception {
		expectedChecksum(getRMatGraphParameters("hash", "directed"), 1168, 0x00000000001579bdL);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithDirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getRMatGraphParameters("print", "directed"), new Checksum(1168, 0x0000020e35b0f35dL));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithUndirectedRMatGraph() throws Exception {
		expectedChecksum(
			parameters("RMatGraph", "hash", "--scale", "7", "--simplify", "undirected"),
			1854, 0x0000000000242920L);
=======
	public void testParallelismWithDirectedRMatGraph() throws Exception {
		TestUtils.verifyParallelism(getRMatGraphParameters("print", "directed"));
	}

	@Test
	public void testHashWithUndirectedRMatGraph() throws Exception {
		expectedChecksum(getRMatGraphParameters("hash", "undirected"), 1854, 0x0000000000242920L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithUndirectedRMatGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getRMatGraphParameters("print", "undirected"), new Checksum(1854, 0x0000036fe5802162L));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithSingletonEdgeGraph() throws Exception {
		expectedChecksum(
			parameters("SingletonEdgeGraph", "hash", "--vertex_pair_count", "42"),
			84, 0x000000000001b3c0L);
=======
	public void testParallelismWithUndirectedRMatGraph() throws Exception {
		TestUtils.verifyParallelism(getRMatGraphParameters("print", "undirected"));
	}

	// SingletonEdgeGraph

	private String[] getSingletonEdgeGraphParameters(String output) {
		return parameters("SingletonEdgeGraph", output, "--vertex_pair_count", "42");
	}

	@Test
	public void testHashWithSingletonEdgeGraph() throws Exception {
		expectedChecksum(getSingletonEdgeGraphParameters("hash"), 84, 0x000000000001b3c0L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithSingletonEdgeGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getSingletonEdgeGraphParameters("print"), new Checksum(84, 0x0000002e59e10d9aL));
	}

	@Test
<<<<<<< HEAD
	public void testHashWithStarGraph() throws Exception {
		expectedChecksum(
			parameters("StarGraph", "hash", "--vertex_count", "42"),
			82, 0x0000000000006ba0L);
=======
	public void testParallelismWithSingletonEdgeGraph() throws Exception {
		TestUtils.verifyParallelism(getSingletonEdgeGraphParameters("print"));
	}

	// StarGraph

	private String[] getStarGraphParameters(String output) {
		return parameters("StarGraph", output, "--vertex_count", "42");
	}

	@Test
	public void testHashWithStarGraph() throws Exception {
		expectedChecksum(getStarGraphParameters("hash"), 82, 0x0000000000006ba0L);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	@Test
	public void testPrintWithStarGraph() throws Exception {
		// skip 'char' since it is not printed as a number
		Assume.assumeFalse(idType.equals("char") || idType.equals("nativeChar"));

		expectedOutputChecksum(getStarGraphParameters("print"), new Checksum(82, 0x00000011ec3faee8L));
	}

	@Test
	public void testParallelismWithStarGraph() throws Exception {
		TestUtils.verifyParallelism(getStarGraphParameters("print"));
	}
}
