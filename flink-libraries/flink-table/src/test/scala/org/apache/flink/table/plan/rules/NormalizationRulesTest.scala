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
package org.apache.flink.table.plan.rules

import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule
import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class NormalizationRulesTest extends TableTestBase {

  @Test
  def testApplyNormalizationRuleForBatchSQL(): Unit = {
    val util = batchTestUtil()

    // rewrite distinct aggregate
    val cc: CalciteConfig = new CalciteConfigBuilder()
        .replaceNormRuleSet(RuleSets.ofList(AggregateExpandDistinctAggregatesRule.JOIN))
        .replaceLogicalOptRuleSet(RuleSets.ofList())
        .replacePhysicalOptRuleSet(RuleSets.ofList())
        .build()
    util.tEnv.getConfig.setCalciteConfig(cc)

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT " +
      "COUNT(DISTINCT a)" +
      "FROM MyTable group by b"

    // expect double aggregate
    val expected = unaryNode("LogicalProject",
      unaryNode("LogicalAggregate",
        unaryNode("LogicalAggregate",
          unaryNode("LogicalProject",
            values("LogicalTableScan", term("table", "[_DataSetTable_0]")),
            term("b", "$1"), term("a", "$0")),
          term("group", "{0, 1}")),
        term("group", "{0}"), term("EXPR$0", "COUNT($1)")
      ),
      term("EXPR$0", "$1")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testApplyNormalizationRuleForStreamSQL(): Unit = {
    val util = streamTestUtil()

    // rewrite distinct aggregate
    val cc: CalciteConfig = new CalciteConfigBuilder()
        .replaceNormRuleSet(RuleSets.ofList(AggregateExpandDistinctAggregatesRule.JOIN))
        .replaceLogicalOptRuleSet(RuleSets.ofList())
        .replacePhysicalOptRuleSet(RuleSets.ofList())
        .build()
    util.tEnv.getConfig.setCalciteConfig(cc)

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT " +
      "COUNT(DISTINCT a)" +
      "FROM MyTable group by b"

    // expect double aggregate
    val expected = unaryNode(
      "LogicalProject",
      unaryNode("LogicalAggregate",
        unaryNode("LogicalAggregate",
          unaryNode("LogicalProject",
            values("LogicalTableScan", term("table", "[_DataStreamTable_0]")),
            term("b", "$1"), term("a", "$0")),
          term("group", "{0, 1}")),
        term("group", "{0}"), term("EXPR$0", "COUNT($1)")
      ),
      term("EXPR$0", "$1")
    )

    util.verifySql(sqlQuery, expected)
  }

}
