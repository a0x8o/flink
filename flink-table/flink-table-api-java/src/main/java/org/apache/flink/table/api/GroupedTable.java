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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.Expression;

/**
 * A table that has been grouped on a set of grouping keys.
 */
@PublicEvolving
public interface GroupedTable {

	/**
	 * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy("key").select("key, value.avg + ' The average' as average")
	 * }
	 * </pre>
	 */
	Table select(String fields);

	/**
	 * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
	 * The field expressions can contain complex expressions and aggregations.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy('key).select('key, 'value.avg + " The average" as 'average)
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);

	/**
	 * Performs a flatAggregate operation on a grouped table. FlatAggregate takes a
	 * TableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc: TableAggregateFunction = new MyTableAggregateFunction
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   tab.groupBy("key")
	 *     .flatAggregate("tableAggFunc(a, b) as (x, y, z)")
	 *     .select("key, x, y, z")
	 * }
	 * </pre>
	 */
	FlatAggregateTable flatAggregate(String tableAggFunction);

	/**
	 * Performs a flatAggregate operation on a grouped table. FlatAggregate takes a
	 * TableAggregateFunction which returns multiple rows. Use a selection after flatAggregate.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc: TableAggregateFunction = new MyTableAggregateFunction
	 *   tab.groupBy('key)
	 *     .flatAggregate(tableAggFunc('a, 'b) as ('x, 'y, 'z))
	 *     .select('key, 'x, 'y, 'z)
	 * }
	 * </pre>
	 */
	FlatAggregateTable flatAggregate(Expression tableAggFunction);
}
