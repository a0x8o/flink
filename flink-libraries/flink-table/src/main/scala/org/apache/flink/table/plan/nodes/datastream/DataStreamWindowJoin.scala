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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
<<<<<<< HEAD
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
=======
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.UpdatingPlanChecker
<<<<<<< HEAD
import org.apache.flink.table.runtime.join.{ProcTimeBoundedStreamInnerJoin, RowTimeBoundedStreamInnerJoin, WindowJoinUtil}
import org.apache.flink.table.runtime.operators.KeyedCoProcessOperatorWithWatermarkDelay
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector
=======
import org.apache.flink.table.runtime.join.{ProcTimeWindowInnerJoin, WindowJoinUtil}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

/**
  * RelNode for a time windowed stream join.
  */
class DataStreamWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinType: JoinRelType,
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    schema: RowSchema,
    isRowTime: Boolean,
    leftLowerBound: Long,
    leftUpperBound: Long,
<<<<<<< HEAD
    leftTimeIdx: Int,
    rightTimeIdx: Int,
=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    remainCondition: Option[RexNode],
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
    with CommonJoin
<<<<<<< HEAD
    with DataStreamRel
    with Logging {
=======
    with DataStreamRel {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamWindowJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      leftSchema,
      rightSchema,
      schema,
      isRowTime,
      leftLowerBound,
      leftUpperBound,
<<<<<<< HEAD
      leftTimeIdx,
      rightTimeIdx,
=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      remainCondition,
      ruleDescription)
  }

  override def toString: String = {
    joinToString(
      schema.relDataType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    joinExplainTerms(
      super.explainTerms(pw),
      schema.relDataType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig

    val isLeftAppendOnly = UpdatingPlanChecker.isAppendOnly(left)
    val isRightAppendOnly = UpdatingPlanChecker.isAppendOnly(right)
    if (!isLeftAppendOnly || !isRightAppendOnly) {
      throw new TableException(
        "Windowed stream join does not support updates.")
    }

    val leftDataStream = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val rightDataStream = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

<<<<<<< HEAD
    // get the equi-keys and other conditions
    val joinInfo = JoinInfo.of(leftNode, rightNode, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray
    val relativeWindowSize = leftUpperBound - leftLowerBound
    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)
=======
    // get the equality keys and other condition
    val joinInfo = JoinInfo.of(leftNode, rightNode, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

    // generate join function
    val joinFunction =
    WindowJoinUtil.generateJoinFunction(
      config,
      joinType,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      schema,
      remainCondition,
      ruleDescription)

<<<<<<< HEAD
    val joinOpName =
      s"where: (" +
        s"${joinConditionToString(schema.relDataType, joinCondition, getExpressionString)}), " +
        s"join: (${joinSelectionToString(schema.relDataType)})"

    joinType match {
      case JoinRelType.INNER =>
        if (relativeWindowSize < 0) {
          LOG.warn(s"The relative window size $relativeWindowSize is negative," +
            " please check the join conditions.")
          createEmptyInnerJoin(leftDataStream, rightDataStream, returnTypeInfo)
        } else {
          if (isRowTime) {
            createRowTimeInnerJoin(
              leftDataStream,
              rightDataStream,
              returnTypeInfo,
              joinOpName,
              joinFunction.name,
              joinFunction.code,
              leftKeys,
              rightKeys
            )
          } else {
            createProcTimeInnerJoin(
              leftDataStream,
              rightDataStream,
              returnTypeInfo,
              joinOpName,
              joinFunction.name,
              joinFunction.code,
              leftKeys,
              rightKeys
            )
          }
=======
    joinType match {
      case JoinRelType.INNER =>
        if (isRowTime) {
          // RowTime JoinCoProcessFunction
          throw new TableException(
            "RowTime inner join between stream and stream is not supported yet.")
        } else {
          // Proctime JoinCoProcessFunction
          createProcTimeInnerJoinFunction(
            leftDataStream,
            rightDataStream,
            joinFunction.name,
            joinFunction.code,
            leftKeys,
            rightKeys
          )
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
        }
      case JoinRelType.FULL =>
        throw new TableException(
          "Full join between stream and stream is not supported yet.")
      case JoinRelType.LEFT =>
        throw new TableException(
          "Left join between stream and stream is not supported yet.")
      case JoinRelType.RIGHT =>
        throw new TableException(
          "Right join between stream and stream is not supported yet.")
    }
  }

<<<<<<< HEAD
  def createEmptyInnerJoin(
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      returnTypeInfo: TypeInformation[CRow]): DataStream[CRow] = {
    leftDataStream.connect(rightDataStream).process(
      new CoProcessFunction[CRow, CRow, CRow] {
        override def processElement1(
          value: CRow,
          ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
          out: Collector[CRow]): Unit = {
          //Do nothing.
        }
        override def processElement2(
          value: CRow,
          ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
          out: Collector[CRow]): Unit = {
          //Do nothing.
        }
      }).returns(returnTypeInfo)
  }

  def createProcTimeInnerJoin(
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      returnTypeInfo: TypeInformation[CRow],
      operatorName: String,
=======
  def createProcTimeInnerJoinFunction(
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): DataStream[CRow] = {

<<<<<<< HEAD
    val procInnerJoinFunc = new ProcTimeBoundedStreamInnerJoin(
=======
    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)

    val procInnerJoinFunc = new ProcTimeWindowInnerJoin(
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      leftLowerBound,
      leftUpperBound,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      joinFunctionName,
      joinFunctionCode)

    if (!leftKeys.isEmpty) {
      leftDataStream.connect(rightDataStream)
        .keyBy(leftKeys, rightKeys)
        .process(procInnerJoinFunc)
<<<<<<< HEAD
        .name(operatorName)
=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
        .returns(returnTypeInfo)
    } else {
      leftDataStream.connect(rightDataStream)
        .keyBy(new NullByteKeySelector[CRow](), new NullByteKeySelector[CRow]())
        .process(procInnerJoinFunc)
        .setParallelism(1)
        .setMaxParallelism(1)
<<<<<<< HEAD
        .name(operatorName)
        .returns(returnTypeInfo)
    }
  }

  def createRowTimeInnerJoin(
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      returnTypeInfo: TypeInformation[CRow],
      operatorName: String,
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): DataStream[CRow] = {

    val rowTimeInnerJoinFunc = new RowTimeBoundedStreamInnerJoin(
      leftLowerBound,
      leftUpperBound,
      allowedLateness = 0L,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      joinFunctionName,
      joinFunctionCode,
      leftTimeIdx,
      rightTimeIdx)

    if (!leftKeys.isEmpty) {
      leftDataStream
        .connect(rightDataStream)
        .keyBy(leftKeys, rightKeys)
        .transform(
          operatorName,
          returnTypeInfo,
          new KeyedCoProcessOperatorWithWatermarkDelay[Tuple, CRow, CRow, CRow](
            rowTimeInnerJoinFunc,
            rowTimeInnerJoinFunc.getMaxOutputDelay)
        )
    } else {
      leftDataStream.connect(rightDataStream)
        .keyBy(new NullByteKeySelector[CRow](), new NullByteKeySelector[CRow])
        .transform(
          operatorName,
          returnTypeInfo,
          new KeyedCoProcessOperatorWithWatermarkDelay[java.lang.Byte, CRow, CRow, CRow](
            rowTimeInnerJoinFunc,
            rowTimeInnerJoinFunc.getMaxOutputDelay)
        )
        .setParallelism(1)
        .setMaxParallelism(1)
    }
  }
=======
        .returns(returnTypeInfo)
    }
  }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
}
