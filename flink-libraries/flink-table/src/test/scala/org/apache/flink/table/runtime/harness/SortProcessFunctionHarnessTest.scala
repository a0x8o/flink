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

package org.apache.flink.table.runtime.harness

import java.lang.{Integer => JInt, Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.typeutils.runtime.RowComparator
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.table.runtime.aggregate.{CollectionRowComparator, ProcTimeSortProcessFunction, RowTimeSortProcessFunction}
import org.apache.flink.table.runtime.harness.SortProcessFunctionHarnessTest.TupleRowSelector
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
<<<<<<< HEAD
=======
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.types.Row
import org.junit.Test

class SortProcessFunctionHarnessTest {
  
  @Test
  def testSortProcTimeHarnessPartitioned(): Unit = {
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a", "b", "c", "d", "e"))
    
    val indexes = Array(1, 2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
    

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new ProcTimeSortProcessFunction(
        inputCRowType,
        collectionRowComparator))
  
<<<<<<< HEAD
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer,CRow,CRow](
=======
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer, CRow, CRow](
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
   testHarness.open()

   testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
<<<<<<< HEAD
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2003))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)
    
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2007))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2007))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2007))
    
    testHarness.setProcessingTime(1008)
    
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
=======
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true)))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)

    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong), true)))

    testHarness.setProcessingTime(1008)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    // (1,0) (2,0)
<<<<<<< HEAD
    
     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 4))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),true), 4))
    
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong),true), 1006))
    expectedOutput.add(new StreamRecord(new CRow(
        Row.of(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong),true), 1006))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong),true), 1006))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)
    
    testHarness.close()
  }
  
  @Test
  def testSortRowTimeHarnessPartitioned(): Unit = {
    
=======

     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true)))
     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),true)))

    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
        Row.of(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong),true)))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testSortRowTimeHarnessPartitioned(): Unit = {

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
<<<<<<< HEAD
      LONG_TYPE_INFO),
      Array("a", "b", "c", "d", "e"))

    val indexes = Array(1, 2)
      
    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)    
=======
      TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
      Array("a", "b", "c", "d", "e"))

    val indexes = Array(1, 2)

    val fieldComps = Array[TypeComparator[AnyRef]](
      LONG_TYPE_INFO.createComparator(true, null).asInstanceOf[TypeComparator[AnyRef]],
      INT_TYPE_INFO.createComparator(false, null).asInstanceOf[TypeComparator[AnyRef]] )
    val booleanOrders = Array(true, false)
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

    val rowComp = new RowComparator(
      rT.getTotalFields,
      indexes,
      fieldComps,
      new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
      booleanOrders)
<<<<<<< HEAD
    
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(rT)
    
    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new RowTimeSortProcessFunction(
        inputCRowType,
        Some(collectionRowComparator)))
  
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer, CRow, CRow](
      processFunction, 
      new TupleRowSelector(0), 
      BasicTypeInfo.INT_TYPE_INFO)
    
=======

    val collectionRowComparator = new CollectionRowComparator(rowComp)

    val inputCRowType = CRowTypeInfo(rT)

    val processFunction = new KeyedProcessOperator[Integer,CRow,CRow](
      new RowTimeSortProcessFunction(
        inputCRowType,
        4,
        Some(collectionRowComparator)))

   val testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer, CRow, CRow](
      processFunction,
      new TupleRowSelector(0),
      BasicTypeInfo.INT_TYPE_INFO)

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
   testHarness.open()

   testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime)
   testHarness.processWatermark(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
<<<<<<< HEAD
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true), 1001))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2002))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong), true), 2004))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2006))
=======
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1001L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2004L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2006L: JLong), true)))
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

    // move watermark forward
    testHarness.processWatermark(2007)

    testHarness.processElement(new StreamRecord(new CRow(
<<<<<<< HEAD
      Row.of(1: JInt, 20L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2008))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2002)) // too late
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong), true), 2019)) // too early
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2008))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2010))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 19L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2008))
=======
      Row.of(1: JInt, 20L: JLong, 1: JInt, "aaa", 2008L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong), true))) // too late
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2019L: JLong), true))) // too early
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 2: JInt, "aaa", 2008L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2010L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 19L: JLong, 0: JInt, "aaa", 2008L: JLong), true)))
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

    // move watermark forward
    testHarness.processWatermark(2012)

    val result = testHarness.getOutput
<<<<<<< HEAD
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
=======

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    expectedOutput.add(new Watermark(3))
    expectedOutput.add(new StreamRecord(new CRow(
<<<<<<< HEAD
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true), 1001))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 11L: JLong),true), 2002))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 11L: JLong),true), 2004))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true), 2006))
    expectedOutput.add(new Watermark(2007))

    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 19L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2008))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 2: JInt, "aaa", 11L: JLong), true), 2008))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 1: JInt, "aaa", 11L: JLong), true), 2008))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true), 2010))
=======
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1001L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2004L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2006L: JLong), true)))
    expectedOutput.add(new Watermark(2007))

    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 19L: JLong, 0: JInt, "aaa", 2008L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 2: JInt, "aaa", 2008L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 1: JInt, "aaa", 2008L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2010L: JLong), true)))
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

    expectedOutput.add(new Watermark(2012))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)
        
    testHarness.close()
        
  }
}

object SortProcessFunctionHarnessTest {

  /**
   * Simple test class that returns a specified field as the selector function
   */
  class TupleRowSelector(private val selectorField: Int) extends KeySelector[CRow, Integer] {

    override def getKey(value: CRow): Integer = {
      value.row.getField(selectorField).asInstanceOf[Integer]
    }
  }
}
