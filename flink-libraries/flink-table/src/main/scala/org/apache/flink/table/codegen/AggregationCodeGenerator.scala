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
package org.apache.flink.table.codegen

<<<<<<< HEAD
import java.lang.reflect.ParameterizedType
import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getUserDefinedMethod, signatureToString}
import org.apache.flink.table.runtime.aggregate.{GeneratedAggregations, SingleElementIterable}
=======
import java.lang.reflect.{Modifier, ParameterizedType}
import java.lang.{Iterable => JIterable}

import org.apache.commons.codec.binary.Base64
import org.apache.flink.api.common.state.{State, StateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.{newName, reflectiveFieldWriteAccess}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getUserDefinedMethod, signatureToString}
import org.apache.flink.table.runtime.aggregate.{GeneratedAggregations, SingleElementIterable}
import org.apache.flink.util.InstantiationUtil

import scala.collection.mutable
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

/**
  * A code generator for generating [[GeneratedAggregations]].
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input type information about the input of the Function
  */
class AggregationCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[_ <: Any])
  extends CodeGenerator(config, nullableInput, input) {

<<<<<<< HEAD
=======
  // set of statements for cleanup dataview that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCleanupStatements = mutable.LinkedHashSet[String]()

  /**
    * @return code block of statements that need to be placed in the cleanup() method of
    *         [[GeneratedAggregations]]
    */
  def reuseCleanupCode(): String = {
    reusableCleanupStatements.mkString("", "\n", "\n")
  }

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
  /**
    * Generates a [[org.apache.flink.table.runtime.aggregate.GeneratedAggregations]] that can be
    * passed to a Java compiler.
    *
    * @param name        Class name of the function.
    *                    Does not need to be unique but has to be a valid Java class identifier.
<<<<<<< HEAD
    * @param generator   The code generator instance
=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    * @param physicalInputTypes Physical input row types
    * @param aggregates  All aggregate functions
    * @param aggFields   Indexes of the input fields for all aggregate functions
    * @param aggMapping  The mapping of aggregates to output fields
    * @param partialResults A flag defining whether final or partial results (accumulators) are set
    *                       to the output row.
    * @param fwdMapping  The mapping of input fields to output fields
    * @param mergeMapping An optional mapping to specify the accumulators to merge. If not set, we
    *                     assume that both rows have the accumulators at the same position.
    * @param constantFlags An optional parameter to define where to set constant boolean flags in
    *                      the output row.
    * @param outputArity The number of fields in the output row.
    * @param needRetract a flag to indicate if the aggregate needs the retract method
    * @param needMerge a flag to indicate if the aggregate needs the merge method
    * @param needReset a flag to indicate if the aggregate needs the resetAccumulator method
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateAggregations(
    name: String,
<<<<<<< HEAD
    generator: CodeGenerator,
=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    physicalInputTypes: Seq[TypeInformation[_]],
    aggregates: Array[AggregateFunction[_ <: Any, _ <: Any]],
    aggFields: Array[Array[Int]],
    aggMapping: Array[Int],
    partialResults: Boolean,
    fwdMapping: Array[Int],
    mergeMapping: Option[Array[Int]],
    constantFlags: Option[Array[(Int, Boolean)]],
    outputArity: Int,
    needRetract: Boolean,
    needMerge: Boolean,
<<<<<<< HEAD
    needReset: Boolean)
=======
    needReset: Boolean,
    accConfig: Option[Array[Seq[DataViewSpec[_]]]])
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
  : GeneratedAggregationsFunction = {

    // get unique function name
    val funcName = newName(name)
    // register UDAGGs
<<<<<<< HEAD
    val aggs = aggregates.map(a => generator.addReusableFunction(a))
=======
    val aggs = aggregates.map(a => addReusableFunction(a, contextTerm))

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    // get java types of accumulators
    val accTypeClasses = aggregates.map { a =>
      a.getClass.getMethod("createAccumulator").getReturnType
    }
    val accTypes = accTypeClasses.map(_.getCanonicalName)

<<<<<<< HEAD
    // get java classes of input fields
    val javaClasses = physicalInputTypes.map(t => t.getTypeClass)
    // get parameter lists for aggregation functions
    val parameters = aggFields.map { inFields =>
      val fields = for (f <- inFields) yield
        s"(${javaClasses(f).getCanonicalName}) input.getField($f)"
      fields.mkString(", ")
    }
    val methodSignaturesList = aggFields.map {
      inFields => for (f <- inFields) yield javaClasses(f)
    }

    // check and validate the needed methods
    aggregates.zipWithIndex.map {
      case (a, i) => {
=======
    // get parameter lists for aggregation functions
    val parametersCode = aggFields.map { inFields =>
      val fields = for (f <- inFields) yield
        s"(${CodeGenUtils.boxedTypeTermForTypeInfo(physicalInputTypes(f))}) input.getField($f)"
      fields.mkString(", ")
    }

    // get method signatures
    val classes = UserDefinedFunctionUtils.typeInfoToClass(physicalInputTypes)
    val methodSignaturesList = aggFields.map { inFields =>
      inFields.map(classes(_))
    }

    // initialize and create data views
    addReusableDataViews()

    // check and validate the needed methods
    aggregates.zipWithIndex.map {
      case (a, i) =>
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
        getUserDefinedMethod(a, "accumulate", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
        .getOrElse(
          throw new CodeGenException(
            s"No matching accumulate method found for AggregateFunction " +
              s"'${a.getClass.getCanonicalName}'" +
              s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
        )

        if (needRetract) {
          getUserDefinedMethod(a, "retract", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
          .getOrElse(
            throw new CodeGenException(
              s"No matching retract method found for AggregateFunction " +
                s"'${a.getClass.getCanonicalName}'" +
                s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
          )
        }

        if (needMerge) {
          val methods =
            getUserDefinedMethod(a, "merge", Array(accTypeClasses(i), classOf[JIterable[Any]]))
            .getOrElse(
              throw new CodeGenException(
                s"No matching merge method found for AggregateFunction " +
                  s"${a.getClass.getCanonicalName}'.")
            )

          var iterableTypeClass = methods.getGenericParameterTypes.apply(1)
                                  .asInstanceOf[ParameterizedType].getActualTypeArguments.apply(0)
          // further extract iterableTypeClass if the accumulator has generic type
          iterableTypeClass match {
            case impl: ParameterizedType => iterableTypeClass = impl.getRawType
            case _ =>
          }

          if (iterableTypeClass != accTypeClasses(i)) {
            throw new CodeGenException(
              s"merge method in AggregateFunction ${a.getClass.getCanonicalName} does not have " +
                s"the correct Iterable type. Actually: ${iterableTypeClass.toString}. " +
                s"Expected: ${accTypeClasses(i).toString}")
          }
        }

        if (needReset) {
          getUserDefinedMethod(a, "resetAccumulator", Array(accTypeClasses(i)))
          .getOrElse(
            throw new CodeGenException(
              s"No matching resetAccumulator method found for " +
                s"aggregate ${a.getClass.getCanonicalName}'.")
          )
        }
<<<<<<< HEAD
=======
    }

    /**
      * Create DataView Term, for example, acc1_map_dataview.
      *
      * @param aggIndex index of aggregate function
      * @param fieldName field name of DataView
      * @return term to access [[MapView]] or [[ListView]]
      */
    def createDataViewTerm(aggIndex: Int, fieldName: String): String = {
      s"acc${aggIndex}_${fieldName}_dataview"
    }

    /**
      * Adds a reusable [[org.apache.flink.table.api.dataview.DataView]] to the open, cleanup,
      * close and member area of the generated function.
      *
      */
    def addReusableDataViews(): Unit = {
      if (accConfig.isDefined) {
        val descMapping: Map[String, StateDescriptor[_, _]] = accConfig.get
          .flatMap(specs => specs.map(s => (s.stateId, s.toStateDescriptor)))
          .toMap[String, StateDescriptor[_ <: State, _]]

        for (i <- aggs.indices) yield {
          for (spec <- accConfig.get(i)) yield {
            val dataViewField = spec.field
            val dataViewTypeTerm = dataViewField.getType.getCanonicalName
            val desc = descMapping.getOrElse(spec.stateId,
              throw new CodeGenException(
                s"Can not find DataView in accumulator by id: ${spec.stateId}"))

            // define the DataView variables
            val serializedData = serializeStateDescriptor(desc)
            val dataViewFieldTerm = createDataViewTerm(i, dataViewField.getName)
            val field =
              s"""
                 |    transient $dataViewTypeTerm $dataViewFieldTerm = null;
                 |""".stripMargin
            reusableMemberStatements.add(field)

            // create DataViews
            val descFieldTerm = s"${dataViewFieldTerm}_desc"
            val descClassQualifier = classOf[StateDescriptor[_, _]].getCanonicalName
            val descDeserializeCode =
              s"""
                 |    $descClassQualifier $descFieldTerm = ($descClassQualifier)
                 |      org.apache.flink.util.InstantiationUtil.deserializeObject(
                 |      org.apache.commons.codec.binary.Base64.decodeBase64("$serializedData"),
                 |      $contextTerm.getUserCodeClassLoader());
                 |""".stripMargin
            val createDataView = if (dataViewField.getType == classOf[MapView[_, _]]) {
              s"""
                 |    $descDeserializeCode
                 |    $dataViewFieldTerm = new org.apache.flink.table.dataview.StateMapView(
                 |      $contextTerm.getMapState((
                 |      org.apache.flink.api.common.state.MapStateDescriptor)$descFieldTerm));
                 |""".stripMargin
            } else if (dataViewField.getType == classOf[ListView[_]]) {
              s"""
                 |    $descDeserializeCode
                 |    $dataViewFieldTerm = new org.apache.flink.table.dataview.StateListView(
                 |      $contextTerm.getListState((
                 |      org.apache.flink.api.common.state.ListStateDescriptor)$descFieldTerm));
                 |""".stripMargin
            } else {
              throw new CodeGenException(s"Unsupported dataview type: $dataViewTypeTerm")
            }
            reusableOpenStatements.add(createDataView)

            // cleanup DataViews
            val cleanup =
              s"""
                 |    $dataViewFieldTerm.clear();
                 |""".stripMargin
            reusableCleanupStatements.add(cleanup)
          }
        }
      }
    }

    /**
      * Generate statements to set data view field when use state backend.
      *
      * @param accTerm aggregation term
      * @param aggIndex index of aggregation
      * @return data view field set statements
      */
    def genDataViewFieldSetter(accTerm: String, aggIndex: Int): String = {
      if (accConfig.isDefined) {
        val setters = for (spec <- accConfig.get(aggIndex)) yield {
          val field = spec.field
          val dataViewTerm = createDataViewTerm(aggIndex, field.getName)
          val fieldSetter = if (Modifier.isPublic(field.getModifiers)) {
            s"$accTerm.${field.getName} = $dataViewTerm;"
          } else {
            val fieldTerm = addReusablePrivateFieldAccess(field.getDeclaringClass, field.getName)
            s"${reflectiveFieldWriteAccess(fieldTerm, field, accTerm, dataViewTerm)};"
          }

          s"""
             |    $fieldSetter
          """.stripMargin
        }
        setters.mkString("\n")
      } else {
        ""
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      }
    }

    def genSetAggregationResults: String = {

      val sig: String =
        j"""
           |  public final void setAggregationResults(
           |    org.apache.flink.types.Row accs,
<<<<<<< HEAD
           |    org.apache.flink.types.Row output)""".stripMargin
=======
           |    org.apache.flink.types.Row output) throws Exception """.stripMargin
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

      val setAggs: String = {
        for (i <- aggs.indices) yield

          if (partialResults) {
            j"""
               |    output.setField(
               |      ${aggMapping(i)},
               |      (${accTypes(i)}) accs.getField($i));""".stripMargin
          } else {
            j"""
               |    org.apache.flink.table.functions.AggregateFunction baseClass$i =
               |      (org.apache.flink.table.functions.AggregateFunction) ${aggs(i)};
<<<<<<< HEAD
               |
               |    output.setField(
               |      ${aggMapping(i)},
               |      baseClass$i.getValue((${accTypes(i)}) accs.getField($i)));""".stripMargin
=======
               |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
               |    ${genDataViewFieldSetter(s"acc$i", i)}
               |    output.setField(
               |      ${aggMapping(i)},
               |      baseClass$i.getValue(acc$i));""".stripMargin
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
          }
      }.mkString("\n")

      j"""
         |$sig {
         |$setAggs
         |  }""".stripMargin
    }

    def genAccumulate: String = {

      val sig: String =
        j"""
           |  public final void accumulate(
           |    org.apache.flink.types.Row accs,
<<<<<<< HEAD
           |    org.apache.flink.types.Row input)""".stripMargin

      val accumulate: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.accumulate(
             |      ((${accTypes(i)}) accs.getField($i)),
             |      ${parameters(i)});""".stripMargin
=======
           |    org.apache.flink.types.Row input) throws Exception """.stripMargin

      val accumulate: String = {
        for (i <- aggs.indices) yield {
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    ${genDataViewFieldSetter(s"acc$i", i)}
             |    ${aggs(i)}.accumulate(
             |      acc$i,
             |      ${parametersCode(i)});""".stripMargin
        }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      }.mkString("\n")

      j"""$sig {
         |$accumulate
         |  }""".stripMargin
    }

    def genRetract: String = {

      val sig: String =
        j"""
           |  public final void retract(
           |    org.apache.flink.types.Row accs,
<<<<<<< HEAD
           |    org.apache.flink.types.Row input)""".stripMargin

      val retract: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.retract(
             |      ((${accTypes(i)}) accs.getField($i)),
             |      ${parameters(i)});""".stripMargin
=======
           |    org.apache.flink.types.Row input) throws Exception """.stripMargin

      val retract: String = {
        for (i <- aggs.indices) yield {
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    ${genDataViewFieldSetter(s"acc$i", i)}
             |    ${aggs(i)}.retract(
             |      acc$i,
             |      ${parametersCode(i)});""".stripMargin
        }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      }.mkString("\n")

      if (needRetract) {
        j"""
           |$sig {
           |$retract
           |  }""".stripMargin
      } else {
        j"""
           |$sig {
           |  }""".stripMargin
      }
    }

    def genCreateAccumulators: String = {

      val sig: String =
        j"""
<<<<<<< HEAD
           |  public final org.apache.flink.types.Row createAccumulators()
=======
           |  public final org.apache.flink.types.Row createAccumulators() throws Exception
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
           |    """.stripMargin
      val init: String =
        j"""
           |      org.apache.flink.types.Row accs =
           |          new org.apache.flink.types.Row(${aggs.length});"""
        .stripMargin
      val create: String = {
<<<<<<< HEAD
        for (i <- aggs.indices) yield
          j"""
             |    accs.setField(
             |      $i,
             |      ${aggs(i)}.createAccumulator());"""
          .stripMargin
=======
        for (i <- aggs.indices) yield {
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) ${aggs(i)}.createAccumulator();
             |    ${genDataViewFieldSetter(s"acc$i", i)}
             |    accs.setField(
             |      $i,
             |      acc$i);"""
            .stripMargin
        }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      }.mkString("\n")
      val ret: String =
        j"""
           |      return accs;"""
        .stripMargin

      j"""$sig {
         |$init
         |$create
         |$ret
         |  }""".stripMargin
    }

    def genSetForwardedFields: String = {

      val sig: String =
        j"""
           |  public final void setForwardedFields(
           |    org.apache.flink.types.Row input,
           |    org.apache.flink.types.Row output)
           |    """.stripMargin

      val forward: String = {
        for (i <- fwdMapping.indices if fwdMapping(i) >= 0) yield
          {
            j"""
               |    output.setField(
               |      $i,
               |      input.getField(${fwdMapping(i)}));"""
            .stripMargin
          }
      }.mkString("\n")

      j"""$sig {
         |$forward
         |  }""".stripMargin
    }

    def genSetConstantFlags: String = {

      val sig: String =
        j"""
           |  public final void setConstantFlags(org.apache.flink.types.Row output)
           |    """.stripMargin

      val setFlags: String = if (constantFlags.isDefined) {
        {
          for (cf <- constantFlags.get) yield {
            j"""
               |    output.setField(${cf._1}, ${if (cf._2) "true" else "false"});"""
            .stripMargin
          }
        }.mkString("\n")
      } else {
        ""
      }

      j"""$sig {
         |$setFlags
         |  }""".stripMargin
    }

    def genCreateOutputRow: String = {
      j"""
         |  public final org.apache.flink.types.Row createOutputRow() {
         |    return new org.apache.flink.types.Row($outputArity);
         |  }""".stripMargin
    }

    def genMergeAccumulatorsPair: String = {

      val mapping = mergeMapping.getOrElse(aggs.indices.toArray)

      val sig: String =
        j"""
           |  public final org.apache.flink.types.Row mergeAccumulatorsPair(
           |    org.apache.flink.types.Row a,
           |    org.apache.flink.types.Row b)
           """.stripMargin
      val merge: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${accTypes(i)} aAcc$i = (${accTypes(i)}) a.getField($i);
             |    ${accTypes(i)} bAcc$i = (${accTypes(i)}) b.getField(${mapping(i)});
             |    accIt$i.setElement(bAcc$i);
             |    ${aggs(i)}.merge(aAcc$i, accIt$i);
             |    a.setField($i, aAcc$i);
          """.stripMargin
      }.mkString("\n")
      val ret: String =
        j"""
           |      return a;
           """.stripMargin

      if (needMerge) {
<<<<<<< HEAD
=======
        if (accConfig.isDefined) {
          throw new CodeGenException("DataView doesn't support merge when the backend uses " +
            s"state when generate aggregation for $funcName.")
        }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
        j"""
           |$sig {
           |$merge
           |$ret
           |  }""".stripMargin
      } else {
        j"""
           |$sig {
           |$ret
           |  }""".stripMargin
      }
    }

    def genMergeList: String = {
      {
        val singleIterableClass = classOf[SingleElementIterable[_]].getCanonicalName
        for (i <- accTypes.indices) yield
          j"""
             |    private final $singleIterableClass<${accTypes(i)}> accIt$i =
             |      new $singleIterableClass<${accTypes(i)}>();
             """.stripMargin
      }.mkString("\n")
    }

    def genResetAccumulator: String = {

      val sig: String =
        j"""
           |  public final void resetAccumulator(
<<<<<<< HEAD
           |    org.apache.flink.types.Row accs)""".stripMargin

      val reset: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.resetAccumulator(
             |      ((${accTypes(i)}) accs.getField($i)));""".stripMargin
=======
           |    org.apache.flink.types.Row accs) throws Exception """.stripMargin

      val reset: String = {
        for (i <- aggs.indices) yield {
          j"""
             |    ${accTypes(i)} acc$i = (${accTypes(i)}) accs.getField($i);
             |    ${genDataViewFieldSetter(s"acc$i", i)}
             |    ${aggs(i)}.resetAccumulator(acc$i);""".stripMargin
        }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
      }.mkString("\n")

      if (needReset) {
        j"""$sig {
           |$reset
           |  }""".stripMargin
      } else {
        j"""$sig {
           |  }""".stripMargin
      }
    }

<<<<<<< HEAD
=======
    val aggFuncCode = Seq(
      genSetAggregationResults,
      genAccumulate,
      genRetract,
      genCreateAccumulators,
      genSetForwardedFields,
      genSetConstantFlags,
      genCreateOutputRow,
      genMergeAccumulatorsPair,
      genResetAccumulator).mkString("\n")

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
    val generatedAggregationsClass = classOf[GeneratedAggregations].getCanonicalName
    var funcCode =
      j"""
         |public final class $funcName extends $generatedAggregationsClass {
         |
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
<<<<<<< HEAD
         """.stripMargin

    funcCode += genSetAggregationResults + "\n"
    funcCode += genAccumulate + "\n"
    funcCode += genRetract + "\n"
    funcCode += genCreateAccumulators + "\n"
    funcCode += genSetForwardedFields + "\n"
    funcCode += genSetConstantFlags + "\n"
    funcCode += genCreateOutputRow + "\n"
    funcCode += genMergeAccumulatorsPair + "\n"
    funcCode += genResetAccumulator + "\n"
    funcCode += "}"

    GeneratedAggregationsFunction(funcName, funcCode)
  }

=======
         |  public final void open(
         |    org.apache.flink.api.common.functions.RuntimeContext $contextTerm) throws Exception {
         |    ${reuseOpenCode()}
         |  }
         |
         |  $aggFuncCode
         |
         |  public final void cleanup() throws Exception {
         |    ${reuseCleanupCode()}
         |  }
         |
         |  public final void close() throws Exception {
         |    ${reuseCloseCode()}
         |  }
         |}
         """.stripMargin

    GeneratedAggregationsFunction(funcName, funcCode)
  }

  @throws[Exception]
  def serializeStateDescriptor(stateDescriptor: StateDescriptor[_, _]): String = {
    val byteArray = InstantiationUtil.serializeObject(stateDescriptor)
    Base64.encodeBase64URLSafeString(byteArray)
  }
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
}
