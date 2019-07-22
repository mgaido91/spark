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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Cast, If}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate, First}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

class AggregationExpressionsSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("OptimizeAggregationExpressions", Once,
        OptimizeAggregationExpressions(SimpleTestOptimizer)) :: Nil
  }

  val testRelation = LocalRelation('a.int.notNull, 'b.int)

  test("count optimization") {
    val countAnalyzed = testRelation.select(count('a), count('b), countDistinct('a, 'b)).analyze
    val optimizedExprs = Optimize.execute(countAnalyzed)
      .expressions.map(_.children.head.asInstanceOf[AggregateExpression].aggregateFunction)
      .asInstanceOf[Seq[DeclarativeAggregate]]
    val bAttrResolved = countAnalyzed.references.find(_.name == "b").get
    val countAttr0 = optimizedExprs(0).aggBufferAttributes.head
    compareExpressions(optimizedExprs(0).updateExpressions.head, countAttr0 + 1L)
    val countAttr1 = optimizedExprs(1).aggBufferAttributes.head
    compareExpressions(optimizedExprs(1).updateExpressions.head,
      If(bAttrResolved.isNull, countAttr1, countAttr1 + 1L))
    val countAttr2 = optimizedExprs(2).aggBufferAttributes.head
    compareExpressions(optimizedExprs(2).updateExpressions.head,
      If(bAttrResolved.isNull, countAttr2, countAttr1 + 1L))
  }

  test("first optimization") {
    val countAnalyzed = testRelation.select(First('a, true), First('b, true)).analyze
    val optimizedExprs = Optimize.execute(
      countAnalyzed).expressions.map(_.children.head).asInstanceOf[Seq[DeclarativeAggregate]]
    val aAttrResolved = countAnalyzed.references.find(_.name == "a").get
    val bAttrResolved = countAnalyzed.references.find(_.name == "b").get
    val firstAttr1 = optimizedExprs(0).aggBufferAttributes.head
    val valueSet1 = optimizedExprs(0).aggBufferAttributes.last
    compareExpressions(optimizedExprs(0).updateExpressions.head,
      If(valueSet1, firstAttr1, aAttrResolved))
    compareExpressions(optimizedExprs(0).updateExpressions.last,
      true)
    val firstAttr2 = optimizedExprs(1).aggBufferAttributes.head
    val valueSet2 = optimizedExprs(1).aggBufferAttributes.last
    compareExpressions(optimizedExprs(1).updateExpressions.head,
      If(valueSet2 || bAttrResolved.isNull, firstAttr2, bAttrResolved))
    compareExpressions(optimizedExprs(1).updateExpressions.last,
      valueSet2 || bAttrResolved.isNotNull)
  }

  test("sum optimization") {
    val countAnalyzed = testRelation.select(sum('a), sum('b)).analyze
    val optimizedExprs = Optimize.execute(countAnalyzed)
      .expressions.map(_.children.head.asInstanceOf[AggregateExpression].aggregateFunction)
      .asInstanceOf[Seq[DeclarativeAggregate]]
    val aAttrResolved = countAnalyzed.references.find(_.name == "a").get
    val bAttrResolved = countAnalyzed.references.find(_.name == "b").get
    val sumAttr1 = optimizedExprs(0).aggBufferAttributes.head
    val timeZone = SQLConf.get.sessionLocalTimeZone
    compareExpressions(optimizedExprs(0).updateExpressions.head,
      coalesce(sumAttr1, 0L) + Cast(aAttrResolved, LongType, Some(timeZone)))
    val sumAttr2 = optimizedExprs(1).aggBufferAttributes.head
    compareExpressions(optimizedExprs(1).updateExpressions.head,
      coalesce(coalesce(sumAttr2, 0L) + Cast(bAttrResolved, LongType, Some(timeZone)), sumAttr2))
  }
}
