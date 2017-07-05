/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestTransformCorrelatedNonAggregationScalarToJoin
{
    private static final TableHandle NATION = new TableHandle(new ConnectorId("local"), new TpchTableHandle("local", "nation", 1));

    private RuleTester tester;
    private FunctionRegistry functionRegistry;
    private Rule rule;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
        TypeRegistry typeRegistry = new TypeRegistry();
        functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        rule = new TransformCorrelatedNonAggregationScalarToJoin(functionRegistry);
    }

    @Test
    public void doesNotFireOnPlanWithoutLateralNode()
    {
        tester.assertThat(rule)
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithoutAggregation()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(),
                        p.values(p.symbol("a")),
                        p.values(p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithoutScan()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.filter(
                                p.expression("corr = a"),
                                p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void rewritesOnSubqueryWithoutProjection()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.filter(
                                        p.expression("corr = a"),
                                        p.tableScan(
                                                NATION,
                                                ImmutableList.of(p.symbol("a")),
                                                ImmutableMap.of(p.symbol("a"), new TpchColumnHandle("nationkey", BIGINT)))))))
                .matches(
                        project(
                                filter(
                                        "CASE c WHEN 0 THEN true WHEN 1 THEN true ELSE CAST(fail(28, 'Scalar sub-query has returned multiple rows') AS BOOLEAN) END",
                                        aggregation(
                                                ImmutableMap.of(
                                                        "c", functionCall("count", ImmutableList.of("non_null")),
                                                        "a", functionCall("arbitrary", ImmutableList.of("a1"))),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("corr = a1"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values(ImmutableMap.of("corr", 0))),
                                                        project(
                                                                ImmutableMap.of("non_null", expression("true")),
                                                                tableScan("nation", ImmutableMap.of("a1", "nationkey"))))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjection()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("a2"), p.expression("a * corr")),
                                        p.filter(
                                                p.expression("corr = a"),
                                                p.tableScan(
                                                        NATION,
                                                        ImmutableList.of(p.symbol("a")),
                                                        ImmutableMap.of(p.symbol("a"), new TpchColumnHandle("nationkey", BIGINT))))))))
                .matches(
                        project(
                                ImmutableMap.of("a2", expression("a * corr")),
                                filter(
                                        "CASE c WHEN 0 THEN true WHEN 1 THEN true ELSE CAST(fail(28, 'Scalar sub-query has returned multiple rows') AS BOOLEAN) END",
                                        aggregation(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableMap.of(
                                                        "c", functionCall("count", ImmutableList.of("non_null")),
                                                        "a", functionCall("arbitrary", ImmutableList.of("a1"))),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("corr = a1"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values(ImmutableMap.of("corr", 0))),
                                                        project(ImmutableMap.of("non_null", expression("true")),
                                                                tableScan("nation", ImmutableMap.of("a1", "nationkey"))))))));
    }

    @Test
    public void doesNotRewritesWhenCorrelationIsUsedInTwoFilters()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("a2"), p.expression("a * 2")),
                                        p.filter(
                                                p.expression("corr = a"),
                                                p.filter(
                                                        p.expression("corr = a"),
                                                        p.tableScan(
                                                                NATION,
                                                                ImmutableList.of(p.symbol("a")),
                                                                ImmutableMap.of(p.symbol("a"), new TpchColumnHandle("nationkey", BIGINT)))))))))
                .doesNotFire();
    }

    @Test
    public void rewritesWithValuesAndLiteral()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                        p.filter(
                                                p.expression("corr = 1"),
                                                p.values(p.symbol("a"))))))
                .matches(
                        project(
                                ImmutableMap.of("a2", expression("a * corr")),
                                filter(
                                        "CASE c WHEN 0 THEN true WHEN 1 THEN true ELSE CAST(fail(28, 'Scalar sub-query has returned multiple rows') AS BOOLEAN) END",
                                        aggregation(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableMap.of(
                                                        "c", functionCall("count", ImmutableList.of("non_null")),
                                                        "a", functionCall("arbitrary", ImmutableList.of("a1"))),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("corr = a1"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values(ImmutableMap.of("corr", 0))),
                                                        project(ImmutableMap.of("non_null", expression("true")),
                                                                tableScan("nation", ImmutableMap.of("a1", "nationkey"))))))));
    }
}
