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
package com.facebook.presto.cost;

import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestCoefficientBasedStatsCalculator
{
    private final LocalQueryRunner queryRunner;

    public TestCoefficientBasedStatsCalculator()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1, true),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testStatsCalculatorUsesLayout()
    {
        assertPlan("SELECT orderstatus FROM orders WHERE orderstatus = 'P'",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                anyTree(
                        node(TableScanNode.class)
                                .withStats(PlanNodeStatsEstimate.builder()
                                        .setOutputRowCount(385.0)
                                        .build())));

        assertPlan("SELECT orderstatus FROM orders WHERE orderkey = 42",
                LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
                anyTree(
                        node(TableScanNode.class)
                                .withStats(PlanNodeStatsEstimate.builder()
                                        .setOutputRowCount(0)
                                        .build())));
    }

    private void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, stage);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getLookup(), actualPlan, pattern);
            return null;
        });
    }
}
