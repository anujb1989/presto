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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.optimizations.ScalarSubqueryToJoinRewriter;
import com.facebook.presto.sql.planner.optimizations.ScalarSubqueryToJoinRewriter.SubqueryJoin;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.optimizations.SymbolMapper.recurse;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Scalar filter scan query is something like:
 * <pre>
 *     SELECT a,b,c FROM rel WHERE a = correlated1 AND b = correlated2
 * </pre>
 * <p>
 * This optimizer can rewrite to aggregation over a left outer join:
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Project F
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Filter(CASE CN WHEN 0 true WHEN 1 true ELSE fail('Scalar sub-query has returned multiple rows'))
 *   - Aggregation(GROUP BY A, B, C, U; functions: [count(non_null) AS CN, arbitrary(F1) AS F...]
 *     - Join(LEFT_OUTER, D = C)
 *       - AssignUniqueId(adds symbol U)
 *         - (input) plan which produces symbols: [A, B, C]
 *       - Filter(E > 5)
 *         - projection which adds no null symbol used for count() function
 *           - plan which produces symbols: [D, E, F1]
 * </pre>
 * <p>
 * Note only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedNonAggregationScalarToJoin
        implements Rule
{
    private static final Pattern PATTERN = Pattern.typeOf(LateralJoinNode.class);
    private final FunctionRegistry functionRegistry;

    public TransformCorrelatedNonAggregationScalarToJoin(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        LateralJoinNode lateralJoinNode = (LateralJoinNode) node;
        PlanNode subquery = lookup.resolve(lateralJoinNode.getSubquery());

        if (lateralJoinNode.getCorrelation().isEmpty() || !isScalar(subquery, lookup)) {
            return Optional.empty();
        }

        Optional<FilterNode> filterScan = getFilterScan(subquery, lookup);
        if (!filterScan.isPresent()) {
            return Optional.empty();
        }

        if (!isSupportedBySymbolMapper(filterScan.get(), lookup)) {
            return Optional.empty();
        }

        return rewriteScalarFilterScan(lateralJoinNode, filterScan.get(), symbolAllocator, idAllocator, lookup);
    }

    private Optional<FilterNode> getFilterScan(PlanNode planNode, Lookup lookup)
    {
        return searchFrom(planNode, lookup)
                .where(FilterNode.class::isInstance)
                .skipOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                .findFirst();
    }

    private boolean isSupportedBySymbolMapper(FilterNode node, Lookup lookup)
    {
        return searchFrom(node, lookup)
                .where(isInstanceOfAny(TableScanNode.class, ValuesNode.class))
                .skipOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class, AggregationNode.class, TopNNode.class, FilterNode.class))
                .matches();
    }

    private Optional<PlanNode> rewriteScalarFilterScan(LateralJoinNode lateralJoinNode, FilterNode filterNode, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        PlanNodeDecorrelator planNodeDecorrelator = new PlanNodeDecorrelator(idAllocator, lookup);
        ScalarSubqueryToJoinRewriter rewriter = new ScalarSubqueryToJoinRewriter(functionRegistry, symbolAllocator, idAllocator, lookup);
        List<Symbol> correlation = lateralJoinNode.getCorrelation();
        Optional<DecorrelatedNode> decorrelatedSubquerySource = planNodeDecorrelator.decorrelateFilters(filterNode, correlation);
        if (!decorrelatedSubquerySource.isPresent()) {
            return Optional.empty();
        }

        // we need to map output symbols to some different symbols, so output symbols could be used as output of aggregation
        PlanNode decorrelatedSubquerySourceNode = decorrelatedSubquerySource.get().getNode();
        Map<Symbol, Symbol> subqueryOutputsMapping = decorrelatedSubquerySourceNode.getOutputSymbols().stream()
                .collect(toImmutableMap(symbol -> symbol, symbol -> symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol))));
        SymbolMapper symbolMapper = new SymbolMapper(subqueryOutputsMapping);

        PlanNode mappedDecorelatedSubquerySourceNode = symbolMapper.map(decorrelatedSubquerySourceNode, idAllocator, recurse());
        SubqueryJoin subqueryJoin = rewriter.createSubqueryJoin(
                lateralJoinNode,
                mappedDecorelatedSubquerySourceNode,
                decorrelatedSubquerySource.get().getCorrelatedPredicates().map(symbolMapper::map));

        Symbol count = symbolAllocator.newSymbol("count", BIGINT);
        AggregationNode aggregationNode = new AggregationNode(
                idAllocator.getNextId(),
                subqueryJoin.getLeftOuterJoin(),
                ImmutableMap.<Symbol, Aggregation>builder()
                        .put(count, rewriter.createCountAggregation(subqueryJoin.getNonNull(), Optional.empty()))
                        .putAll(rewriter.createArbitraries(subqueryOutputsMapping))
                        .build(),
                ImmutableList.of(subqueryJoin.getLeftOuterJoin().getLeft().getOutputSymbols()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        FilterNode filterNodeToMakeSureThatCheckCountSymbolIsNotPruned = new FilterNode(
                idAllocator.getNextId(),
                aggregationNode,
                new SimpleCaseExpression(
                        count.toSymbolReference(),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("0"), TRUE_LITERAL),
                                new WhenClause(new LongLiteral("1"), TRUE_LITERAL)),
                        Optional.of(new Cast(
                                new FunctionCall(
                                        QualifiedName.of("fail"),
                                        ImmutableList.of(
                                                new LongLiteral(Integer.toString(SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode())),
                                                new StringLiteral("Scalar sub-query has returned multiple rows"))),
                                BOOLEAN))));

        return rewriter.projectToLateralOutputSymbols(
                lateralJoinNode,
                filterNodeToMakeSureThatCheckCountSymbolIsNotPruned,
                Optional.of(new ComparisonExpression(GREATER_THAN, count.toSymbolReference(), new Cast(new LongLiteral("0"), StandardTypes.BIGINT))));
    }
}
