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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.ComparisonStatsCalculator.comparisonExpressionToLiteralStats;
import static com.facebook.presto.cost.ComparisonStatsCalculator.comparisonSymbolToSymbolStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInNonRangeStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInStats;
import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    private static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final Metadata metadata;

    @Inject
    public FilterStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            Map<Symbol, Type> types)
    {
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types).process(predicate);
    }

    public static PlanNodeStatsEstimate filterStatsForUnknownExpression(PlanNodeStatsEstimate inputStatistics)
    {
        return inputStatistics.mapOutputRowCount(size -> size * UNKNOWN_FILTER_COEFFICIENT);
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final Map<Symbol, Type> types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, Map<Symbol, Type> types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return filterForUnknownExpression();
        }

        private PlanNodeStatsEstimate filterForUnknownExpression()
        {
            return filterStatsForUnknownExpression(input);
        }

        private PlanNodeStatsEstimate filterForFalseExpression()
        {
            PlanNodeStatsEstimate.Builder falseStatsBuilder = PlanNodeStatsEstimate.builder();

            input.getSymbolsWithKnownStatistics().forEach(
                    symbol ->
                            falseStatsBuilder.addSymbolStatistics(symbol,
                                    buildFrom(input.getSymbolStatistics(symbol))
                                            .setLowValue(NaN)
                                            .setHighValue(NaN)
                                            .setDistinctValuesCount(0.0)
                                            .setNullsFraction(NaN).build()));

            return falseStatsBuilder.setOutputRowCount(0.0).build();
        }

        @Override
        protected PlanNodeStatsEstimate visitNotExpression(NotExpression node, Void context)
        {
            return differenceInStats(input, process(node.getValue()));
        }

        @Override
        protected PlanNodeStatsEstimate visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            PlanNodeStatsEstimate leftStats = process(node.getLeft());
            PlanNodeStatsEstimate andStats = new FilterExpressionStatsCalculatingVisitor(leftStats, session, types).process(node.getRight());

            switch (node.getType()) {
                case AND:
                    return andStats;
                case OR:
                    PlanNodeStatsEstimate rightStats = process(node.getRight());
                    PlanNodeStatsEstimate sumStats = addStats(leftStats, rightStats);
                    return differenceInNonRangeStats(sumStats, andStats);
                default:
                    throw new IllegalStateException(format("Unimplemented logical binary operator expression %s", node.getType()));
            }
        }

        @Override
        protected PlanNodeStatsEstimate visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.equals(BooleanLiteral.TRUE_LITERAL)) {
                return input;
            }
            else {
                return filterForFalseExpression();
            }
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getValue());
                SymbolStatsEstimate symbolStatsEstimate = input.getSymbolStatistics(symbol);
                return input.mapOutputRowCount(rowCount -> rowCount * (1 - symbolStatsEstimate.getNullsFraction()))
                        .mapSymbolColumnStatistics(symbol, statsEstimate -> statsEstimate.mapNullsFraction(x -> 0.0));
            }
            return visitExpression(node, context);
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getValue());
                SymbolStatsEstimate symbolStatsEstimate = input.getSymbolStatistics(symbol);
                return input.mapOutputRowCount(rowCount -> rowCount * symbolStatsEstimate.getNullsFraction())
                        .mapSymbolColumnStatistics(symbol, statsEstimate ->
                                SymbolStatsEstimate.builder().setNullsFraction(1.0)
                                        .setLowValue(NaN)
                                        .setHighValue(NaN)
                                        .setDistinctValuesCount(0.0).build());
            }
            return visitExpression(node, context);
        }

        @Override
        protected PlanNodeStatsEstimate visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            if (!(node.getValue() instanceof SymbolReference) || !(node.getMin() instanceof Literal) || !(node.getMax() instanceof Literal)) {
                return visitExpression(node, context);
            }

            SymbolStatsEstimate valueStats = input.getSymbolStatistics(Symbol.from((SymbolReference) node.getValue()));
            Expression leftComparison;
            Expression rightComparison;

            // We want to do heuristic cut (infinite range to finite range) ASAP and than do filtering on finite range.
            if (isInfinite(valueStats.getLowValue())) {
                leftComparison = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
                rightComparison = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());
            }
            else {
                rightComparison = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
                leftComparison = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());
            }

            // we relay on and processing left to right
            return process(and(leftComparison, rightComparison));
        }

        @Override
        protected PlanNodeStatsEstimate visitInPredicate(InPredicate node, Void context)
        {
            if (!(node.getValueList() instanceof InListExpression) || !(node.getValue() instanceof SymbolReference)) {
                return visitExpression(node, context);
            }

            InListExpression inList = (InListExpression) node.getValueList();
            PlanNodeStatsEstimate statsSum = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .reduce(filterForFalseExpression(), PlanNodeStatsEstimateMath::addStats);

            Symbol inValueSymbol = Symbol.from(node.getValue());
            SymbolStatsEstimate symbolStat = input.getSymbolStatistics(inValueSymbol);
            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - symbolStat.getNullsFraction());

            return statsSum.mapOutputRowCount(rowCount -> min(rowCount, notNullValuesBeforeIn))
                    .mapSymbolColumnStatistics(inValueSymbol,
                            symbolStats ->
                                    symbolStats.mapNullsFraction(x -> 0.0)
                                            .mapDistinctValuesCount(distinctValues ->
                                                    min(distinctValues, input.getSymbolStatistics(inValueSymbol).getDistinctValuesCount())));
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            // TODO: verify we eliminate Literal-Literal earlier or support them here

            ComparisonExpressionType type = node.getType();
            Expression left = node.getLeft();
            Expression right = node.getRight();

            if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
                // normalize so that symbol is on the left
                return process(new ComparisonExpression(type.flip(), right, left));
            }

            if (left instanceof Literal && !(right instanceof Literal)) {
                // normalize so that literal is on the right
                return process(new ComparisonExpression(type.flip(), right, left));
            }

            if (right instanceof Literal) {
                // TODO support Cast(Literal) same way as Literal (nested Casts too)
                Optional<Symbol> symbol = asSymbol(left);
                SymbolStatsEstimate leftStats = getSymbolStatsEstimate(left);
                OptionalDouble literal = doubleValueFromLiteral(getType(left), (Literal) right);
                return comparisonExpressionToLiteralStats(input, symbol, leftStats, literal, type);
            }

            if (right instanceof SymbolReference) {
                // left is SymbolReference too
                return comparisonSymbolToSymbolStats(input, Symbol.from(left), Symbol.from(right), type);
            }

            return filterStatsForUnknownExpression(input);
        }

        private Optional<Symbol> asSymbol(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return Optional.of(Symbol.from(expression));
            }
            return Optional.empty();
        }

        private Type getType(Expression expression)
        {
            return asSymbol(expression)
                    .map(symbol -> requireNonNull(types.get(symbol), () -> format("No type for symbol %s", symbol)))
                    .orElseGet(() -> {
                        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                                metadata.getFunctionRegistry(),
                                metadata.getTypeManager(),
                                session,
                                types,
                                ImmutableList.of(),
                                // At this stage, there should be no subqueries in the plan.
                                node -> new IllegalStateException("Unexpected Subquery"),
                                false);
                        Type type = expressionAnalyzer.analyze(expression, Scope.create());
                        return type;
                    });
        }

        private SymbolStatsEstimate getSymbolStatsEstimate(Expression expression)
        {
            return asSymbol(expression)
                    .map(symbol -> requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol)))
                    .orElseGet(() -> new ScalarStatsCalculator(metadata).calculate(expression, input, session, types));
        }

        private OptionalDouble doubleValueFromLiteral(Type type, Literal literal)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            DomainConverter domainConverter = new DomainConverter(type, metadata.getFunctionRegistry(), session.toConnectorSession());
            return domainConverter.translateToDouble(literalValue);
        }
    }
}
