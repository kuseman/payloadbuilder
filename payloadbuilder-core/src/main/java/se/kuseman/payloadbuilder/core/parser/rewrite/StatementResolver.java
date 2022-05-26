package se.kuseman.payloadbuilder.core.parser.rewrite;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.TableAlias.Type;
import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.TableMeta.Column;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction.LambdaBinding;
import se.kuseman.payloadbuilder.core.parser.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.parser.AJoin;
import se.kuseman.payloadbuilder.core.parser.ASelectNode;
import se.kuseman.payloadbuilder.core.parser.AnalyzeStatement;
import se.kuseman.payloadbuilder.core.parser.Apply;
import se.kuseman.payloadbuilder.core.parser.ArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.parser.ArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.core.parser.AsteriskSelectItem;
import se.kuseman.payloadbuilder.core.parser.CacheFlushRemoveStatement;
import se.kuseman.payloadbuilder.core.parser.CaseExpression;
import se.kuseman.payloadbuilder.core.parser.CaseExpression.WhenClause;
import se.kuseman.payloadbuilder.core.parser.ComparisonExpression;
import se.kuseman.payloadbuilder.core.parser.DereferenceExpression;
import se.kuseman.payloadbuilder.core.parser.DescribeSelectStatement;
import se.kuseman.payloadbuilder.core.parser.DescribeTableStatement;
import se.kuseman.payloadbuilder.core.parser.DropTableStatement;
import se.kuseman.payloadbuilder.core.parser.Expression;
import se.kuseman.payloadbuilder.core.parser.ExpressionSelectItem;
import se.kuseman.payloadbuilder.core.parser.ExpressionVisitor;
import se.kuseman.payloadbuilder.core.parser.IfStatement;
import se.kuseman.payloadbuilder.core.parser.InExpression;
import se.kuseman.payloadbuilder.core.parser.Join;
import se.kuseman.payloadbuilder.core.parser.LambdaExpression;
import se.kuseman.payloadbuilder.core.parser.LikeExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralDoubleExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralFloatExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralLongExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralNullExpression;
import se.kuseman.payloadbuilder.core.parser.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.parser.LogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.parser.LogicalNotExpression;
import se.kuseman.payloadbuilder.core.parser.NamedExpression;
import se.kuseman.payloadbuilder.core.parser.NestedExpression;
import se.kuseman.payloadbuilder.core.parser.NullPredicateExpression;
import se.kuseman.payloadbuilder.core.parser.Option;
import se.kuseman.payloadbuilder.core.parser.ParseException;
import se.kuseman.payloadbuilder.core.parser.PrintStatement;
import se.kuseman.payloadbuilder.core.parser.QualifiedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import se.kuseman.payloadbuilder.core.parser.QueryStatement;
import se.kuseman.payloadbuilder.core.parser.Select;
import se.kuseman.payloadbuilder.core.parser.SelectItem;
import se.kuseman.payloadbuilder.core.parser.SelectStatement;
import se.kuseman.payloadbuilder.core.parser.SelectVisitor;
import se.kuseman.payloadbuilder.core.parser.SetStatement;
import se.kuseman.payloadbuilder.core.parser.ShowStatement;
import se.kuseman.payloadbuilder.core.parser.SortItem;
import se.kuseman.payloadbuilder.core.parser.Statement;
import se.kuseman.payloadbuilder.core.parser.StatementVisitor;
import se.kuseman.payloadbuilder.core.parser.SubQueryTableSource;
import se.kuseman.payloadbuilder.core.parser.SubscriptExpression;
import se.kuseman.payloadbuilder.core.parser.Table;
import se.kuseman.payloadbuilder.core.parser.TableFunction;
import se.kuseman.payloadbuilder.core.parser.TableSource;
import se.kuseman.payloadbuilder.core.parser.TableSourceJoined;
import se.kuseman.payloadbuilder.core.parser.TemplateStringExpression;
import se.kuseman.payloadbuilder.core.parser.UnresolvedDereferenceExpression;
import se.kuseman.payloadbuilder.core.parser.UnresolvedQualifiedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.parser.UnresolvedQualifiedReferenceExpression;
import se.kuseman.payloadbuilder.core.parser.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.parser.UnresolvedTableFunction;
import se.kuseman.payloadbuilder.core.parser.UseStatement;
import se.kuseman.payloadbuilder.core.parser.VariableExpression;

/**
 * Class that resolves an unresolved statement. Looking up paths to qualifiers etc.
 *
 * <pre>
 * 2:nd pass of query compilation.
 *  - Resolving qualifiers
 *  - Validating operations of known datatypes
 * </pre>
 */
public class StatementResolver implements StatementVisitor<Statement, StatementResolver.Context>
{
    private static final StatementResolver STATEMENT_RESOLVER = new StatementResolver();
    private static final ExpressionResolver EXPRESSION_RESOLVER = new ExpressionResolver();
    private static final SelectResolver SELECT_RESOLVER = new SelectResolver();
    private static final Map<Class<? extends ASelectNode>, List<SelectRewriter<? extends ASelectNode>>> SELECT_AFTER_REWRITERS = MapUtils
            .ofEntries(MapUtils.entry(Select.class, asList(new SelectItemsAggregateRewriter(), new OrderBySelectItemRewriter())));

    /**
     * Definition of a select writer
     *
     * @param <T> Type of node
     */
    interface SelectRewriter<T extends ASelectNode>
    {
        /** Rewrite node */
        T rewrite(T source, Context context);
    }

    /** Resolve an unresolved statement */
    public static QueryStatement resolve(QueryStatement statement, QuerySession session)
    {
        Context context = new Context(session);
        List<Statement> result = new ArrayList<>(statement.getStatements()
                .size());
        for (Statement stm : statement.getStatements())
        {
            context.newStatement();
            result.add(stm.accept(STATEMENT_RESOLVER, context));
        }
        return new QueryStatement(result);
    }

    /** Resolve expression. Used for testing purpose only */
    public static Expression resolve(Expression expression, QuerySession session)
    {
        Context context = new Context(session);
        return expression.accept(EXPRESSION_RESOLVER, context)
                .getValue();
    }

    /** Resolve expression. Used for testing purpose only */
    public static Expression resolve(Expression expression, QuerySession session, Function<UnresolvedQualifiedReferenceExpression, QualifiedReferenceExpression> qualifierConsumer)
    {
        Context context = new Context(session);
        context.qualifierConsumer = qualifierConsumer;
        return expression.accept(EXPRESSION_RESOLVER, context)
                .getValue();
    }

    /** Resolve expression using provided table alias as scope. Used for testing purpose only */
    public static Expression resolve(Expression expression, TableAlias alias)
    {
        Context context = new Context(null);
        context.scopeAliases = singleton(alias);
        return expression.accept(EXPRESSION_RESOLVER, context)
                .getValue();
    }

    /** Resolve select. Used for testing purpose only */
    public static Select resolve(Select select, QuerySession session)
    {
        Context context = new Context(session);
        return (Select) select.accept(SELECT_RESOLVER, context);
    }

    /** Context used during resolving */
    static class Context
    {
        private final QuerySession session;

        Context(QuerySession session)
        {
            this.session = session;
            newStatement();
        }

        /** Test function that modifies qualifiers resolve paths */
        Function<UnresolvedQualifiedReferenceExpression, QualifiedReferenceExpression> qualifierConsumer;

        /** Current scope aliases used when resolving qualifiers */
        Set<TableAlias> scopeAliases;
        /** Parent table alias */
        TableAlias parentAlias;

        /** Counter for tuple ordinals */
        int tupleOrdinal;

        /** Marker for selects to know if current select is in a sub query or not */
        boolean insideSubQuery;

        /** Flag that indicates if the resolver is inside the root select or not. */
        boolean isRootSelect = true;

        /**
         * Table meta by table name.
         *
         * <pre>
         * NOTE! This is kept between statements to be able to retrieve meta for a table
         * that is added in an earlier statement (eg. a temporary table)
         * </pre>
         */
        Map<String, TableMeta> tableMetaByName = new HashMap<>();
        /** Table meta by alias ordinal. Used for sub query columns */
        Map<Integer, TableMeta> tableMetaByOrdinal;
        /** Table alias by table ordinal */
        Map<Integer, TableAlias> tableAliasByOrdinal;
        /** Lambda bindings. Holds which lambda id points to which alias */
        Map<Integer, Set<TableAlias>> lambdaAliasById;

        /** Reset context state for a new statement */
        void newStatement()
        {
            insideSubQuery = false;
            isRootSelect = true;
            tupleOrdinal = 0;
            scopeAliases = emptySet();
            parentAlias = TableAliasBuilder.of(-1, Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                    .build();
            lambdaAliasById = new HashMap<>();
            tableMetaByOrdinal = new HashMap<>();
            tableAliasByOrdinal = new HashMap<>();
        }
    }

    @Override
    public Statement visit(IfStatement statement, Context context)
    {
        return new IfStatement(resolveE(statement.getCondition(), context), resolveSs(statement.getStatements(), context, true), resolveSs(statement.getElseStatements(), context, true));
    }

    @Override
    public Statement visit(PrintStatement statement, Context context)
    {
        return new PrintStatement(resolveE(statement.getExpression(), context));
    }

    @Override
    public Statement visit(SetStatement statement, Context context)
    {
        return new SetStatement(statement.getName(), resolveE(statement.getExpression(), context), statement.isSystemProperty());
    }

    @Override
    public Statement visit(UseStatement statement, Context context)
    {
        return new UseStatement(statement.getQname(), resolveE(statement.getExpression(), context));
    }

    @Override
    public Statement visit(DescribeTableStatement statement, Context context)
    {
        return statement;
    }

    @Override
    public Statement visit(DescribeSelectStatement statement, Context context)
    {
        return new DescribeSelectStatement((SelectStatement) resolveS(statement.getSelectStatement(), context, true));
    }

    @Override
    public Statement visit(AnalyzeStatement statement, Context context)
    {
        return new AnalyzeStatement((SelectStatement) resolveS(statement.getSelectStatement(), context, true));
    }

    @Override
    public Statement visit(ShowStatement statement, Context context)
    {
        return statement;
    }

    @Override
    public Statement visit(CacheFlushRemoveStatement statement, Context context)
    {
        return statement;
    }

    @Override
    public Statement visit(SelectStatement statement, Context context)
    {
        return new SelectStatement((Select) statement.getSelect()
                .accept(SELECT_RESOLVER, context), statement.isAssignmentSelect());
    }

    @Override
    public Statement visit(DropTableStatement statement, Context context)
    {
        return statement;
    }

    private static Statement resolveS(Statement statement, Context context, boolean newStatement)
    {
        if (newStatement)
        {
            context.newStatement();
        }
        return statement.accept(STATEMENT_RESOLVER, context);
    }

    private static List<Statement> resolveSs(List<Statement> statements, Context context, boolean newStatement)
    {
        return statements.stream()
                .map(s ->
                {
                    if (newStatement)
                    {
                        context.newStatement();
                    }
                    return s.accept(STATEMENT_RESOLVER, context);
                })
                .collect(toList());
    }

    /** Resolve provided expression and ignore any resulting table aliases */
    private static Expression resolveE(Expression expression, Context context)
    {
        if (expression == null)
        {
            return null;
        }

        return expression.accept(EXPRESSION_RESOLVER, context)
                .getValue();
    }

    /** Resolve provided expressions and ignore any resulting table aliases */
    private static List<Expression> resolveEs(List<Expression> expressions, Context context)
    {
        List<Expression> result = expressions.stream()
                .map(e -> e.accept(EXPRESSION_RESOLVER, context)
                        .getValue())
                .collect(toList());
        return result;
    }

    /**
     * Resolves for expressions
     *
     * <pre>
     * Returns a pair of resulting table aliases and a resolved expression
     * </pre>
     */
    static class ExpressionResolver implements ExpressionVisitor<Pair<Set<TableAlias>, Expression>, StatementResolver.Context>
    {
        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralNullExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralBooleanExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralIntegerExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralLongExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralFloatExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralDoubleExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LiteralStringExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(ComparisonExpression expression, Context context)
        {
            return Pair.of(emptySet(), new ComparisonExpression(expression.getType(), resolveE(expression.getLeft(), context), resolveE(expression.getRight(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(ArithmeticUnaryExpression expression, Context context)
        {
            return Pair.of(emptySet(), new ArithmeticUnaryExpression(expression.getType(), resolveE(expression.getExpression(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(ArithmeticBinaryExpression expression, Context context)
        {
            return Pair.of(emptySet(), new ArithmeticBinaryExpression(expression.getType(), resolveE(expression.getLeft(), context), resolveE(expression.getRight(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LogicalBinaryExpression expression, Context context)
        {
            return Pair.of(emptySet(), new LogicalBinaryExpression(expression.getType(), resolveE(expression.getLeft(), context), resolveE(expression.getRight(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LogicalNotExpression expression, Context context)
        {
            return Pair.of(emptySet(), new LogicalNotExpression(resolveE(expression.getExpression(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(InExpression expression, Context context)
        {
            return Pair.of(emptySet(), new InExpression(resolveE(expression.getExpression(), context), resolveEs(expression.getArguments(), context), expression.isNot()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LikeExpression expression, Context context)
        {
            return Pair.of(emptySet(), new LikeExpression(resolveE(expression.getExpression(), context), expression.getPatternExpression(), expression.isNot(), null));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(QualifiedReferenceExpression expression, Context context)
        {
            throw new IllegalStateException("A " + expression.getClass() + " should not be visited");
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(UnresolvedQualifiedReferenceExpression expression, Context context)
        {
            if (context.qualifierConsumer != null)
            {
                return Pair.of(emptySet(), context.qualifierConsumer.apply(expression));
            }

            Pair<Set<TableAlias>, ResolvePath[]> result = resolveQualifier(context, expression.getLambdaId(), expression.getQname(), expression.getToken());

            return Pair.of(result.getKey(), new QualifiedReferenceExpression(expression.getQname(), expression.getLambdaId(), result.getValue(), expression.getToken()));
        }

        @SuppressWarnings("unchecked")
        @Override
        public Pair<Set<TableAlias>, Expression> visit(UnresolvedQualifiedFunctionCallExpression expression, Context context)
        {
            Pair<String, FunctionInfo> functionInfo = context.session.resolveFunctionInfo(expression.getCatalogAlias(), expression.getName());
            if (functionInfo == null)
            {
                throw new ParseException("No function named: " + expression.getName() + " found.", expression.getToken());
            }

            if (!(functionInfo.getValue() instanceof ScalarFunctionInfo))
            {
                throw new ParseException("Expected a SCALAR function for " + functionInfo.getValue()
                        .toString(), expression.getToken());
            }

            List<Expression> arguments = expression.getArguments();
            validateFunction(functionInfo.getValue(), arguments, expression.getToken());

            arguments = (List<Expression>) functionInfo.getValue()
                    .foldArguments(arguments);

            QualifiedFunctionCallExpression resolvedFunctionCallExpression = new QualifiedFunctionCallExpression(expression.getCatalogAlias(), (ScalarFunctionInfo) functionInfo.getValue(), arguments,
                    expression.getToken());
            return visit(resolvedFunctionCallExpression, context);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(NestedExpression expression, Context context)
        {
            Pair<Set<TableAlias>, Expression> pair = expression.getExpression()
                    .accept(this, context);
            return Pair.of(pair.getKey(), new NestedExpression(pair.getValue()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(NullPredicateExpression expression, Context context)
        {
            return Pair.of(emptySet(), new NullPredicateExpression(resolveE(expression.getExpression(), context), expression.isNot()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(QualifiedFunctionCallExpression expression, Context context)
        {
            Pair<List<Expression>, Set<TableAlias>> result = resolveFunction(context, null, expression.getFunctionInfo(), expression.getArguments(), expression.getToken());
            return Pair.of(result.getValue(), new QualifiedFunctionCallExpression(expression.getCatalogAlias(), expression.getFunctionInfo(), result.getKey(), expression.getToken()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(DereferenceExpression expression, Context context)
        {
            throw new IllegalStateException("A " + expression.getClass() + " should not be visited");
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(UnresolvedDereferenceExpression expression, Context context)
        {
            Pair<Set<TableAlias>, Expression> left = expression.getLeft()
                    .accept(this, context);

            // Set left sides scope aliases in context before resolving right side
            context.scopeAliases = left.getKey();

            Pair<Set<TableAlias>, Expression> right = expression.getRight()
                    .accept(this, context);

            return Pair.of(right.getKey(), new DereferenceExpression(left.getValue(), (QualifiedReferenceExpression) right.getValue()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(LambdaExpression expression, Context context)
        {
            Pair<Set<TableAlias>, Expression> result = expression.getExpression()
                    .accept(this, context);
            return Pair.of(result.getKey(), new LambdaExpression(expression.getIdentifiers(), result.getValue(), expression.getLambdaIds()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(VariableExpression expression, Context context)
        {
            return Pair.of(emptySet(), expression);
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(TemplateStringExpression expression, Context context)
        {
            return Pair.of(emptySet(), new TemplateStringExpression(resolveEs(expression.getExpressions(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(SubscriptExpression expression, Context context)
        {
            // A subscripts resulting aliases is the the result of the value's aliases
            Pair<Set<TableAlias>, Expression> result = expression.getValue()
                    .accept(this, context);
            return Pair.of(result.getKey(), new SubscriptExpression(result.getValue(), resolveE(expression.getSubscript(), context)));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(CaseExpression expression, Context context)
        {
            // A case expressions aliases is all the paths aliases
            Set<TableAlias> result = new LinkedHashSet<>();
            List<WhenClause> whenClauses = new ArrayList<>(expression.getWhenClauses()
                    .size());
            for (WhenClause w : expression.getWhenClauses())
            {
                Pair<Set<TableAlias>, Expression> conditionPair = w.getResult()
                        .accept(this, context);
                result.addAll(conditionPair.getKey());
                whenClauses.add(new CaseExpression.WhenClause(resolveE(w.getCondition(), context), conditionPair.getValue()));
            }

            Expression elseExpression = null;
            if (expression.getElseExpression() != null)
            {
                Pair<Set<TableAlias>, Expression> elsePair = expression.getElseExpression()
                        .accept(this, context);
                result.addAll(elsePair.getKey());
                elseExpression = elsePair.getValue();
            }
            return Pair.of(result, new CaseExpression(whenClauses, elseExpression));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(UnresolvedSubQueryExpression expression, Context context)
        {
            /*
             * When building sub query expression table alias hierarchy we only bind one way, the sub query knows it's parent but the parent does not know about the sub query as child
             *
             * ie.
             *
             * select col , (select a.col, b.col from tableB b for object) obj from tableA a where b.col > 10 <---- this should not be possible
             *
             * Here we will have a table alias hierarchy like this:
             *
             * ROOT tableA <---- Should NOT know about tableB tableB <---- Should know about it's parent
             *
             * From a sub query perspective: we should be able to access everything, ie. traverse upwards From a table source perspective: we should not be able to access sub query expressions since
             * these are not calculated until after FROM part is complete
             */

            TableAlias subQueryAlias = TableAliasBuilder.of(context.tupleOrdinal++, TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), "")
                    .parent(context.parentAlias, false)
                    .build();

            // Start a new table alias with sub query alias as root
            TableAlias root = TableAliasBuilder.of(-1, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                    .parent(subQueryAlias)
                    .build();

            final TableAlias prevParentAlias = context.parentAlias;
            boolean prevInsideSubQuery = context.insideSubQuery;
            context.insideSubQuery = true;
            context.parentAlias = root;
            SelectStatement selectStatement = (SelectStatement) resolveS(expression.getSelectStatement(), context, false);
            context.insideSubQuery = prevInsideSubQuery;
            context.parentAlias = prevParentAlias;

            return Pair.of(emptySet(), new UnresolvedSubQueryExpression(selectStatement, subQueryAlias, expression.getToken()));
        }

        @Override
        public Pair<Set<TableAlias>, Expression> visit(NamedExpression expression, Context context)
        {
            Pair<Set<TableAlias>, Expression> pair = expression.getExpression()
                    .accept(this, context);
            return Pair.of(pair.getKey(), new NamedExpression(expression.getName(), pair.getValue()));
        }
    }

    /** Resolver for select nodes */
    static class SelectResolver implements SelectVisitor<ASelectNode, StatementResolver.Context>
    {
        @Override
        public ASelectNode visit(Select select, Context context)
        {
            boolean isRootSelect = context.isRootSelect;
            // Set root select to false before we visit the from part
            // this to properly mark that we from now on is not
            // in root context
            context.isRootSelect = false;

            TableAlias prevParentAlias = context.parentAlias;
            final Set<TableAlias> prevScopeAliases = context.scopeAliases;

            // Resolve from
            TableSourceJoined from = null;
            if (select.getFrom() != null)
            {
                from = (TableSourceJoined) select.getFrom()
                        .accept(this, context);
                context.parentAlias = from.getTableSource()
                        .getTableAlias();
                // Table functions sets it's own scope aliases based on the result
                // of the function so don't overwrite it
                if (!(from.getTableSource() instanceof TableFunction)
                        || context.scopeAliases.isEmpty())
                {
                    context.scopeAliases = singleton(from.getTableSource()
                            .getTableAlias());
                }
            }

            // Reset root select flag after we have visited the from part
            // to properly let nested subqueries on root level detect that
            // it's indeed inside root
            context.isRootSelect = isRootSelect;

            // Resolve expressions with from table source as scope
            final Expression topExpression = resolveE(select.getTopExpression(), context);
            final Expression where = resolveE(select.getWhere(), context);
            final List<Expression> groupBy = resolveEs(select.getGroupBy(), context);
            final List<SortItem> orderBy = select.getOrderBy()
                    .stream()
                    .map(o -> (SortItem) o.accept(this, context))
                    .collect(toList());

            Set<TableAlias> fromAlias = context.scopeAliases;
            // A select with no from then use the parent alias as scope
            if (fromAlias.isEmpty())
            {
                fromAlias = singleton(prevParentAlias);
            }

            List<SelectItem> selectItems = new ArrayList<>(select.getSelectItems()
                    .size());
            List<Expression> computedExpressions = new ArrayList<>();
            extractComputedExpressions(select, context, isRootSelect, from, fromAlias, selectItems, computedExpressions);
            populateTableMeta(select, context, prevParentAlias, selectItems);

            // Put back previous scope aliases
            context.scopeAliases = prevScopeAliases;
            context.parentAlias = prevParentAlias;

            return rewriteAfter(new Select(selectItems, from, select.getInto(), topExpression, where, groupBy, orderBy, select.getForOutput(), computedExpressions), context);
        }

        @Override
        public ASelectNode visit(TableSourceJoined tableSourceJoined, Context context)
        {
            Set<TableAlias> prevScopeAliases = context.scopeAliases;
            TableSource tableSource = (TableSource) tableSourceJoined.getTableSource()
                    .accept(this, context);
            List<AJoin> joins = new ArrayList<>(tableSourceJoined.getJoins()
                    .size());
            for (AJoin join : tableSourceJoined.getJoins())
            {
                joins.add((AJoin) join.accept(this, context));
            }
            // Restore scope only if there is any joins can than potentially change it
            if (joins.size() > 0)
            {
                context.scopeAliases = prevScopeAliases;
            }
            return new TableSourceJoined(tableSource, joins);
        }

        @Override
        public ASelectNode visit(SortItem sortItem, Context context)
        {
            return new SortItem(resolveE(sortItem.getExpression(), context), sortItem.getOrder(), sortItem.getNullOrder(), sortItem.getToken());
        }

        @Override
        public ASelectNode visit(ExpressionSelectItem item, Context context)
        {
            Expression resolvedExpression = resolveE(item.getExpression(), context);
            // Verify qualifiers got resolved
            // CSOFF
            resolvedExpression.accept(new AExpressionVisitor<Void, Void>()
            // CSON
            {
                private boolean validateQre = true;

                @Override
                public Void visit(DereferenceExpression expression, Void context)
                {
                    expression.getLeft()
                            .accept(this, null);

                    // Cannot validate the right part since
                    // the left could be unknown and hence right will also
                    // be unknown
                    boolean prevValidateQre = validateQre;
                    validateQre = false;
                    expression.getRight()
                            .accept(this, null);
                    validateQre = prevValidateQre;
                    return null;
                }

                @Override
                public Void visit(QualifiedReferenceExpression expression, Void context)
                {
                    if (!validateQre)
                    {
                        return null;
                    }

                    for (ResolvePath path : expression.getResolvePaths())
                    {
                        if (path.getTargetTupleOrdinal() == -1
                                && expression.getLambdaId() == -1)
                        {
                            throw new ParseException("Unkown reference '" + expression + "'", expression.getToken());
                        }
                    }
                    return null;
                }
            }, null);

            return new ExpressionSelectItem(resolvedExpression, item.isEmptyIdentifier(), item.getIdentifier(), item.getAssignmentName(), item.getToken());
        }

        @Override
        public ASelectNode visit(AsteriskSelectItem selectItem, Context context)
        {
            List<Integer> tupleOrdinals = new ArrayList<>();
            List<TableAlias> siblingAliases = context.parentAlias.getSiblingAliases();
            if (siblingAliases.isEmpty())
            {
                throw new ParseException("Cannot use asterisk select without any table source", selectItem.getToken());
            }
            boolean foundAlias = false;

            for (TableAlias alias : siblingAliases)
            {
                if (selectItem.getAlias() != null
                        && !equalsIgnoreCase(selectItem.getAlias(), alias.getAlias()))
                {
                    continue;
                }

                foundAlias = true;
                tupleOrdinals.add(alias.getTupleOrdinal());
            }

            if (selectItem.getAlias() != null
                    && !foundAlias)
            {
                throw new ParseException("No alias found with name: " + selectItem.getAlias(), selectItem.getToken());
            }

            return new AsteriskSelectItem(selectItem.getAlias(), selectItem.getToken(), tupleOrdinals);
        }

        @Override
        public ASelectNode visit(Table table, Context context)
        {
            String tableName = defaultString(table.getCatalogAlias(), "") + "#"
                               + table.getTable()
                                       .toDotDelimited();
            TableMeta tableMeta = context.tableMetaByName.get(tableName);

            if (table.getTableAlias()
                    .getType() == Type.TEMPORARY_TABLE
                    && tableMeta == null)
            {
                throw new ParseException("No temporary table found named '#" + table.getTable()
                        .toDotDelimited() + "'", table.getToken());
            }

            // Recreate alias with proper meta and hierarchy
            TableAlias tableAlias = TableAliasBuilder.of(context.tupleOrdinal++, table.getTableAlias()
                    .getType(), table.getTable(),
                    table.getTableAlias()
                            .getAlias())
                    .tableMeta(tableMeta)
                    .parent(context.parentAlias)
                    .build();

            context.tableAliasByOrdinal.put(tableAlias.getTupleOrdinal(), tableAlias);

            List<Option> resolvedOptions = table.getOptions()
                    .stream()
                    .map(o -> new Option(o.getOption(), resolveE(o.getValueExpression(), context)))
                    .collect(toList());

            return new Table(table.getCatalogAlias(), tableAlias, resolvedOptions, table.getToken());
        }

        @SuppressWarnings("unchecked")
        @Override
        public ASelectNode visit(UnresolvedTableFunction tableFunction, Context context)
        {
            Pair<String, FunctionInfo> functionInfo = context.session.resolveFunctionInfo(tableFunction.getCatalogAlias(), tableFunction.getName());
            if (functionInfo == null)
            {
                throw new ParseException("No function named: " + tableFunction.getName() + " found.", tableFunction.getToken());
            }

            if (!(functionInfo.getValue() instanceof TableFunctionInfo))
            {
                throw new ParseException("Expected a TABLE function for " + functionInfo.getValue()
                        .toString(), tableFunction.getToken());
            }

            List<Expression> arguments = tableFunction.getArguments();
            validateFunction(functionInfo.getValue(), arguments, tableFunction.getToken());
            arguments = (List<Expression>) functionInfo.getValue()
                    .foldArguments(arguments);

            TableFunction resolvedTableFunction = new TableFunction(tableFunction.getCatalogAlias(), tableFunction.getTableAlias(), (TableFunctionInfo) functionInfo.getValue(), arguments,
                    tableFunction.getOptions(), tableFunction.getToken());

            return visit(resolvedTableFunction, context);
        }

        @Override
        public ASelectNode visit(TableFunction tableFunction, Context context)
        {
            // Recreate alias with proper meta and hierarchy
            TableAlias tableAlias = TableAliasBuilder.of(context.tupleOrdinal++, TableAlias.Type.FUNCTION, QualifiedName.of(tableFunction.getFunctionInfo()
                    .getName()), tableFunction.getTableAlias()
                            .getAlias())
                    .tableMeta(tableFunction.getFunctionInfo()
                            .getTableMeta())
                    .parent(context.parentAlias)
                    .build();

            // Use the functions alias when resolving if empty
            if (context.scopeAliases.isEmpty())
            {
                context.scopeAliases = singleton(tableAlias);
            }

            context.tableAliasByOrdinal.put(tableAlias.getTupleOrdinal(), tableAlias);
            Pair<List<Expression>, Set<TableAlias>> result = resolveFunction(context, tableAlias, tableFunction.getFunctionInfo(), tableFunction.getArguments(), tableFunction.getToken());

            context.scopeAliases = result.getValue();

            return new TableFunction(tableFunction.getCatalogAlias(), tableAlias, tableFunction.getFunctionInfo(), result.getKey(), tableFunction.getOptions()
                    .stream()
                    .map(o -> new Option(o.getOption(), resolveE(o.getValueExpression(), context)))
                    .collect(toList()), tableFunction.getToken());
        }

        @Override
        public ASelectNode visit(Join join, Context context)
        {
            // First visit table source, then set it's alias to context and resolve condition
            TableSource tableSource = (TableSource) join.getTableSource()
                    .accept(this, context);
            context.scopeAliases = singleton(tableSource.getTableAlias());
            Expression condition = resolveE(join.getCondition(), context);
            return new Join(tableSource, join.getType(), condition);
        }

        @Override
        public ASelectNode visit(Apply apply, Context context)
        {
            return new Apply((TableSource) apply.getTableSource()
                    .accept(this, context), apply.getType());
        }

        @Override
        public ASelectNode visit(SubQueryTableSource subQuery, Context context)
        {
            /*
             * When visiting a sub query we want to build a new table alias hierarchy Since this is a complete query.
             */

            TableAlias subQueryAlias = TableAliasBuilder.of(context.tupleOrdinal++, TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), subQuery.getTableAlias()
                    .getAlias())
                    .parent(context.parentAlias)
                    .build();

            context.tableAliasByOrdinal.put(subQueryAlias.getTupleOrdinal(), subQueryAlias);

            // Start a new table alias with sub query alias as root
            TableAlias root = TableAliasBuilder.of(-1, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                    .parent(subQueryAlias)
                    .build();

            final TableAlias prevParentAlias = context.parentAlias;

            // Options of a sub query is scoped to the parent so resolve those
            // before switching scope
            final List<Option> options = subQuery.getOptions()
                    .stream()
                    .map(o -> new Option(o.getOption(), resolveE(o.getValueExpression(), context)))
                    .collect(toList());

            context.parentAlias = root;

            boolean prevInsideSubQuery = context.insideSubQuery;
            context.insideSubQuery = true;
            // Resolve select
            Select select = (Select) subQuery.getSelect()
                    .accept(this, context);

            context.insideSubQuery = prevInsideSubQuery;
            context.parentAlias = prevParentAlias;

            return new SubQueryTableSource(subQueryAlias, select, options, subQuery.getToken());
        }

        @SuppressWarnings({ "unchecked" })
        private <T extends ASelectNode> T rewriteAfter(T source, Context context)
        {
            T result = source;
            List<SelectRewriter<? extends ASelectNode>> rules = SELECT_AFTER_REWRITERS.get(source.getClass());
            for (@SuppressWarnings("rawtypes")
            SelectRewriter rule : rules)
            {
                result = (T) rule.rewrite(result, context);
            }
            return result;
        }

        /** Extracts computed expressions from select items */
        private void extractComputedExpressions(Select select, Context context, boolean isRootSelect, TableSourceJoined from, Set<TableAlias> fromAlias, List<SelectItem> selectItems,
                List<Expression> computedExpressions)
        {
            for (SelectItem s : select.getSelectItems())
            {
                // Reset scope
                context.scopeAliases = fromAlias;

                SelectItem item = (SelectItem) s.accept(this, context);

                // Computed select item, move to computed expressions
                // and create a new expression select item that resolves into
                // the computed one
                // Computed expressions will trigger a computed values operator
                // later on
                if (!isRootSelect
                        && from != null
                        && item.isComputed()
                        && item.getAssignmentName() == null)
                {
                    int tupleOrdinal = from.getTableSource()
                            .getTableAlias()
                            .getTupleOrdinal();
                    QualifiedReferenceExpression qre = new QualifiedReferenceExpression(QualifiedName.of(String.format("expr_%d_%d", tupleOrdinal, computedExpressions.size())), -1,
                            new ResolvePath[] { new ResolvePath(-1, tupleOrdinal, emptyList(), TableMeta.MAX_COLUMNS + computedExpressions.size()) }, null);

                    computedExpressions.add(((ExpressionSelectItem) item).getExpression());

                    item = new ExpressionSelectItem(qre, item.isEmptyIdentifier(), item.getIdentifier(), null, item.getToken());
                }
                selectItems.add(item);
            }
        }

        /** Populate table meta in context for provided select */
        // CSOFF
        private void populateTableMeta(
                // CSON
                Select select, Context context, TableAlias prevParentAlias, List<SelectItem> selectItems)
        {
            // Build table meta for temp tables and subqueries
            if (!(select.getInto() != null
                    || context.insideSubQuery))
            {
                return;
            }
            boolean hasAsterisk = false;
            List<TableMeta.Column> columns = new ArrayList<>(selectItems.size());

            Iterator<SelectItem> it = selectItems.iterator();

            while (it.hasNext())
            {
                SelectItem item = it.next();

                if (item.isAsterisk())
                {
                    hasAsterisk = true;
                    continue;
                }

                // TODO: if select into temp table then all non computed select items
                // can be removed since they are reachable anyways since table source
                // tuple is included

                String column = item.getIdentifier();
                DataType dataType = DataType.ANY;

                ResolvePath[] paths = item.getResolvePaths();
                if (paths != null
                        && paths.length == 1)
                {
                    dataType = paths[0].getColumnType();
                }

                if (context.insideSubQuery)
                {
                    columns.add(new ResolveTableColumn(column, dataType, paths));
                }
                else
                {
                    TableAlias tableAlias = null;
                    if (dataType == DataType.TUPLE
                            && paths.length == 1)
                    {
                        tableAlias = context.tableAliasByOrdinal.get(paths[0].getTargetTupleOrdinal());
                    }

                    columns.add(new Column(column, dataType, tableAlias));
                }
            }

            TableMeta meta = columns.isEmpty() ? null
                    : new ResolveTableMeta(columns, hasAsterisk);
            if (select.getInto() != null)
            {
                String tableName = "#" + select.getInto()
                        .getTable()
                        .toDotDelimited();
                if (context.tableMetaByName.put(tableName, meta) != null)
                {
                    throw new ParseException("Temporary table '#" + select.getInto()
                            .getTable()
                            .toDotDelimited() + "' already exists", select.getInto()
                                    .getToken());
                }
            }
            // Sub query columns
            else
            {
                context.tableMetaByOrdinal.put(prevParentAlias.getParent()
                        .getTupleOrdinal(), meta);
            }
        }
    }

    private static void validateFunction(FunctionInfo functionInfo, List<Expression> arguments, Token token)
    {
        int arity = functionInfo.arity();
        if (arity != -1
                && arguments.size() != arity)
        {
            throw new ParseException("Function " + functionInfo.getName() + " expects " + arity + " arguments but got " + arguments.size(), token);
        }
        if (functionInfo.requiresNamedArguments()
                && (arguments.size() <= 0
                        || arguments.stream()
                                .anyMatch(a -> !(a instanceof NamedExpression))))
        {
            if (arguments.stream()
                    .anyMatch(a -> !(a instanceof NamedExpression)))
            {
                throw new ParseException("Function " + functionInfo.getName() + " expects named parameters", token);
            }
        }
    }

    // CSOFF
    private static Pair<Set<TableAlias>, ResolvePath[]> resolveQualifier(
            // CSON
            Context context, int lambdaId, QualifiedName qname, Token token)
    {
        // No aliases in scope then return a path the resolves parts at runtime
        if (context.scopeAliases.isEmpty())
        {
            List<String> parts = qname.getParts();

            if (lambdaId >= 0)
            {
                parts = new ArrayList<>(parts.subList(1, parts.size()));
            }

            return Pair.of(emptySet(), new ResolvePath[] { new ResolvePath(-1, -1, parts, -1) });
        }

        List<String> parts = new ArrayList<>(qname.getParts());
        Set<TableAlias> scopeAliases = context.scopeAliases;

        // Lambda reference => resolve table alias
        if (lambdaId >= 0)
        {
            scopeAliases = context.lambdaAliasById.get(lambdaId);

            // Remove first part since it's resolved to an alias via lambda identifier
            parts.remove(0);

            // No alias connected to this lambda
            // which means we can only try to resolve the lambda value with parts
            // at runtime
            if (scopeAliases == null)
            {
                return Pair.of(emptySet(), new ResolvePath[] { new ResolvePath(-1, -1, parts, -1) });
            }

            // Nothing left to process
            if (parts.isEmpty())
            {
                // This is a lambda access ie. we have an identity lambda of form 'x -> x'
                // Runtime this means simply return the value we encounter in the lambda
                return Pair.of(scopeAliases, new ResolvePath[] { new ResolvePath(-1, -1, emptyList(), -1) });
            }
        }

        // If we have multiple aliases at this stage
        // this means we have some form of function the concatenates multiple
        // aliases ie. 'unionall(aa, ap).map(x -> x.column .....)' here x.column will point
        // to both aa and ap
        // And then we need to check the sourceTupleOrdinal before we
        // can know which targetTupleOrdinal we should use

        List<ResolvePath> resolvePaths = new ArrayList<>(scopeAliases.size());
        boolean needSourceTupleOrdinal = scopeAliases.size() > 1;

        int prevTargetOrdinal = -1;

        Set<TableAlias> output = new LinkedHashSet<>();
        for (TableAlias alias : scopeAliases)
        {
            List<String> tempParts = new ArrayList<>(parts);
            TableAlias pathAlias = getFromQualifiedName(alias, tempParts, token);

            int sourceTupleOrdinal = needSourceTupleOrdinal ? alias.getTupleOrdinal()
                    : -1;
            // Multi alias context but we never changed target then we can remove the previous path
            // since they are the same
            // this happens if we are traversing upwards and access a common tuple ordinal
            if (sourceTupleOrdinal >= 0
                    && pathAlias.getTupleOrdinal() == prevTargetOrdinal /* targetTupleOrdinal != -1 && targetTupleOrdinal == prevTargetOrdinal */)
            {
                sourceTupleOrdinal = -1;
                resolvePaths.remove(resolvePaths.size() - 1);
            }

            prevTargetOrdinal = pathAlias.getTupleOrdinal();

            /*
             * No parts left that means we have a tuple access ie. no columns/fields
             */
            if (tempParts.isEmpty())
            {
                resolvePaths.add(new ResolvePath(sourceTupleOrdinal, pathAlias.getTupleOrdinal(), emptyList(), -1, DataType.TUPLE, null));
                output.add(pathAlias);
                continue;
            }

            String column = tempParts.get(0);
            TableMeta tableMeta = pathAlias.getTableMeta();
            boolean hasAsterisk = tableMeta instanceof ResolveTableMeta
                    && ((ResolveTableMeta) tableMeta).hasAsterisk;
            // When resolving columns on sub queries the prio is like this:
            // - First check if there is computed columns on sub query then
            // check if the wanted column is among those
            // - If no column found or the is asterisk select items
            // then dig into the sub queries child alias
            if (pathAlias.getType() == Type.SUBQUERY)
            {
                tableMeta = context.tableMetaByOrdinal.get(pathAlias.getTupleOrdinal());

                // Check if the sub query has a column we are looking for
                // if so then copy it's resolve paths
                ResolveTableColumn subQueryColumn = tableMeta != null ? (ResolveTableColumn) tableMeta.getColumn(lowerCase(column))
                        : null;
                if (subQueryColumn != null
                        && subQueryColumn.resolvePaths != null)
                {
                    for (int i = 0; i < subQueryColumn.resolvePaths.length; i++)
                    {
                        ResolvePath r = subQueryColumn.resolvePaths[i];

                        // The sub queries resolve path's unresolved parts
                        // is less than current parts then copy it and use current
                        // parts
                        // CSOFF
                        if (tempParts.size() - 1 > (r.getUnresolvedPath().length - (r.getColumnOrdinal() >= 0 ? 0
                                : 1)))
                        // CSON
                        {
                            resolvePaths.add(new ResolvePath(r, tempParts.subList(r.getColumnOrdinal() >= 0 ? 1
                                    : 0, tempParts.size())));
                        }
                        else
                        {
                            resolvePaths.add(r);
                        }
                    }

                    // Move on in outer loop since we found a matching column and is readdy
                    continue;
                }

                hasAsterisk = tableMeta instanceof ResolveTableMeta
                        && ((ResolveTableMeta) tableMeta).hasAsterisk;
                // If the sub query has an asterisk select item, then don't check the sub queries
                // table meta, dig down to it's child alias
                if (tableMeta != null
                        && hasAsterisk)
                {
                    tableMeta = null;
                }

                // If we don't have table meta then dig down to the first
                // child table alias if any else keep on and verify
                // the sub queries columns
                if (tableMeta == null
                        && pathAlias.getChildAliases()
                                .get(0)
                                .getChildAliases()
                                .size() > 0)
                {
                    pathAlias = pathAlias.getChildAliases()
                            .get(0)
                            .getChildAliases()
                            .get(0);

                    tableMeta = pathAlias.getTableMeta();
                }
            }

            TableMeta.Column tableColumn = null;
            int columnOrdinal = -1;
            DataType dataType = DataType.ANY;
            if (tableMeta != null)
            {
                tableColumn = tableMeta.getColumn(column);
                // CSOFF
                if (tableColumn != null)
                // CSON
                {
                    dataType = tableColumn.getType();
                    columnOrdinal = tableColumn.getOrdinal();
                    // Strip the first unresolved part from path
                    tempParts = tempParts.subList(1, tempParts.size());
                }
                // If table meta has an asterisk then we cannot know if the column exists or not
                else if (!hasAsterisk)
                {
                    throw new ParseException("Unknown column: '" + column
                                             + "' in table source: '"
                                             + (pathAlias.getType() == TableAlias.Type.TEMPORARY_TABLE ? "#"
                                                     : "")
                                             + pathAlias.getTable()
                                                     .toDotDelimited()
                                             + (!isBlank(pathAlias.getAlias()) ? (" (" + pathAlias.getAlias() + ")")
                                                     : "")
                                             + "', expected one of: ["
                                             + tableMeta.getColumns()
                                                     .stream()
                                                     .map(Column::getName)
                                                     .collect(joining(", "))
                                             + "]",
                            token);
                }
            }

            List<ResolvePath> subPath = emptyList();
            // Keep on resolving tuple types into sub resolve paths
            if (tableColumn != null
                    && tableColumn.getType() == DataType.TUPLE
                    && tempParts.size() > 0)
            {
                subPath = new ArrayList<>();

                TableAlias tableAlias = tableColumn.getTableAlias();
                while (tempParts.size() > 0
                        && tableAlias != null)
                {
                    // Reset type
                    dataType = DataType.ANY;

                    String part = tempParts.get(0);

                    TableAlias childAlias = null;
                    int subTargetTupleOrdinal = -1;
                    int subColumnOrdinal = -1;
                    // Sub query, then try child aliases
                    if (tableAlias.getType() == Type.SUBQUERY)
                    {
                        childAlias = tableAlias.getChildAlias(part);
                        // CSOFF
                        if (childAlias != null)
                        // CSON
                        {
                            tempParts.remove(0);
                            subTargetTupleOrdinal = childAlias.getTupleOrdinal();
                            dataType = DataType.TUPLE;
                        }
                    }
                    // Temporary table, then try it's columns
                    else if (tableAlias.getType() == Type.TEMPORARY_TABLE)
                    {
                        Column subColumn = tableAlias.getTableMeta()
                                .getColumn(part);
                        // CSOFF
                        if (subColumn != null)
                        // CSON
                        {
                            tempParts.remove(0);
                            subColumnOrdinal = subColumn.getOrdinal();
                            childAlias = subColumn.getTableAlias();
                            dataType = subColumn.getType();
                        }
                    }

                    if (!(subTargetTupleOrdinal == -1
                            && subColumnOrdinal == -1))
                    {
                        subPath.add(new ResolvePath(-1, subTargetTupleOrdinal, emptyList(), subColumnOrdinal));
                    }

                    tableAlias = childAlias;
                }
            }

            resolvePaths.add(new ResolvePath(sourceTupleOrdinal, pathAlias.getTupleOrdinal(), tempParts, columnOrdinal, dataType, subPath.toArray(ResolvePath.EMPTY_ARRAY)));
        }

        return Pair.of(output, resolvePaths.toArray(new ResolvePath[0]));
    }

    /**
     * Find relative alias to provided according to parts. Note! Removes found parts from list
     **/
    // CSOFF
    private static TableAlias getFromQualifiedName(TableAlias parent, List<String> parts, Token token)
    // CSON
    {
        TableAlias result = parent;
        TableAlias current = parent;

        int initSize = parts.size();
        while (current != null
                && parts.size() > 0)
        {
            String part = parts.get(0);

            // 1. Alias match, move on
            if (equalsIgnoreCase(part, current.getAlias()))
            {
                result = current;
                parts.remove(0);
                continue;
            }

            // 2. Child alias
            TableAlias alias = current.getChildAlias(part);
            if (alias == null)
            {
                // 3. Sibling alias
                alias = current.getSiblingAlias(part);
            }
            if (alias != null)
            {
                parts.remove(0);
                result = alias;
                current = alias;
                continue;
            }

            // 4. Parent alias match upwards
            current = current.getParent();
        }

        // We have a multi part identifier that did got any alias match => throw
        if (!isBlank(result.getAlias())
                && initSize == parts.size()
                && initSize > 1)
        {
            throw new ParseException("Invalid table source reference '" + parts.get(0) + "'", token);
        }

        return result;
    }

    /**
     * Visit function info and resolve arguments Takes special care with lambda functions and binds their identifiers etc.
     *
     * @param alias Table alias for the function. Only applicable when FunctionInfo is a {@link TableFunctionInfo}
     * @param functionInfo The function to resolve
     * @param arguments Arguments for the function
     */
    private static Pair<List<Expression>, Set<TableAlias>> resolveFunction(Context context, TableAlias alias, FunctionInfo functionInfo, List<Expression> arguments, Token token)
    {
        /*
         * Lambda function
         *
         * Parent aliases (ROOT)
         *
         * map(aa, x -> x.id) Lambda binding aa -> x Alias result -> arg 1
         *
         * 1.visit aa Result alias => aa bind x to aa 2.visit x -> x.id Result alias => ROOT 3.Alias result = arg 1 => [ROOT]
         *
         * Multi alias function
         *
         * Parent aliases (ROOT)
         *
         * concat(aa, ap) Alias result => all args
         *
         * 1.visit aa Result alias => aa 2.visit ap Result alias => ap 3.Alias result => [aa, ap]
         *
         */

        // Store parent aliases before resolving this function call
        Set<TableAlias> parentAliases = context.scopeAliases;
        List<Expression> newArgs = new ArrayList<>(arguments);
        // Fill resulting aliases per argument list with
        List<Set<TableAlias>> argumentAliases = new ArrayList<>(Collections.nCopies(arguments.size(), null));
        Set<Integer> resolvedIndices = emptySet();
        if (functionInfo instanceof LambdaFunction)
        {
            resolvedIndices = bindLambdaArguments(context, newArgs, argumentAliases, functionInfo, token);
        }

        // Visit non visited arguments
        int size = newArgs.size();
        for (int i = 0; i < size; i++)
        {
            if (resolvedIndices.contains(i))
            {
                continue;
            }
            // Restore parent aliases before every argument process
            context.scopeAliases = parentAliases;
            Pair<Set<TableAlias>, Expression> resolvedArgument = newArgs.get(i)
                    .accept(EXPRESSION_RESOLVER, context);
            newArgs.set(i, resolvedArgument.getValue());
            argumentAliases.set(i, resolvedArgument.getKey());
        }

        // Resolve alias from function
        Set<TableAlias> result;

        if (functionInfo instanceof ScalarFunctionInfo)
        {
            result = ((ScalarFunctionInfo) functionInfo).resolveAlias(parentAliases, argumentAliases);
        }
        else
        {
            result = ((TableFunctionInfo) functionInfo).resolveAlias(alias, parentAliases, argumentAliases);
        }

        return Pair.of(newArgs, result);
    }

    /**
     * <pre>
     * Bind lambda arguments
     * Connect the calculated aliases to the lambda identifier ids
     *
     * ie. map(aa, x -> x.id)
     *
     * Calculate the result alias of 'aa' and connect 'x'-s unique lamda id
     * to it. This to be able to property resolve the qualifier 'x.id' later
     * </pre>
     */
    private static Set<Integer> bindLambdaArguments(Context context, List<Expression> arguments, List<Set<TableAlias>> argumentAliases, FunctionInfo functionInfo, Token token)
    {
        Set<TableAlias> scopeAliases = context.scopeAliases;
        Set<Integer> resolvedIndices = new HashSet<>();
        List<LambdaBinding> lambdaBindings = ((LambdaFunction) functionInfo).getLambdaBindings();
        for (LambdaBinding binding : lambdaBindings)
        {
            resolvedIndices.add(binding.getToArg());
            Expression lambdaExpression = arguments.get(binding.getLambdaArg());

            if (!(lambdaExpression instanceof LambdaExpression))
            {
                throw new ParseException("Function " + functionInfo.getName()
                                         + " expects a LambdaExpression as parameter at index "
                                         + binding.getLambdaArg()
                                         + " but got "
                                         + lambdaExpression.getClass()
                                                 .getSimpleName(),
                        token);
            }

            LambdaExpression le = (LambdaExpression) lambdaExpression;
            for (int id : le.getLambdaIds())
            {
                context.lambdaAliasById.remove(id);
            }

            Expression targetExpression = arguments.get(binding.getToArg());
            // Reset scope aliases before each resolve
            context.scopeAliases = scopeAliases;
            Pair<Set<TableAlias>, Expression> resolvedTargetExpression = targetExpression.accept(EXPRESSION_RESOLVER, context);
            Set<TableAlias> lambdaAliases = resolvedTargetExpression.getKey();
            arguments.set(binding.getToArg(), resolvedTargetExpression.getValue());
            argumentAliases.set(binding.getToArg(), lambdaAliases);
            if (isEmpty(lambdaAliases))
            {
                continue;
            }

            for (int id : le.getLambdaIds())
            {
                context.lambdaAliasById.put(id, lambdaAliases);
            }
        }
        return resolvedIndices;
    }

    /** Marker class for table metas belonging to temp tables and sub queries */
    private static class ResolveTableMeta extends TableMeta
    {
        private final boolean hasAsterisk;

        ResolveTableMeta(List<TableMeta.Column> columns, boolean hasAsterisk)
        {
            super(columns);
            this.hasAsterisk = hasAsterisk;
        }
    }

    /** Table column used for sub query select items to properly resolve target columns */
    private static class ResolveTableColumn extends TableMeta.Column
    {
        private final ResolvePath[] resolvePaths;

        ResolveTableColumn(String name, DataType dataType, ResolvePath[] resolvePaths)
        {
            super(name, dataType);
            this.resolvePaths = resolvePaths;
        }
    }
}
