package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.apache.commons.lang3.StringUtils.upperCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.CompiledQuery;
import se.kuseman.payloadbuilder.core.CompiledQuery.Warning;
import se.kuseman.payloadbuilder.core.cache.CacheType;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.ArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.ArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.core.expression.AssignmentExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.AtTimeZoneExpression;
import se.kuseman.payloadbuilder.core.expression.CaseExpression;
import se.kuseman.payloadbuilder.core.expression.CastExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;
import se.kuseman.payloadbuilder.core.expression.DateAddExpression;
import se.kuseman.payloadbuilder.core.expression.DateDiffExpression;
import se.kuseman.payloadbuilder.core.expression.DatePartExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.InExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LikeExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralNullExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.expression.LogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.LogicalNotExpression;
import se.kuseman.payloadbuilder.core.expression.NamedExpression;
import se.kuseman.payloadbuilder.core.expression.NestedExpression;
import se.kuseman.payloadbuilder.core.expression.NullPredicateExpression;
import se.kuseman.payloadbuilder.core.expression.SubscriptExpression;
import se.kuseman.payloadbuilder.core.expression.TemplateStringExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.AnalyzeStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.BracketExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CacheFlushContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CacheNameContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CacheRemoveContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CaseExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CastExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ColumnReferenceContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CountExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DateAddExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DateDiffExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DatePartExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DescribeStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DropTableStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_addContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_andContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_at_time_zoneContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_compareContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_inContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_is_not_nullContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_likeContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_listContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_mulContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_orContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_unary_notContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.Expr_unary_signContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FullCacheQualifierContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionArgumentContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.GenericFunctionCallExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.IdentifierContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.IfStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.IndirectionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.JoinPartContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LambdaExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LiteralContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.PrintStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.QnameContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.QueryContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SelectItemContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SelectStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SetStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ShowStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SortItemContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableSourceContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableSourceJoinedContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableSourceOptionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TemplateStringAtomContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TemplateStringLiteralContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TopCountContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TopExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TopSelectContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.UseStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.VariableExpressionContext;
import se.kuseman.payloadbuilder.core.statement.CacheFlushRemoveStatement;
import se.kuseman.payloadbuilder.core.statement.DescribeSelectStatement;
import se.kuseman.payloadbuilder.core.statement.DropTableStatement;
import se.kuseman.payloadbuilder.core.statement.IfStatement;
import se.kuseman.payloadbuilder.core.statement.InsertIntoStatement;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.PrintStatement;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;
import se.kuseman.payloadbuilder.core.statement.SelectStatement;
import se.kuseman.payloadbuilder.core.statement.SetStatement;
import se.kuseman.payloadbuilder.core.statement.ShowStatement;
import se.kuseman.payloadbuilder.core.statement.Statement;
import se.kuseman.payloadbuilder.core.statement.UseStatement;

/** Parser for a payload builder query */
public class QueryParser
{
    /** Parse query */
    public QueryStatement parseQuery(String query, List<CompiledQuery.Warning> warnings)
    {
        return getTree(query, warnings, p -> p.query());
    }

    /** Parse select. NOTE! Used in test only */
    public Statement parseSelect(String query)
    {
        return getTree(query, null, p -> p.topSelect());
    }

    /** Parse expression. NOTE! Used in test only */
    public IExpression parseExpression(String expression)
    {
        return getTree(expression, null, p -> p.topExpression());
    }

    @SuppressWarnings("unchecked")
    private <T> T getTree(String body, List<CompiledQuery.Warning> warnings, Function<PayloadBuilderQueryParser, ParserRuleContext> function)
    {
        BaseErrorListener errorListener = new BaseErrorListener()
        {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
            {
                int startIndex = 0;
                int stopIndex = 0;
                if (offendingSymbol instanceof Token)
                {
                    startIndex = ((Token) offendingSymbol).getStartIndex();
                    stopIndex = ((Token) offendingSymbol).getStopIndex() + 1;
                }
                else if (e != null)
                {
                    if (e.getOffendingToken() != null)
                    {
                        startIndex = e.getOffendingToken()
                                .getStartIndex();
                        stopIndex = e.getOffendingToken()
                                .getStopIndex() + 1;

                    }
                    else if (e instanceof LexerNoViableAltException)
                    {
                        startIndex = ((LexerNoViableAltException) e).getStartIndex();
                        stopIndex = startIndex + 1;
                    }
                }

                Location location = new Location(line, startIndex, stopIndex);
                throw new ParseException(msg, location);
            }
        };

        CharStream charStream = CharStreams.fromString(body);
        PayloadBuilderQueryLexer lexer = new PayloadBuilderQueryLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);

        PayloadBuilderQueryParser parser = new PayloadBuilderQueryParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        ParserRuleContext tree;
        try
        {
            parser.getInterpreter()
                    .setPredictionMode(PredictionMode.SLL);

            tree = function.apply(parser);
        }
        catch (ParseCancellationException ex)
        {
            // if we fail, parse with LL mode
            tokens.seek(0); // rewind input stream
            parser.reset();

            parser.getInterpreter()
                    .setPredictionMode(PredictionMode.LL);
            tree = function.apply(parser);
        }

        return (T) new AstBuilder(warnings).visit(tree);
    }

    /** Builds tree */
    private static class AstBuilder extends PayloadBuilderQueryParserBaseVisitor<Object>
    {
        //@formatter:off
        private static final Map<Integer, IArithmeticBinaryExpression.Type> ARITHMETIC_TYPES = Map.of(
                PayloadBuilderQueryLexer.PLUS, IArithmeticBinaryExpression.Type.ADD,
                PayloadBuilderQueryLexer.MINUS, IArithmeticBinaryExpression.Type.SUBTRACT,
                PayloadBuilderQueryLexer.ASTERISK, IArithmeticBinaryExpression.Type.MULTIPLY,
                PayloadBuilderQueryLexer.SLASH, IArithmeticBinaryExpression.Type.DIVIDE,
                PayloadBuilderQueryLexer.PERCENT, IArithmeticBinaryExpression.Type.MODULUS);
        //@formatter:on

        /** Lambda parameters and slot id in current scope */
        private final Map<String, Integer> lambdaParameters = new HashMap<>();

        private boolean insideProjection;
        private boolean insideSubQuery;
        private boolean assignmentSelect = false;
        private List<Warning> warnings;
        private int tableSourceCounter;

        private AstBuilder(List<CompiledQuery.Warning> warnings)
        {
            this.warnings = warnings;
        }

        @Override
        public Object visitStatement(PayloadBuilderQueryParser.StatementContext ctx)
        {
            tableSourceCounter = 0;
            assignmentSelect = false;
            return super.visitStatement(ctx);
        }

        @Override
        public Object visitIfStatement(IfStatementContext ctx)
        {
            IExpression condition = getExpression(ctx.condition);
            List<Statement> statements = ctx.stms.stms.stream()
                    .map(s -> (Statement) visit(s))
                    .collect(toList());
            List<Statement> elseStatements = ctx.elseStatements != null ? ctx.elseStatements.stms.stream()
                    .map(s -> (Statement) visit(s))
                    .collect(toList())
                    : emptyList();
            return new IfStatement(condition, statements, elseStatements);
        }

        @Override
        public Object visitQuery(QueryContext ctx)
        {
            List<Statement> statements = ctx.statements().children.stream()
                    .map(c -> (Statement) visit(c))
                    .filter(Objects::nonNull)
                    .collect(toList());
            return new QueryStatement(statements);
        }

        @Override
        public Object visitPrintStatement(PrintStatementContext ctx)
        {
            return new PrintStatement(getExpression(ctx.expression()));
        }

        @Override
        public Object visitSetStatement(SetStatementContext ctx)
        {
            boolean systemProperty = ctx.AT()
                    .size() == 2;
            return new SetStatement(getIdentifier(ctx.identifier()).toLowerCase(), getExpression(ctx.expression()), systemProperty);
        }

        @Override
        public Object visitUseStatement(UseStatementContext ctx)
        {
            QualifiedName qname = getQualifiedName(ctx.qname());
            IExpression expression = ctx.expression() != null ? getExpression(ctx.expression())
                    : null;
            if (expression != null
                    && qname.getParts()
                            .size() == 1)
            {
                throw new ParseException("Cannot assign value to a catalog alias", ctx);
            }
            else if (expression == null
                    && qname.getParts()
                            .size() > 1)
            {
                throw new ParseException("Must provide an assignment value to a catalog property", ctx);
            }
            return new UseStatement(qname, expression);
        }

        @Override
        public Object visitDescribeStatement(DescribeStatementContext ctx)
        {
            return new DescribeSelectStatement((SelectStatement) visit(ctx.selectStatement()), false, false);
        }

        @Override
        public Object visitAnalyzeStatement(AnalyzeStatementContext ctx)
        {
            return new DescribeSelectStatement((SelectStatement) visit(ctx.selectStatement()), true, false);
        }

        @Override
        public Object visitShowStatement(ShowStatementContext ctx)
        {
            String catalog = getIdentifier(ctx.catalog);
            ShowStatement.Type type = ShowStatement.Type.valueOf(upperCase(ctx.getChild(ctx.getChildCount() - 1)
                    .getText()));
            return new ShowStatement(type, catalog, Location.from(ctx));
        }

        @Override
        public Object visitCacheFlush(CacheFlushContext ctx)
        {
            CacheQualifier cache = (CacheQualifier) visit(ctx.cache);
            IExpression key = getExpression(ctx.expression());
            return new CacheFlushRemoveStatement(cache.type, cache.name, cache.all, true, key);
        }

        @Override
        public Object visitFullCacheQualifier(FullCacheQualifierContext ctx)
        {
            CacheType type = CacheType.valueOf(StringUtils.upperCase(getIdentifier(ctx.type)));
            QualifiedName cacheName = getQualifiedName(ctx.name);
            boolean isAll = ctx.all != null;

            CacheQualifier cache = new CacheQualifier();
            cache.type = type;
            cache.name = cacheName;
            cache.all = isAll;

            return cache;
        }

        @Override
        public Object visitCacheRemove(CacheRemoveContext ctx)
        {
            CacheQualifier cache = (CacheQualifier) visit(ctx.cache);
            return new CacheFlushRemoveStatement(cache.type, cache.name, cache.all, false, null);
        }

        @Override
        public Object visitTopSelect(TopSelectContext ctx)
        {
            return visit(ctx.selectStatement());
        }

        @Override
        public Object visitDropTableStatement(DropTableStatementContext ctx)
        {
            String catalogAlias = ctx.tableName().catalog != null ? ctx.tableName().catalog.getText()
                    : null;
            QualifiedName qname = getQualifiedName(ctx.tableName()
                    .qname());
            boolean lenient = ctx.EXISTS() != null;
            boolean tempTable = ctx.tableName().tempHash != null;
            return new DropTableStatement(catalogAlias, qname, lenient, tempTable, Location.from(ctx));
        }

        @Override
        public Object visitSelectStatement(SelectStatementContext ctx)
        {
            /*
             * @formatter:off
             * 
             * Operator order
             *   FROM (joins) 
             *   WHERE
             *   GROUP BY
             *   HAVING
             *   SELECT (project) 
             *   WINDOW
             *   QUALIFY (filter window functions) 
             *   DISTINCT
             *   ORDER BY
             *   LIMIT
             * 
             * @formatter:on
             */
            boolean prevInsideProjection = insideProjection;
            insideProjection = false;

            ILogicalPlan plan = getTableSource(ctx);

            insideProjection = prevInsideProjection;

            plan = wrapFilter(plan, ctx);
            plan = wrapAggregate(plan, ctx);
            plan = wrapHaving(plan, ctx);
            plan = wrapProjection(plan, ctx);
            // WINDOW
            // QUALIFY
            plan = wrapDistinct(plan, ctx);
            plan = wrapSort(plan, ctx);
            plan = wrapTop(plan, ctx);
            plan = wrapOperatorFunction(plan, ctx);

            LogicalSelectStatement statement = new LogicalSelectStatement(plan, assignmentSelect);

            if (ctx.into != null)
            {
                if (ctx.into.tempHash == null)
                {
                    throw new ParseException("Can only insert into temp tables", ctx.into);
                }
                else if (insideSubQuery)
                {
                    throw new ParseException("SELECT INTO are not allowed in sub query context", ctx.into);
                }

                List<Option> intoOptions = ctx.intoOptions != null ? ctx.intoOptions.options.stream()
                        .map(to -> (Option) visit(to))
                        .collect(toList())
                        : emptyList();

                QualifiedName tableName = getQualifiedName(ctx.into.qname()).toLowerCase();
                statement = new InsertIntoStatement(statement, tableName, intoOptions, Location.from(ctx.into));
            }

            return statement;
        }

        @Override
        public Object visitTopCount(TopCountContext ctx)
        {
            if (ctx.expression() != null)
            {
                return getExpression(ctx.expression());
            }
            return LiteralExpression.createLiteralNumericExpression(ctx.NUMERIC_LITERAL()
                    .getText());
        }

        @Override
        public Object visitTopExpression(TopExpressionContext ctx)
        {
            return getExpression(ctx.expression());
        }

        @Override
        public Object visitSelectItem(SelectItemContext ctx)
        {
            IExpression expression = getExpression(ctx.expression());
            if (ctx.variable() != null)
            {
                if (ctx.variable().system != null)
                {
                    throw new ParseException("Cannot assign to system variables", ctx);
                }

                return new AssignmentExpression(expression, getIdentifier(ctx.variable()
                        .identifier()).toLowerCase());
            }

            if (ctx.ASTERISK() != null)
            {
                QualifiedName alias = QualifiedName.of();
                if (ctx.alias != null)
                {
                    alias = QualifiedName.of(getIdentifier(ctx.alias));
                }
                return new AsteriskExpression(alias, Location.from(ctx));
            }

            String alias = getIdentifier(ctx.identifier());
            if (alias != null)
            {
                return new AliasExpression(expression, alias);
            }

            return expression;
        }

        @Override
        public Object visitTableSourceJoined(TableSourceJoinedContext ctx)
        {
            boolean hasAlias = ctx.tableSource()
                    .identifier() != null;
            if (ctx.joinPart()
                    .size() > 0
                    && !hasAlias)
            {
                throw new ParseException("Alias is mandatory on table sources when having joins", ctx.tableSource());
            }

            ILogicalPlan current = (ILogicalPlan) visit(ctx.tableSource());
            // Wrap joins
            for (JoinPartContext joinCtx : ctx.joinPart())
            {
                if (joinCtx.tableSource()
                        .identifier() == null)
                {
                    throw new ParseException("Alias is mandatory on joined table sources", joinCtx.tableSource());
                }

                Join.Type type;
                if (joinCtx.JOIN() != null)
                {
                    if (joinCtx.LEFT() != null)
                    {
                        type = Join.Type.LEFT;
                    }
                    else if (joinCtx.INNER() != null)
                    {
                        type = Join.Type.INNER;
                    }
                    else if (joinCtx.RIGHT() != null)
                    {
                        type = Join.Type.RIGHT;
                    }
                    else
                    {
                        type = Join.Type.CROSS;
                    }
                }
                else
                {
                    type = joinCtx.OUTER() != null ? Join.Type.LEFT
                            : Join.Type.INNER;
                }

                ILogicalPlan joinTableSource = (ILogicalPlan) visit(joinCtx.tableSource());
                IExpression condition = getExpression(joinCtx.expression());

                boolean populate = joinCtx.POPULATE() != null;

                if (!populate
                        && joinCtx.tableSource()
                                .tableSourceOptions() != null)
                {
                    for (TableSourceOptionContext opt : joinCtx.tableSource()
                            .tableSourceOptions().options)
                    {
                        QualifiedName qname = getQualifiedName(opt.qname());

                        if (PayloadBuilderQueryLexer.VOCABULARY.getSymbolicName(PayloadBuilderQueryLexer.POPULATE)
                                .equalsIgnoreCase(qname.toString()))
                        {
                            addDeprecationWarning("Deprecated usage of populating join. Use \"" + type + " POPULATE JOIN\" instead of table source hint.", opt);
                            populate = true;
                        }
                    }
                }

                String joinAlias = getIdentifier(joinCtx.tableSource()
                        .identifier());
                current = new Join(current, joinTableSource, type, populate ? joinAlias
                        : null, condition, emptySet(), false, Schema.EMPTY);
            }

            return current;
        }

        @Override
        public Object visitTableSource(TableSourceContext ctx)
        {
            String alias = defaultIfBlank(getIdentifier(ctx.identifier()), "");

            List<Option> options = ctx.tableSourceOptions() != null ? ctx.tableSourceOptions().options.stream()
                    .map(to -> (Option) visit(to))
                    .collect(toList())
                    : emptyList();

            if (ctx.tableName() != null)
            {
                String catalogAlias = defaultIfBlank(getIdentifier(ctx.tableName().catalog), "");
                TableSourceReference tableSourceRef = new TableSourceReference(tableSourceCounter++, TableSourceReference.Type.TABLE, catalogAlias, getQualifiedName(ctx.tableName()
                        .qname()), alias);

                boolean tempTable = ctx.tableName().tempHash != null;
                return new TableScan(TableSchema.EMPTY, tableSourceRef, emptyList(), tempTable, options, Location.from(ctx.tableName()));
            }
            else if (ctx.selectStatement() != null)
            {
                boolean prevInsideSubQuery = insideSubQuery;
                insideSubQuery = true;
                LogicalSelectStatement stm = (LogicalSelectStatement) visit(ctx.selectStatement());
                insideSubQuery = prevInsideSubQuery;

                TableSourceReference tableSourceRef = new TableSourceReference(tableSourceCounter++, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of(alias), alias);
                return new SubQuery(stm.getSelect(), tableSourceRef, Location.from(ctx.selectStatement()));
            }
            else if (ctx.variable() != null)
            {
                if (ctx.variable().system != null)
                {
                    throw new ParseException("Variable scans cannot be a system variable", ctx.variable());
                }

                String variable = getIdentifier(ctx.variable()
                        .identifier());
                IExpression expression = new VariableExpression(variable);
                TableSourceReference tableSource = new TableSourceReference(tableSourceCounter++, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of(variable), alias);
                return new ExpressionScan(tableSource, Schema.EMPTY, expression, Location.from(ctx.variable()));
            }
            else if (ctx.expression() != null)
            {
                if (!options.isEmpty())
                {
                    throw new ParseException("Expression scans cannot have options", ctx.tableSourceOptions());
                }

                IExpression expression = getExpression(ctx.expression());
                TableSourceReference tableSource = new TableSourceReference(tableSourceCounter++, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of(expression.toString()), alias);
                return new ExpressionScan(tableSource, Schema.EMPTY, expression, Location.from(ctx.expression()));
            }

            FunctionCallContext functionCall = ctx.functionCall();
            String catalogAlias = defaultIfBlank(getIdentifier(functionCall.functionName().catalog), "");
            String functioName = getIdentifier(ctx.functionCall()
                    .functionName().function);
            TableSourceReference tableSourceRef = new TableSourceReference(tableSourceCounter++, TableSourceReference.Type.FUNCTION, catalogAlias, QualifiedName.of(functioName), alias);
            List<IExpression> arguments = ctx.functionCall().arguments.stream()
                    .map(this::getExpression)
                    .collect(toList());
            return new TableFunctionScan(tableSourceRef, Schema.EMPTY, arguments, options, Location.from(functionCall));
        }

        @Override
        public Object visitTableSourceOption(TableSourceOptionContext ctx)
        {
            QualifiedName option = getQualifiedName(ctx.qname());
            IExpression valueExpression = getExpression(ctx.expression());
            return new Option(option, valueExpression);
        }

        @Override
        public Object visitCaseExpression(CaseExpressionContext ctx)
        {
            List<ICaseExpression.WhenClause> whenClauses = ctx.when()
                    .stream()
                    .map(w -> new ICaseExpression.WhenClause(getExpression(w.condition), getExpression(w.result)))
                    .collect(toList());

            IExpression elseExpression = getExpression(ctx.elseExpr);

            return new CaseExpression(whenClauses, elseExpression);
        }

        @Override
        public Object visitColumnReference(ColumnReferenceContext ctx)
        {
            QualifiedName qname = getQualifiedName(ctx.qname());
            int lambdaId = lambdaParameters.getOrDefault(qname.getFirst(), -1);
            IExpression colE = new UnresolvedColumnExpression(qname, lambdaId, Location.from(ctx));

            if (!ctx.indirection()
                    .isEmpty())
            {
                return wrapIndirection(colE, ctx.indirection());
            }

            return colE;
        }

        @Override
        public Object visitFunctionCallExpression(FunctionCallExpressionContext ctx)
        {
            IExpression functionCallExpression = (IExpression) visit(ctx.scalarFunctionCall());

            if (!ctx.indirection()
                    .isEmpty())
            {
                return wrapIndirection(functionCallExpression, ctx.indirection());
            }

            return functionCallExpression;
        }

        @Override
        public Object visitVariableExpression(VariableExpressionContext ctx)
        {
            IExpression result = new VariableExpression(getIdentifier(ctx.variable()
                    .identifier()), ctx.variable().system != null);

            if (!ctx.indirection()
                    .isEmpty())
            {
                return wrapIndirection(result, ctx.indirection());
            }

            return result;
        }

        @Override
        public Object visitCountExpression(CountExpressionContext ctx)
        {
            if (ctx.ASTERISK() != null
                    && (ctx.ALL() != null
                            || ctx.DISTINCT() != null))
            {
                throw new ParseException("COUNT asterisk doesn't support ALL/DISTINCT", ctx);
            }

            AggregateMode mode = null;
            if (ctx.ALL() != null)
            {
                mode = AggregateMode.ALL;
            }
            else if (ctx.DISTINCT() != null)
            {
                mode = AggregateMode.DISTINCT;
            }

            IExpression argument = ctx.ASTERISK() != null ? new AsteriskExpression(Location.from(ctx.ASTERISK()))
                    : getExpression(ctx.arg);

            return new UnresolvedFunctionCallExpression(Catalog.SYSTEM_CATALOG_ALIAS, "count", mode, asList(argument), Location.from(ctx));
        }

        @Override
        public Object visitCastExpression(CastExpressionContext ctx)
        {
            IExpression expression = getExpression(ctx.input);

            String dataType = null;
            if (ctx.COMMA() != null)
            {
                // Old style cast as ordinary function call
                addDeprecationWarning("Deprecated CAST function call. Use CAST(<expression> AS <datatype>)", ctx);
                IExpression typeArg = getExpression(ctx.arg);
                if (typeArg instanceof LiteralStringExpression)
                {
                    dataType = ((LiteralStringExpression) typeArg).getValue()
                            .toString();
                }
                else if (typeArg instanceof UnresolvedColumnExpression)
                {
                    dataType = ((UnresolvedColumnExpression) typeArg).getColumn()
                            .toString();
                }

                if (dataType == null)
                {
                    throw new ParseException("Cannot convert " + typeArg + " to a datatype", ctx);
                }
            }
            else
            {
                dataType = ctx.dataType.getText();
            }

            ResolvedType resolvedType;
            Type type = EnumUtils.getEnumIgnoreCase(Type.class, dataType);
            if (type == Type.Array)
            {
                resolvedType = ResolvedType.array(Type.Any);
            }
            else
            {
                resolvedType = ResolvedType.of(type);
            }
            return new CastExpression(expression, resolvedType);
        }

        @Override
        public Object visitDateAddExpression(DateAddExpressionContext ctx)
        {
            String partString = null;
            if (ctx.datepartE != null)
            {
                // Old style dateadd as ordinary function call
                addDeprecationWarning("Deprecated DATEADD function call. Use DATEADD(<datepart-literal>, <number-expression>, <date-expression>)", ctx);
                IExpression partArg = getExpression(ctx.datepartE);
                if (partArg instanceof LiteralStringExpression)
                {
                    partString = ((LiteralStringExpression) partArg).getValue()
                            .toString();
                }
                else if (partArg instanceof UnresolvedColumnExpression)
                {
                    partString = ((UnresolvedColumnExpression) partArg).getColumn()
                            .toString();
                }

                if (partString == null)
                {
                    throw new ParseException("Cannot convert " + partArg + " to a date part", ctx);
                }
            }
            else
            {
                partString = ctx.datepart.getText();
            }
            DatePartExpression.Part part = EnumUtils.getEnumIgnoreCase(IDatePartExpression.Part.class, partString);
            IExpression number = getExpression(ctx.number);
            IExpression date = getExpression(ctx.date);

            return new DateAddExpression(part, number, date);
        }

        @Override
        public Object visitDatePartExpression(DatePartExpressionContext ctx)
        {
            String partString = null;
            if (ctx.datepartE != null)
            {
                // Old style dateadd as ordinary function call
                addDeprecationWarning("Deprecated DATEPART function call. Use DATEPART(<datepart-literal>, <date-expression>)", ctx);
                IExpression partArg = getExpression(ctx.datepartE);
                if (partArg instanceof LiteralStringExpression)
                {
                    partString = ((LiteralStringExpression) partArg).getValue()
                            .toString();
                }
                else if (partArg instanceof UnresolvedColumnExpression)
                {
                    partString = ((UnresolvedColumnExpression) partArg).getColumn()
                            .toString();
                }

                if (partString == null)
                {
                    throw new ParseException("Cannot convert " + partArg + " to a date part", ctx);
                }
            }
            else
            {
                partString = ctx.datepart.getText();
            }
            DatePartExpression.Part part = EnumUtils.getEnumIgnoreCase(IDatePartExpression.Part.class, partString);
            IExpression date = getExpression(ctx.date);
            return new DatePartExpression(part, date);
        }

        @Override
        public Object visitDateDiffExpression(DateDiffExpressionContext ctx)
        {
            DatePartExpression.Part part = EnumUtils.getEnumIgnoreCase(IDatePartExpression.Part.class, ctx.datepart.getText());
            IExpression start = getExpression(ctx.start);
            IExpression end = getExpression(ctx.end);
            return new DateDiffExpression(part, start, end);
        }

        @Override
        public Object visitGenericFunctionCallExpression(GenericFunctionCallExpressionContext ctx)
        {
            FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());

            // Find deprecated old built in function calls and move to new expressions
            IExpression builtIn = getOldBuiltInFunction(ctx, functionCallInfo);
            if (builtIn != null)
            {
                return builtIn;
            }

            return new UnresolvedFunctionCallExpression(functionCallInfo.catalogAlias, functionCallInfo.name, functionCallInfo.aggregateMode, functionCallInfo.arguments,
                    Location.from(ctx.functionCall()));
        }

        @Override
        public Object visitFunctionCall(FunctionCallContext ctx)
        {
            AggregateMode mode = null;
            if (ctx.ALL() != null)
            {
                mode = AggregateMode.ALL;
            }
            else if (ctx.DISTINCT() != null)
            {
                mode = AggregateMode.DISTINCT;
            }

            List<IExpression> arguments = ctx.arguments.stream()
                    .map(a -> getExpression(a))
                    .collect(toList());

            String catalog = lowerCase(ctx.functionName().catalog != null ? ctx.functionName().catalog.getText()
                    : null);
            String functionName = getIdentifier(ctx.functionName().function);

            return new FunctionCallInfo(catalog, functionName, mode, arguments);
        }

        @Override
        public Object visitFunctionArgument(FunctionArgumentContext ctx)
        {
            IExpression expression = getExpression(ctx.expression);
            String name = getIdentifier(ctx.name);
            return name != null ? new NamedExpression(name, expression)
                    : expression;
        }

        @Override
        public Object visitExpr_compare(Expr_compareContext ctx)
        {
            IExpression left = getExpression(ctx.left);

            /* Fall through */
            if (ctx.op == null)
            {
                return left;
            }

            IExpression right = getExpression(ctx.right);
            IComparisonExpression.Type type = switch (ctx.op.getType())
            {
                case PayloadBuilderQueryLexer.EQUALS -> IComparisonExpression.Type.EQUAL;
                case PayloadBuilderQueryLexer.NOTEQUALS -> IComparisonExpression.Type.NOT_EQUAL;
                case PayloadBuilderQueryLexer.LESSTHAN -> IComparisonExpression.Type.LESS_THAN;
                case PayloadBuilderQueryLexer.LESSTHANEQUAL -> IComparisonExpression.Type.LESS_THAN_EQUAL;
                case PayloadBuilderQueryLexer.GREATERTHAN -> IComparisonExpression.Type.GREATER_THAN;
                case PayloadBuilderQueryLexer.GREATERTHANEQUAL -> IComparisonExpression.Type.GREATER_THAN_EQUAL;
                default -> throw new RuntimeException("Unkown comparison operator");
            };

            return new ComparisonExpression(type, left, right);
        }

        @Override
        public Object visitExpr_and(Expr_andContext ctx)
        {
            return buildLogicalTree(ILogicalBinaryExpression.Type.AND, ctx.left, ctx.right);
        }

        @Override
        public Object visitExpr_or(Expr_orContext ctx)
        {
            return buildLogicalTree(ILogicalBinaryExpression.Type.OR, ctx.left, ctx.right);
        }

        @Override
        public Object visitExpr_unary_not(Expr_unary_notContext ctx)
        {
            IExpression expression = getExpression(ctx.expr_is_not_null());

            // Fall through
            // Even number of NOT's
            if (ctx.NOT()
                    .size() % 2 == 0)
            {
                return expression;
            }

            return new LogicalNotExpression(getExpression(ctx.expr_is_not_null()));
        }

        @Override
        public Object visitExpr_like(Expr_likeContext ctx)
        {
            IExpression expression = getExpression(ctx.left);

            // Fall through
            if (ctx.LIKE() == null)
            {
                return expression;
            }

            boolean not = ctx.NOT() != null;
            return new LikeExpression(expression, getExpression(ctx.right), not, getExpression(ctx.escape));
        }

        @Override
        public Object visitExpr_in(Expr_inContext ctx)
        {
            IExpression left = getExpression(ctx.left);

            // Fall through
            if (ctx.expr_list() == null)
            {
                return left;
            }

            List<IExpression> arguments = getExpressionList(ctx.expr_list());
            return new InExpression(left, arguments, ctx.NOT() != null);
        }

        @Override
        public Object visitBracketExpression(BracketExpressionContext ctx)
        {
            IExpression result;
            // Plain nested parenthesis expression
            if (ctx.bracket_expression()
                    .expression() != null)
            {
                result = new NestedExpression(getExpression(ctx.bracket_expression()
                        .expression()));
            }
            else
            {

                if (!insideProjection)
                {
                    throw new ParseException("Sub query expressions are only supported in projections", ctx.bracket_expression()
                            .selectStatement());
                }

                LogicalSelectStatement selectStatement = (LogicalSelectStatement) visit(ctx.bracket_expression()
                        .selectStatement());

                ILogicalPlan plan = selectStatement.getSelect();

                result = new UnresolvedSubQueryExpression(plan, Location.from(ctx.bracket_expression()
                        .selectStatement()));
            }

            if (!ctx.indirection()
                    .isEmpty())
            {
                return wrapIndirection(result, ctx.indirection());
            }

            return result;
        }

        @Override
        public Object visitExpr_is_not_null(Expr_is_not_nullContext ctx)
        {
            IExpression expression = getExpression(ctx.expr_compare());

            // Fall through
            if (ctx.IS() == null)
            {
                return expression;
            }

            return new NullPredicateExpression(expression, ctx.NOT() != null);
        }

        @Override
        public Object visitLambdaExpression(LambdaExpressionContext ctx)
        {
            List<String> identifiers = ctx.identifier()
                    .stream()
                    .map(i -> getIdentifierString(i.getText()))
                    .collect(toList());
            identifiers.forEach(i ->
            {
                if (lambdaParameters.containsKey(i))
                {
                    throw new ParseException("Lambda identifier " + i + " is already defined in scope.", ctx);
                }

                lambdaParameters.put(i, lambdaParameters.size());
            });

            IExpression expression = getExpression(ctx.expression());
            int[] uniqueLambdaIds = new int[identifiers.size()];
            for (int i = 0; i < identifiers.size(); i++)
            {
                uniqueLambdaIds[i] = lambdaParameters.size() - 1 + i;
            }
            identifiers.forEach(i -> lambdaParameters.remove(i));
            return new LambdaExpression(identifiers, expression, uniqueLambdaIds);
        }

        @Override
        public Object visitExpr_unary_sign(Expr_unary_signContext ctx)
        {
            IExpression expression = getExpression(ctx.expr_at_time_zone());

            // Fall through
            if (ctx.op == null)
            {
                return expression;
            }

            IArithmeticUnaryExpression.Type type = switch (ctx.op.getType())
            {
                case PayloadBuilderQueryLexer.PLUS -> IArithmeticUnaryExpression.Type.PLUS;
                case PayloadBuilderQueryLexer.MINUS -> IArithmeticUnaryExpression.Type.MINUS;
                default -> throw new RuntimeException("Unkown unary operator " + ctx.op.getType());
            };

            return new ArithmeticUnaryExpression(type, expression);
        }

        @Override
        public Object visitExpr_add(Expr_addContext ctx)
        {
            return buildArithmeticTree(ctx.op, ctx.left, ctx.right);
        }

        @Override
        public Object visitExpr_mul(Expr_mulContext ctx)
        {
            return buildArithmeticTree(ctx.op, ctx.left, ctx.right);
        }

        @Override
        public Object visitExpr_at_time_zone(Expr_at_time_zoneContext ctx)
        {
            IExpression expression = getExpression(ctx.value);

            // Fall through
            if (ctx.AT_WORD() == null)
            {
                return expression;
            }

            IExpression timeZone = getExpression(ctx.expression());
            return new AtTimeZoneExpression(expression, timeZone);
        }

        @Override
        public Object visitLiteral(LiteralContext ctx)
        {
            if (ctx.templateStringLiteral() != null)
            {
                return visit(ctx.templateStringLiteral());
            }

            return getLiteralExpression(ctx);
        }

        @Override
        public Object visitTemplateStringLiteral(TemplateStringLiteralContext ctx)
        {
            List<IExpression> expressions = new ArrayList<>();
            StringBuilder sb = new StringBuilder();

            for (TemplateStringAtomContext actx : ctx.templateStringAtom())
            {
                // While we have plain strings keep adding to string builder
                if (actx.TEMPLATESTRINGATOM() != null)
                {
                    sb.append(actx.getText());
                    continue;
                }

                // Expresion then add eventual literal string gathered previously
                if (sb.length() > 0)
                {
                    expressions.add(new LiteralStringExpression(sb.toString()));
                    sb.setLength(0);
                }

                expressions.add(getExpression(actx.expression()));
            }

            // Add last literal if any
            if (sb.length() > 0)
            {
                expressions.add(new LiteralStringExpression(sb.toString()));
            }

            return new TemplateStringExpression(expressions);
        }

        private IExpression wrapIndirection(IExpression target, List<IndirectionContext> list)
        {
            IExpression result = null;
            for (IndirectionContext indirection : list)
            {
                if (indirection.DOT() != null)
                {
                    // Identifier
                    if (indirection.identifier() != null)
                    {
                        result = new DereferenceExpression(result == null ? target
                                : result, getIdentifier(indirection.identifier())).fold();
                    }
                    // Function call
                    else
                    {
                        Object visitedFunc = visit(indirection.scalarFunctionCall());

                        if (!(visitedFunc instanceof UnresolvedFunctionCallExpression))
                        {
                            throw new ParseException(visitedFunc.getClass()
                                    .getSimpleName() + " cannot be used as a dereference", indirection.scalarFunctionCall());
                        }
                        UnresolvedFunctionCallExpression ufc = (UnresolvedFunctionCallExpression) visitedFunc;
                        List<IExpression> args = new ArrayList<>(ufc.getArguments());
                        args.add(0, result == null ? target
                                : result);
                        // Folding of function calls are done at a later stage
                        result = new UnresolvedFunctionCallExpression(ufc.getCatalogAlias(), ufc.getName(), ufc.getAggregateMode(), args, ufc.getLocation());
                    }
                }
                // Subscript
                else
                {
                    result = new SubscriptExpression(result == null ? target
                            : result, getExpression(indirection.expression())).fold();
                }
            }

            return result;
        }

        private IExpression buildLogicalTree(ILogicalBinaryExpression.Type type, ParserRuleContext left, List<? extends ParserRuleContext> right)
        {
            IExpression le = getExpression(left);

            // Fall through
            if (right.isEmpty())
            {
                return le;
            }

            IExpression result = null;

            List<IExpression> rightList = right.stream()
                    .map(e -> (IExpression) visit(e))
                    .collect(toList());

            // TODO: This could be keep as a list to avoid building a tree
            for (IExpression r : rightList)
            {
                if (result == null)
                {
                    result = new LogicalBinaryExpression(type, le, r).fold();
                }
                else
                {
                    result = new LogicalBinaryExpression(type, result, r).fold();
                }
            }

            return result;
        }

        private IExpression buildArithmeticTree(List<Token> op, ParserRuleContext left, List<? extends ParserRuleContext> right)
        {
            IExpression leftE = getExpression(left);

            // Fall through
            if (op.isEmpty())
            {
                return leftE;
            }

            // TODO: This could be keep as a list to avoid building a tree
            int count = right.size();
            IExpression result = null;
            for (int i = 0; i < count; i++)
            {
                IArithmeticBinaryExpression.Type type = ARITHMETIC_TYPES.get(op.get(i)
                        .getType());

                IExpression rightE = getExpression(right.get(i));

                if (result == null)
                {
                    result = new ArithmeticBinaryExpression(type, leftE, rightE).fold();
                }
                else
                {
                    result = new ArithmeticBinaryExpression(type, result, rightE).fold();
                }
            }

            return result;
        }

        private List<IExpression> getExpressionList(Expr_listContext ctx)
        {
            return ctx.expression()
                    .stream()
                    .map(e -> (IExpression) visit(e))
                    .collect(toList());
        }

        private ILogicalPlan wrapOperatorFunction(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (ctx.forClause() != null)
            {
                String catalogAlias = getIdentifier(ctx.forClause()
                        .functionName().catalog);
                String function = getIdentifier(ctx.forClause()
                        .functionName().function);

                return new OperatorFunctionScan(Schema.of(Column.of("output", ResolvedType.of(Type.Any))), plan, catalogAlias, function, Location.from(ctx.forClause()));
            }

            return plan;
        }

        private ILogicalPlan wrapTop(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (ctx.TOP() != null)
            {
                IExpression topExpression;
                if (ctx.topCount()
                        .expression() != null)
                {
                    topExpression = getExpression(ctx.topCount()
                            .expression());
                }
                else
                {
                    topExpression = LiteralExpression.createLiteralNumericExpression(ctx.topCount()
                            .NUMERIC_LITERAL()
                            .getText());
                }

                plan = new Limit(plan, topExpression);
            }
            return plan;
        }

        private ILogicalPlan wrapProjection(ILogicalPlan plan, SelectStatementContext ctx)
        {
            /* Aggregates has it's own projection */
            if (!ctx.groupBy.isEmpty())
            {
                return plan;
            }

            // Non qualified asterisk selects must have either a table source
            // or a for clause

            boolean containsNonQualifiedAsterisks = ctx.selectItem()
                    .stream()
                    .anyMatch(i -> i.ASTERISK() != null
                            && i.alias == null);

            boolean containsAsterisks = ctx.selectItem()
                    .stream()
                    .anyMatch(i -> i.ASTERISK() != null);
            if (containsAsterisks
                    && ctx.tableSourceJoined() == null
                    && ctx.forClause() == null)
            {
                throw new ParseException("Must specify table source", ctx);
            }
            else if (containsNonQualifiedAsterisks
                    && ctx.tableSourceJoined() == null)
            {
                throw new ParseException("Must specify table source", ctx);
            }
            else if (insideProjection
                    && ctx.forClause() == null
                    && (ctx.selectItem()
                            .size() > 1
                            || containsAsterisks))
            {
                throw new ParseException("Sub queries without a FOR clause must return a single select item", ctx);
            }
            else if (containsAsterisks
                    && !ctx.groupBy.isEmpty())
            {
                throw new ParseException("Cannot have asterisk select with GROUP BY's", ctx);
            }

            // One select item that is an asterisk one
            boolean isSelectAll = ctx.selectItem()
                    .size() == 1
                    && ctx.selectItem()
                            .get(0)
                            .ASTERISK() != null
                    && ctx.selectItem()
                            .get(0).alias == null;

            if (!isSelectAll)
            {
                boolean prevInsideProjection = insideProjection;
                insideProjection = true;
                List<IExpression> expressions = ctx.selectItem()
                        .stream()
                        .map(i -> (IExpression) visitSelectItem(i))
                        .collect(toList());
                insideProjection = prevInsideProjection;

                validateAssignmentProjection(expressions, ctx);

                plan = new Projection(plan, expressions);
            }

            return plan;
        }

        private ILogicalPlan wrapSort(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (!ctx.sortItem()
                    .isEmpty())
            {
                List<SortItem> orderBy = ctx.sortItem()
                        .stream()
                        .map(this::getSortItem)
                        .collect(toList());
                plan = new Sort(plan, orderBy);
            }
            return plan;
        }

        private ILogicalPlan wrapDistinct(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (ctx.DISTINCT() != null)
            {
                // Empty aggregate and projection
                // We will aggregate the whole input
                plan = new Aggregate(plan, emptyList(), emptyList());
            }

            return plan;
        }

        private ILogicalPlan wrapAggregate(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (!ctx.groupBy.isEmpty())
            {
                if (ctx.tableSourceJoined() == null)
                {
                    throw new ParseException("Cannot have a GROUP BY clause without a FROM.", ctx.groupBy.get(0));
                }

                boolean prevInsideProjection = insideProjection;
                insideProjection = true;
                List<IAggregateExpression> projectionExpressions = ctx.selectItem()
                        .stream()
                        .map(s -> getAggregateExpression(s))
                        .collect(toList());
                insideProjection = prevInsideProjection;

                validateAssignmentProjection(projectionExpressions, ctx);

                List<IExpression> aggregateExpressions = ctx.groupBy.stream()
                        .map(this::getExpression)
                        .collect(toList());
                plan = new Aggregate(plan, aggregateExpressions, projectionExpressions);
            }
            return plan;
        }

        private void validateAssignmentProjection(List<? extends IExpression> projections, SelectStatementContext ctx)
        {
            long assignmentItems = projections.stream()
                    .filter(e -> (e instanceof AssignmentExpression)
                            || (e instanceof AggregateWrapperExpression ae
                                    && ae.getExpression() instanceof AssignmentExpression))
                    .count();
            assignmentSelect = assignmentItems > 0;
            if (assignmentSelect
                    && assignmentItems != projections.size())
            {
                throw new ParseException("Cannot combine variable assignment items with data retrieval items", ctx.selectItem(0));
            }
            else if (assignmentSelect
                    && ctx.into != null)
            {
                throw new ParseException("Cannot have assignments in a SELECT INTO statement", ctx);
            }
            else if (assignmentSelect
                    && insideSubQuery)
            {
                throw new ParseException("Assignment selects are not allowed in sub query context", ctx);
            }
        }

        private ILogicalPlan getTableSource(SelectStatementContext ctx)
        {
            if (ctx.tableSourceJoined() != null)
            {
                return (ILogicalPlan) visit(ctx.tableSourceJoined());
            }
            else
            {
                // Fallback to constant scan to always have an operator to scan
                return ConstantScan.INSTANCE;
            }
        }

        private ILogicalPlan wrapFilter(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (ctx.where != null)
            {
                plan = new Filter(plan, null, getExpression(ctx.where));
            }
            return plan;
        }

        private ILogicalPlan wrapHaving(ILogicalPlan plan, SelectStatementContext ctx)
        {
            if (ctx.having != null)
            {
                plan = new Filter(plan, null, getExpression(ctx.having));
            }
            return plan;
        }

        private SortItem getSortItem(SortItemContext ctx)
        {
            IExpression expression = getExpression(ctx.expression());
            ISortItem.Order order = Order.ASC;
            ISortItem.NullOrder nullOrder = NullOrder.UNDEFINED;

            int orderType = ctx.order != null ? ctx.order.getType()
                    : -1;
            if (orderType == PayloadBuilderQueryLexer.DESC)
            {
                order = Order.DESC;
            }

            int nullOrderType = ctx.nullOrder != null ? ctx.nullOrder.getType()
                    : -1;
            if (nullOrderType == PayloadBuilderQueryLexer.FIRST)
            {
                nullOrder = NullOrder.FIRST;
            }
            else if (nullOrderType == PayloadBuilderQueryLexer.LAST)
            {
                nullOrder = NullOrder.LAST;
            }

            return new SortItem(expression, order, nullOrder, Location.from(ctx));
        }

        private String getIdentifier(ParserRuleContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }

            return getIdentifierString(ctx.getText());
        }

        private IExpression getExpression(ParserRuleContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }

            IExpression expression = (IExpression) visit(ctx);
            return expression.fold();
        }

        private IAggregateExpression getAggregateExpression(ParserRuleContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }

            IExpression expression = (IExpression) visit(ctx);
            expression.fold();

            // Function call already implements IAggregateExpression
            if (expression instanceof IAggregateExpression)
            {
                return (IAggregateExpression) expression;
            }

            // ... else wrap it
            // This will be resolved later to return single value if needed
            return new AggregateWrapperExpression(expression, false, false);
        }

        /** Cache qualifier with type and name */
        private static class CacheQualifier
        {
            CacheType type;
            QualifiedName name;
            boolean all;
        }

        private static class FunctionCallInfo
        {
            ScalarFunctionInfo.AggregateMode aggregateMode;
            String catalogAlias;
            String name;
            List<IExpression> arguments;

            FunctionCallInfo(String catalogAlias, String name, ScalarFunctionInfo.AggregateMode aggregateMode, List<IExpression> arguments)
            {
                this.catalogAlias = Objects.toString(catalogAlias, "");
                this.name = name;
                this.aggregateMode = aggregateMode;
                this.arguments = arguments;
            }
        }

        private IExpression getOldBuiltInFunction(ParserRuleContext ctx, FunctionCallInfo functionCallInfo)
        {
            if ("attimezone".equalsIgnoreCase(functionCallInfo.name))
            {
                addDeprecationWarning("Deprecated usage of ATTIMEZONE function. Use \"<expression> AT TIME ZONE <timezone-expression>\".", ctx);
                IExpression expression = functionCallInfo.arguments.get(0);
                IExpression timeZone = functionCallInfo.arguments.get(1);
                return new AtTimeZoneExpression(expression, timeZone);
            }

            return null;
        }

        private void addDeprecationWarning(String message, ParserRuleContext ctx)
        {
            if (warnings == null)
            {
                return;
            }

            warnings.add(new Warning(message, Location.from(ctx)));
        }
    }

    /** Construct a {@link LiteralExpression} from {@link LiteralContext} */
    public static IExpression getLiteralExpression(LiteralContext ctx)
    {
        if (ctx.NULL() != null)
        {
            // Actual type is unknown here
            return new LiteralNullExpression(ResolvedType.of(Type.Any));
        }
        else if (ctx.booleanLiteral() != null)
        {
            return ctx.booleanLiteral()
                    .TRUE() != null ? LiteralBooleanExpression.TRUE
                            : LiteralBooleanExpression.FALSE;
        }
        else if (ctx.numericLiteral() != null)
        {
            return LiteralExpression.createLiteralNumericExpression(ctx.numericLiteral()
                    .getText());
        }
        else if (ctx.decimalLiteral() != null)
        {
            return LiteralExpression.createLiteralDecimalExpression(ctx.decimalLiteral()
                    .getText());
        }
        else if (ctx.templateStringLiteral() != null)
        {
            return null;
        }

        String text = ctx.stringLiteral()
                .getText();
        text = text.substring(1, text.length() - 1);
        // Remove escaped single quotes
        text = text.replaceAll("''", "'");
        return new LiteralStringExpression(text);
    }

    /** Contruct a {@link QualifiedName} from {@link QnameContext} */
    public static QualifiedName getQualifiedName(QnameContext ctx)
    {
        if (ctx == null)
        {
            return null;
        }

        return getQualifiedName(ctx.identifier());
    }

    private static QualifiedName getQualifiedName(CacheNameContext ctx)
    {
        if (ctx == null)
        {
            return null;
        }

        return getQualifiedName(ctx.identifier());
    }

    private static QualifiedName getQualifiedName(List<IdentifierContext> identifier)
    {
        int size = identifier.size();
        List<String> parts = new ArrayList<>(size);
        identifier.stream()
                .map(i -> getIdentifierString(i.getText()))
                .forEach(i -> parts.add(i));

        return new QualifiedName(parts);
    }

    private static String getIdentifierString(String string)
    {
        String text = string;
        // Strip starting quotes
        if (text.charAt(0) == '"')
        {
            text = text.substring(1, text.length() - 1);
            // Remove escaped double quotes
            text = text.replaceAll("\"\"", "\"");
        }
        return text;
    }

}
