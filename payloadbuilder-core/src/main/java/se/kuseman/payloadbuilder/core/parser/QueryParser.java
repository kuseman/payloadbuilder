package se.kuseman.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.apache.commons.lang3.StringUtils.upperCase;
import static se.kuseman.payloadbuilder.core.parser.LiteralExpression.createLiteralDecimalExpression;
import static se.kuseman.payloadbuilder.core.parser.LiteralExpression.createLiteralNumericExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.core.cache.CacheType;
import se.kuseman.payloadbuilder.core.parser.Apply.ApplyType;
import se.kuseman.payloadbuilder.core.parser.CaseExpression.WhenClause;
import se.kuseman.payloadbuilder.core.parser.Join.JoinType;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.AnalyzeStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ArithmeticBinaryContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ArithmeticUnaryContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.BracketExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CacheFlushContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CacheNameContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CacheRemoveContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.CaseExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ColumnReferenceContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ComparisonExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DereferenceContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DescribeStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.DropTableStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FullCacheQualifierContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionArgumentContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.IdentifierContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.IfStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.InExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.JoinPartContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LambdaExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LikeExpressionContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LiteralContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LogicalBinaryContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.LogicalNotContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.NullPredicateContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.PrintStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.QnameContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.QueryContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SelectItemContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SelectStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SetStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.ShowStatementContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SortItemContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.SubscriptContext;
import se.kuseman.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableNameContext;
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
import se.kuseman.payloadbuilder.core.parser.Select.For;

/** Parser for a payload builder query */
public class QueryParser
{
    /** Parse query */
    public QueryStatement parseQuery(String query)
    {
        return getTree(query, p -> p.query());
    }

    /** Parse select */
    public Select parseSelect(String query)
    {
        return getTree(query, p -> p.topSelect());
    }

    /** Parse expression */
    public Expression parseExpression(String expression)
    {
        return getTree(expression, p -> p.topExpression());
    }

    @SuppressWarnings("unchecked")
    private <T> T getTree(String body, Function<PayloadBuilderQueryParser, ParserRuleContext> function)
    {
        BaseErrorListener errorListener = new BaseErrorListener()
        {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
            {
                throw new ParseException(msg, line, charPositionInLine + 1);
            }
        };

        CharStream charStream = CharStreams.fromString(body);
        PayloadBuilderQueryLexer lexer = new PayloadBuilderQueryLexer(charStream);
        lexer.getErrorListeners()
                .clear();
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);
        PayloadBuilderQueryParser parser = new PayloadBuilderQueryParser(tokens);
        parser.getErrorListeners()
                .clear();
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

        return (T) new AstBuilder().visit(tree);
    }

    /** Builds tree */
    private static class AstBuilder extends PayloadBuilderQueryParserBaseVisitor<Object>
    {
        /** Lambda parameters and slot id in current scope */
        private final Map<String, Integer> lambdaParameters = new HashMap<>();
        private Expression leftDereference;

        private boolean insideSelectItems;
        private boolean isRootSelectStatement;

        @Override
        public Object visitStatement(PayloadBuilderQueryParser.StatementContext ctx)
        {
            // Clear root flag outside of clear function because it's called from within sub query building
            isRootSelectStatement = true;
            return super.visitStatement(ctx);
        }

        @Override
        public Object visitIfStatement(IfStatementContext ctx)
        {
            Expression condition = getExpression(ctx.condition);
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
            boolean systemProperty = ctx.AT() == null;
            return new SetStatement(lowerCase(join(getQualifiedName(ctx.qname()).getParts(), ".")), getExpression(ctx.expression()), systemProperty);
        }

        @Override
        public Object visitUseStatement(UseStatementContext ctx)
        {
            QualifiedName qname = getQualifiedName(ctx.qname());
            Expression expression = ctx.expression() != null ? getExpression(ctx.expression())
                    : null;
            if (expression != null
                    && qname.getParts()
                            .size() == 1)
            {
                throw new ParseException("Cannot assign value to a catalog alias", ctx.start);
            }
            else if (expression == null
                    && qname.getParts()
                            .size() > 1)
            {
                throw new ParseException("Must provide an assignment value to a catalog property", ctx.start);
            }
            return new UseStatement(qname, expression);
        }

        @Override
        public Object visitDescribeStatement(DescribeStatementContext ctx)
        {
            if (ctx.tableName() != null)
            {
                String catalog = lowerCase(ctx.tableName().catalog != null ? ctx.tableName().catalog.getText()
                        : null);
                return new DescribeTableStatement(catalog, getQualifiedName(ctx.tableName()
                        .qname()), ctx.start);
            }

            return new DescribeSelectStatement((SelectStatement) visit(ctx.selectStatement()));
        }

        @Override
        public Object visitAnalyzeStatement(AnalyzeStatementContext ctx)
        {
            return new AnalyzeStatement((SelectStatement) visit(ctx.selectStatement()));
        }

        @Override
        public Object visitShowStatement(ShowStatementContext ctx)
        {
            String catalog = getIdentifier(ctx.catalog);
            ShowStatement.Type type = ShowStatement.Type.valueOf(upperCase(ctx.getChild(ctx.getChildCount() - 1)
                    .getText()));
            return new ShowStatement(type, catalog, ctx.start);
        }

        @Override
        public Object visitCacheFlush(CacheFlushContext ctx)
        {
            CacheQualifier cache = (CacheQualifier) visit(ctx.cache);
            Expression key = getExpression(ctx.expression());
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
            return ((SelectStatement) visit(ctx.selectStatement())).getSelect();
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
            return new DropTableStatement(catalogAlias, qname, lenient, tempTable, ctx.start);
        }

        // CSOFF
        @Override
        // CSON
        public Object visitSelectStatement(SelectStatementContext ctx)
        {
            if (isRootSelectStatement
                    && ctx.forClause() != null)
            {
                throw new ParseException("FOR clause are not allowed in top select", ctx.forClause().start);
            }
            isRootSelectStatement = false;

            TableSourceJoined from = ctx.tableSourceJoined() != null ? (TableSourceJoined) visit(ctx.tableSourceJoined())
                    : null;
            final Expression topExpression = ctx.topCount() != null ? (Expression) visit(ctx.topCount())
                    : null;
            if (from == null)
            {
                if (!ctx.groupBy.isEmpty())
                {
                    throw new ParseException("Cannot have a GROUP BY clause without a FROM.", ctx.groupBy.get(0).start);
                }
            }

            boolean prevInsideSelectItems = insideSelectItems;
            insideSelectItems = true;
            List<SelectItem> selectItems = ctx.selectItem()
                    .stream()
                    .map(s -> (SelectItem) visit(s))
                    .collect(toList());
            insideSelectItems = prevInsideSelectItems;

            boolean assignmentSelect = false;
            /* Verify/determine assignment select */
            if (selectItems.stream()
                    .anyMatch(s -> s.getAssignmentName() != null))
            {
                if (selectItems.stream()
                        .anyMatch(s -> s.getAssignmentName() == null))
                {
                    throw new ParseException("Cannot combine variable assignment items with data retrieval items", ctx.start);
                }
                assignmentSelect = true;
            }

            Table into = null;
            if (ctx.into != null)
            {
                if (assignmentSelect)
                {
                    throw new ParseException("Cannot have assignments in a SELECT INTO statement", ctx.start);
                }

                if (selectItems.stream()
                        .anyMatch(i -> isBlank(i.getIdentifier())
                                && i.isEmptyIdentifier()))
                {
                    throw new ParseException("All items must have an identifier when using a SELECT INTO statement", ctx.start);
                }

                if (selectItems.stream()
                        .anyMatch(SelectItem::isAsterisk))
                {
                    throw new ParseException("Cannot have asterisk (*) select items when usnig a SELECT INTO statement", ctx.start);
                }

                TableName tableName = (TableName) visit(ctx.tableName());
                TableAlias.Type type = tableName.isTempTable() ? TableAlias.Type.TEMPORARY_TABLE
                        : TableAlias.Type.TABLE;
                TableAlias alias = TableAlias.TableAliasBuilder.of(-1, type, getQualifiedName(ctx.into.qname()), "")
                        .build();
                List<Option> options = ctx.intoOptions != null ? ctx.intoOptions.options.stream()
                        .map(to -> (Option) visit(to))
                        .collect(toList())
                        : emptyList();
                into = new Table("", alias, options, ctx.into.start);
            }

            Expression where = getExpression(ctx.where);
            List<Expression> groupBy = ctx.groupBy != null ? ctx.groupBy.stream()
                    .map(si -> getExpression(si))
                    .collect(toList())
                    : emptyList();
            List<SortItem> orderBy = ctx.sortItem() != null ? ctx.sortItem()
                    .stream()
                    .map(si -> getSortItem(si))
                    .collect(toList())
                    : emptyList();
            Select.For forOutput = ctx.forClause() != null ? Select.For.valueOf(upperCase(ctx.forClause().output.getText()))
                    : null;

            Select select = new Select(selectItems, from, into, topExpression, where, groupBy, orderBy, forOutput, emptyList());
            return new SelectStatement(select, assignmentSelect);
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
            // Expression select item
            if (ctx.expression() != null)
            {
                String identifier = getIdentifier(ctx.identifier());
                QualifiedName assignmentQname = ctx.variable() != null ? getQualifiedName(ctx.variable()
                        .qname())
                        : null;
                if (ctx.variable() != null
                        && ctx.variable().system != null)
                {
                    throw new ParseException("Cannot assign to system variables", ctx.start);
                }
                Expression expression = getExpression(ctx.expression());

                String assignmentName = assignmentQname != null ? join(assignmentQname.getParts(), ".")
                        : null;

                boolean emptyIdentifier = isBlank(identifier);
                if (isBlank(identifier)
                        && expression instanceof HasIdentifier)
                {
                    identifier = ((HasIdentifier) expression).identifier();
                }

                return new ExpressionSelectItem(expression, emptyIdentifier, defaultIfBlank(identifier, ""), assignmentName, ctx.expression().start);
            }
            // Asterisk select item
            else if (ctx.ASTERISK() != null)
            {
                String alias = null;
                if (ctx.alias != null)
                {
                    alias = getIdentifier(ctx.alias);
                }
                return new AsteriskSelectItem(alias, ctx.start);
            }

            throw new ParseException("Caould no create a select item.", ctx.start);
        }

        @Override
        public Object visitTableSourceJoined(TableSourceJoinedContext ctx)
        {
            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            String alias = getIdentifier(ctx.tableSource()
                    .identifier());
            if (ctx.joinPart()
                    .size() > 0
                    && isBlank(alias))
            {
                throw new ParseException("Alias is mandatory.", ctx.tableSource().start);
            }

            List<AJoin> joins = ctx.joinPart()
                    .stream()
                    .map(j -> (AJoin) visit(j))
                    .collect(toList());

            return new TableSourceJoined(tableSource, joins);
        }

        @Override
        public Object visitJoinPart(JoinPartContext ctx)
        {
            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            String alias = getIdentifier(ctx.tableSource()
                    .identifier());
            if (isBlank(alias))
            {
                throw new ParseException("Alias is mandatory", ctx.tableSource().start);
            }
            if (ctx.JOIN() != null)
            {
                Expression condition = getExpression(ctx.expression());
                Join.JoinType joinType = ctx.INNER() != null ? JoinType.INNER
                        : JoinType.LEFT;
                return new Join(tableSource, joinType, condition);
            }

            ApplyType applyType = ctx.OUTER() != null ? ApplyType.OUTER
                    : ApplyType.CROSS;
            return new Apply(tableSource, applyType);
        }

        @Override
        public Object visitTableSource(TableSourceContext ctx)
        {
            String alias = defaultIfBlank(getIdentifier(ctx.identifier()), "");

            List<Option> options = ctx.tableSourceOptions() != null ? ctx.tableSourceOptions().options.stream()
                    .map(to -> (Option) visit(to))
                    .collect(toList())
                    : emptyList();
            if (ctx.functionCall() != null)
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                TableAlias tableAlias = TableAlias.TableAliasBuilder.of(-1, TableAlias.Type.FUNCTION, QualifiedName.of(functionCallInfo.name), alias)
                        .build();
                return new UnresolvedTableFunction(functionCallInfo.catalogAlias, functionCallInfo.name, tableAlias, functionCallInfo.arguments, options, ctx.functionCall().start);
            }
            else if (ctx.selectStatement() != null)
            {
                final Token token = ctx.selectStatement().start;

                if (isBlank(alias))
                {
                    throw new ParseException("Sub query must have an alias", ctx.PARENC()
                            .getSymbol());
                }

                validateTableSourceSubQuery(ctx.selectStatement());
                SelectStatement selectStatement = (SelectStatement) visit(ctx.selectStatement());
                SelectItem noIdentifierItem = selectStatement.getSelect()
                        .getSelectItems()
                        .stream()
                        .filter(i -> !(i instanceof AsteriskSelectItem)
                                && isBlank(i.getIdentifier()))
                        .findAny()
                        .orElse(null);

                if (noIdentifierItem != null)
                {
                    throw new ParseException("Missing identifier for select item", noIdentifierItem.getToken());
                }

                TableAlias tableAlias = TableAliasBuilder.of(-1, TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), alias)
                        .build();
                return new SubQueryTableSource(tableAlias, selectStatement.getSelect(), options, token);
            }

            TableName tableName = (TableName) visit(ctx.tableName());
            TableAlias.Type type = tableName.isTempTable() ? TableAlias.Type.TEMPORARY_TABLE
                    : TableAlias.Type.TABLE;
            TableAlias tableAlias = TableAliasBuilder.of(-1, type, tableName.getQname(), defaultIfBlank(alias, ""))
                    .build();
            return new Table(tableName.getCatalogAlias(), tableAlias, options, ctx.tableName().start);
        }

        @Override
        public Object visitTableName(TableNameContext ctx)
        {
            String catalogAlias = ctx.catalog != null ? ctx.catalog.getText()
                    : null;
            QualifiedName qname = getQualifiedName(ctx.qname());
            boolean tempTable = ctx.tempHash != null;
            return new TableName(catalogAlias, qname, tempTable);
        }

        @Override
        public Object visitTableSourceOption(TableSourceOptionContext ctx)
        {
            QualifiedName option = getQualifiedName(ctx.qname());
            Expression valueExpression = getExpression(ctx.expression());
            return new Option(option, valueExpression);
        }

        @Override
        public Object visitCaseExpression(CaseExpressionContext ctx)
        {
            List<WhenClause> whenClauses = ctx.when()
                    .stream()
                    .map(w -> new CaseExpression.WhenClause(getExpression(w.condition), getExpression(w.result)))
                    .collect(toList());

            Expression elseExpression = getExpression(ctx.elseExpr);

            return new CaseExpression(whenClauses, elseExpression);
        }

        @Override
        public Object visitColumnReference(ColumnReferenceContext ctx)
        {
            QualifiedName qname = getQualifiedName(ctx.qname());
            int lambdaId = lambdaParameters.getOrDefault(qname.getFirst(), -1);
            UnresolvedQualifiedReferenceExpression result = new UnresolvedQualifiedReferenceExpression(qname, lambdaId, ctx.start);
            return result;
        }

        @Override
        public Object visitVariableExpression(VariableExpressionContext ctx)
        {
            return new VariableExpression(getQualifiedName(ctx.variable()
                    .qname()), ctx.variable().system != null);
        }

        @Override
        public Object visitFunctionCallExpression(FunctionCallExpressionContext ctx)
        {
            FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
            return new UnresolvedQualifiedFunctionCallExpression(functionCallInfo.catalogAlias, functionCallInfo.name, functionCallInfo.arguments, ctx.functionCall().start);
        }

        @Override
        public Object visitSubscript(SubscriptContext ctx)
        {
            Expression value = getExpression(ctx.value);
            Expression subscript = getExpression(ctx.subscript);
            return new SubscriptExpression(value, subscript);
        }

        @Override
        public Object visitFunctionCall(FunctionCallContext ctx)
        {
            String catalog = lowerCase(ctx.functionName().catalog != null ? ctx.functionName().catalog.getText()
                    : null);
            String functionName = getIdentifier(ctx.functionName().function);

            // Store left dereference
            Expression prevLeftDereference = leftDereference;
            leftDereference = null;

            List<Expression> arguments = ctx.arguments.stream()
                    .map(a -> getExpression(a))
                    .collect(toList());

            /*
             * Wrap argument in a dereference expression if we are inside a dereference Ie. Left dereference: func(field)
             *
             * Right: field.func2()
             *
             * Rewrite to:
             *
             * func2(func(field).field)
             */
            if (prevLeftDereference != null)
            {
                arguments.add(0, prevLeftDereference);
            }

            return new FunctionCallInfo(catalog, functionName, arguments);
        }

        @Override
        public Object visitFunctionArgument(FunctionArgumentContext ctx)
        {
            Expression expression = getExpression(ctx.expression);
            String name = getIdentifier(ctx.name);
            return name != null ? new NamedExpression(name, expression)
                    : expression;
        }

        @Override
        public Object visitComparisonExpression(ComparisonExpressionContext ctx)
        {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            IComparisonExpression.Type type = null;

            switch (ctx.op.getType())
            {
                case PayloadBuilderQueryLexer.EQUALS:
                    type = IComparisonExpression.Type.EQUAL;
                    break;
                case PayloadBuilderQueryLexer.NOTEQUALS:
                    type = IComparisonExpression.Type.NOT_EQUAL;
                    break;
                case PayloadBuilderQueryLexer.LESSTHAN:
                    type = IComparisonExpression.Type.LESS_THAN;
                    break;
                case PayloadBuilderQueryLexer.LESSTHANEQUAL:
                    type = IComparisonExpression.Type.LESS_THAN_EQUAL;
                    break;
                case PayloadBuilderQueryLexer.GREATERTHAN:
                    type = IComparisonExpression.Type.GREATER_THAN;
                    break;
                case PayloadBuilderQueryLexer.GREATERTHANEQUAL:
                    type = IComparisonExpression.Type.GREATER_THAN_EQUAL;
                    break;
                default:
                    throw new RuntimeException("Unkown comparison operator");
            }

            return new ComparisonExpression(type, left, right);
        }

        @Override
        public Object visitLogicalBinary(LogicalBinaryContext ctx)
        {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            LogicalBinaryExpression.Type type = ctx.AND() != null ? LogicalBinaryExpression.Type.AND
                    : LogicalBinaryExpression.Type.OR;

            return new LogicalBinaryExpression(type, left, right);
        }

        @Override
        public Object visitLogicalNot(LogicalNotContext ctx)
        {
            return new LogicalNotExpression(getExpression(ctx.expression()));
        }

        @Override
        public Object visitLikeExpression(LikeExpressionContext ctx)
        {
            boolean not = ctx.NOT() != null;
            return new LikeExpression(getExpression(ctx.left), getExpression(ctx.right), not, getExpression(ctx.escape));
        }

        @Override
        public Object visitInExpression(InExpressionContext ctx)
        {
            return new InExpression(getExpression(ctx.expression(0)), ctx.expression()
                    .stream()
                    .skip(1)
                    .map(e -> getExpression(e))
                    .collect(toList()), ctx.NOT() != null);
        }

        @Override
        public Object visitBracketExpression(BracketExpressionContext ctx)
        {
            // Plain nested parenthesis expression
            if (ctx.bracket_expression()
                    .expression() != null)
            {
                return new NestedExpression(getExpression(ctx.bracket_expression()
                        .expression()));
            }

            /*
             * Prohibit sub query expressions every where BUT in select items will be changed when EXISTS comes into play
             *
             * select * from ( select col1 , ( select col2, col3 for object) myObj <-- here is ok from table where (select col4 .....) <-- not ok ) x
             *
             */

            if (!insideSelectItems)
            {
                throw new ParseException("Subquery expressions are only allowed in select items", ctx.bracket_expression().start);
            }

            validateSelectItemSubQuery(ctx.bracket_expression()
                    .selectStatement());

            TableAlias tableAlias = TableAliasBuilder.of(-1, TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), "")
                    .build();

            SelectStatement selectStatement = (SelectStatement) visit(ctx.bracket_expression()
                    .selectStatement());

            For forOutput = selectStatement.getSelect()
                    .getForOutput();
            for (SelectItem item : selectStatement.getSelect()
                    .getSelectItems())
            {
                if (forOutput == For.ARRAY
                        && !item.isEmptyIdentifier())
                {
                    throw new ParseException("All select items in ARRAY output must have empty identifiers", item.getToken());
                }
                else if ((forOutput == For.OBJECT
                        || forOutput == For.OBJECT_ARRAY)
                        && isBlank(item.getIdentifier()))
                {
                    throw new ParseException("All select items in OBJECT output must have identifiers", item.getToken());
                }
            }

            return new UnresolvedSubQueryExpression(selectStatement, tableAlias, ctx.bracket_expression().start);
        }

        @Override
        public Object visitNullPredicate(NullPredicateContext ctx)
        {
            return new NullPredicateExpression(getExpression(ctx.expression()), ctx.NOT() != null);
        }

        @Override
        public Object visitDereference(DereferenceContext ctx)
        {
            Expression left = getExpression(ctx.left);
            leftDereference = left;

            Expression result;
            // Dereferenced field
            if (ctx.identifier() != null)
            {
                List<String> parts = new ArrayList<>();
                parts.add(getIdentifier(ctx.identifier()));
                result = new UnresolvedDereferenceExpression(left, new UnresolvedQualifiedReferenceExpression(new QualifiedName(parts), -1, ctx.start));
            }
            // Dereferenced function call
            else
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                result = new UnresolvedQualifiedFunctionCallExpression(functionCallInfo.catalogAlias, functionCallInfo.name, functionCallInfo.arguments, ctx.functionCall().start);
            }

            leftDereference = null;
            return result;
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
                    throw new ParseException("Lambda identifier " + i + " is already defined in scope.", ctx.start);
                }

                lambdaParameters.put(i, lambdaParameters.size());
            });

            Expression expression = getExpression(ctx.expression());
            int[] uniqueLambdaIds = new int[identifiers.size()];
            for (int i = 0; i < identifiers.size(); i++)
            {
                uniqueLambdaIds[i] = lambdaParameters.size() - 1 + i;
            }
            identifiers.forEach(i -> lambdaParameters.remove(i));
            return new LambdaExpression(identifiers, expression, uniqueLambdaIds);
        }

        @Override
        public Object visitArithmeticUnary(ArithmeticUnaryContext ctx)
        {
            Expression expression = getExpression(ctx.expression());
            ArithmeticUnaryExpression.Type type = null;

            // CSOFF
            switch (ctx.op.getType())
            // CSOn
            {
                case PayloadBuilderQueryLexer.PLUS:
                    type = ArithmeticUnaryExpression.Type.PLUS;
                    break;
                case PayloadBuilderQueryLexer.MINUS:
                    type = ArithmeticUnaryExpression.Type.MINUS;
                    break;
            }

            return new ArithmeticUnaryExpression(type, expression);
        }

        @Override
        public Object visitArithmeticBinary(ArithmeticBinaryContext ctx)
        {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            ArithmeticBinaryExpression.Type type = null;

            switch (ctx.op.getType())
            {
                case PayloadBuilderQueryLexer.PLUS:
                    type = ArithmeticBinaryExpression.Type.ADD;
                    break;
                case PayloadBuilderQueryLexer.MINUS:
                    type = ArithmeticBinaryExpression.Type.SUBTRACT;
                    break;
                case PayloadBuilderQueryLexer.ASTERISK:
                    type = ArithmeticBinaryExpression.Type.MULTIPLY;
                    break;
                case PayloadBuilderQueryLexer.SLASH:
                    type = ArithmeticBinaryExpression.Type.DIVIDE;
                    break;
                case PayloadBuilderQueryLexer.PERCENT:
                    type = ArithmeticBinaryExpression.Type.MODULUS;
                    break;
                default:
                    throw new RuntimeException("Unkown artithmetic operator");
            }

            return new ArithmeticBinaryExpression(type, left, right);
        }

        @Override
        public Object visitLiteral(LiteralContext ctx)
        {
            if (ctx.NULL() != null)
            {
                return LiteralNullExpression.NULL_LITERAL;
            }
            else if (ctx.booleanLiteral() != null)
            {
                return ctx.booleanLiteral()
                        .TRUE() != null ? LiteralBooleanExpression.TRUE_LITERAL
                                : LiteralBooleanExpression.FALSE_LITERAL;
            }
            else if (ctx.numericLiteral() != null)
            {
                return createLiteralNumericExpression(ctx.numericLiteral()
                        .getText());
            }
            else if (ctx.decimalLiteral() != null)
            {
                return createLiteralDecimalExpression(ctx.decimalLiteral()
                        .getText());
            }
            else if (ctx.templateStringLiteral() != null)
            {
                return visit(ctx.templateStringLiteral());
            }

            String text = ctx.stringLiteral()
                    .getText();
            text = text.substring(1, text.length() - 1);
            // Remove escaped single quotes
            text = text.replaceAll("''", "'");
            return new LiteralStringExpression(text);
        }

        @Override
        public Object visitTemplateStringLiteral(TemplateStringLiteralContext ctx)
        {
            List<Expression> expressions = new ArrayList<>();
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

        /**
         * Validates a subquery when used as an expression
         */
        private void validateSelectItemSubQuery(SelectStatementContext ctx)
        {
            // Since sub query expressions are only allowed in select items, a FOR clause is then mandatory to make this a scalar
            // expression simply
            if (ctx.forClause() == null)
            {
                throw new ParseException("A FOR clause is mandatory when using a subquery expressions", ctx.start);
            }
            else if (ctx.into != null)
            {
                throw new ParseException("SELECT INTO are not allowed in sub query expressions", ctx.into.start);
            }

            for (SelectItemContext item : ctx.selectItem())
            {
                if (item.variable() != null)
                {
                    throw new ParseException("Assignment selects are not allowed in sub query expressions", item.start);
                }
            }
        }

        /**
         * Validates a subquery when used as a table source
         *
         * <pre>
         * Valid select items in a sub query
         *  - One or zero NON alias asterisk selects
         *  - No sub query select items
         * </pre>
         */
        private void validateTableSourceSubQuery(SelectStatementContext ctx)
        {
            if (ctx.into != null)
            {
                throw new ParseException("SELECT INTO are not allowed in sub query context", ctx.into.start);
            }
            else if (ctx.forClause() != null)
            {
                throw new ParseException("FOR clause are not allowed in sub query context", ctx.forClause().start);
            }

            Set<String> seenColumns = new HashSet<>();
            boolean foundAsteriskSelect = false;
            for (SelectItemContext item : ctx.selectItem())
            {
                if (item.ASTERISK() != null)
                {
                    if (foundAsteriskSelect)
                    {
                        throw new ParseException("Only a single asterisk select (*) are supported in sub queries", item.start);
                    }
                    else if (item.alias != null)
                    {
                        throw new ParseException("Only non alias asterisk select (*) are supported in sub queries", item.start);
                    }

                    foundAsteriskSelect = true;
                }
                else if (item.variable() != null)
                {
                    throw new ParseException("Assignment selects are not allowed in sub query context", item.start);
                }

                String identifier = getIdentifier(item.identifier());

                if (!isBlank(identifier)
                        && !seenColumns.add(lowerCase(identifier)))
                {
                    throw new ParseException("Column '" + identifier + "' is defined multiple times", item.start);
                }
            }
        }

        private SortItem getSortItem(SortItemContext ctx)
        {
            Expression expression = getExpression(ctx.expression());
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

            return new SortItem(expression, order, nullOrder, ctx.start);
        }

        private String getIdentifier(ParserRuleContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }
            return getIdentifierString(ctx.getText());
        }

        private String getIdentifierString(String string)
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

        private Expression getExpression(ParserRuleContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }

            Expression expression = (Expression) visit(ctx);
            return expression.fold();
        }

        private QualifiedName getQualifiedName(QnameContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }
            return getQualifiedName(ctx.identifier());
        }

        private QualifiedName getQualifiedName(CacheNameContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }
            return getQualifiedName(ctx.identifier());
        }

        private QualifiedName getQualifiedName(List<IdentifierContext> identifier)
        {
            int size = identifier.size();
            List<String> parts = new ArrayList<>(size);
            identifier.stream()
                    .map(i -> getIdentifierString(i.getText()))
                    .forEach(i -> parts.add(i));

            return new QualifiedName(parts);
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
            String catalogAlias;
            String name;
            List<Expression> arguments;

            FunctionCallInfo(String catalogAlias, String name, List<Expression> arguments)
            {
                this.catalogAlias = defaultString(catalogAlias, "");
                this.name = name;
                this.arguments = arguments;
            }
        }
    }
}
