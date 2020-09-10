package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.apache.commons.lang3.StringUtils.upperCase;
import static org.kuse.payloadbuilder.core.parser.LiteralExpression.createLiteralDecimalExpression;
import static org.kuse.payloadbuilder.core.parser.LiteralExpression.createLiteralNumericExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Apply.ApplyType;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression.Type;
import org.kuse.payloadbuilder.core.parser.Join.JoinType;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ArithmeticBinaryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ArithmeticUnaryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ColumnReferenceContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ComparisonExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.DereferenceContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.DescribeStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionArgumentContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.IfStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.InExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.JoinPartContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.LambdaExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.LiteralContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.LogicalBinaryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.LogicalNotContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.NestedExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.NullPredicateContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.PrintStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.QnameContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.QueryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SelectItemContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SelectStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SetStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ShowStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SortItemContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.StatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SubQueryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SubscriptContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableSourceContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableSourceJoinedContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableSourceOptionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TopCountContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TopExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TopSelectContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.UseStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.VariableExpressionContext;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

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
        lexer.getErrorListeners().clear();
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);
        PayloadBuilderQueryParser parser = new PayloadBuilderQueryParser(tokens);
        parser.getErrorListeners().clear();
        parser.addErrorListener(errorListener);

        ParserRuleContext tree;
        try
        {
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = function.apply(parser);
        }
        catch (ParseCancellationException ex)
        {
            // if we fail, parse with LL mode
            tokens.seek(0); // rewind input stream
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = function.apply(parser);
        }

        return (T) new AstBuilder().visit(tree);
    }

    /** Builds tree */
    private static class AstBuilder extends PayloadBuilderQueryBaseVisitor<Object>
    {
        /**
         * Function identifier counter. Ticks up for each unique catalog/function name combo. This to be able to cache function lookup by id
         */
        private int functionId;
        private final Map<Pair<String, String>, Integer> functionIdByName = new HashMap<>();

        /** Lambda parameters and slot id in current scope */
        private final Map<String, Integer> lambdaParameters = new HashMap<>();
        private Expression leftDereference;

        private String parentAlias;
        private TableAlias parentTableAlias;
        private TableAlias currentTableAlias;

        private void clear()
        {
            parentAlias = null;
            parentTableAlias = null;
            currentTableAlias = null;
        }

        @Override
        public Object visitStatement(StatementContext ctx)
        {
            clear();
            return super.visitStatement(ctx);
        }

        @Override
        public Object visitIfStatement(IfStatementContext ctx)
        {
            Expression condition = getExpression(ctx.condition);
            List<Statement> statements = ctx.stms.stms.stream().map(s -> (Statement) visit(s)).collect(toList());
            List<Statement> elseStatements = ctx.elseStatements != null ? ctx.elseStatements.stms.stream().map(s -> (Statement) visit(s)).collect(toList()) : emptyList();
            return new IfStatement(condition, statements, elseStatements);
        }

        @Override
        public Object visitQuery(QueryContext ctx)
        {
            List<Statement> statements = ctx.statements().children.stream().map(c -> (Statement) visit(c)).collect(toList());
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
            return new SetStatement(getQualifiedName(ctx.qname()), getExpression(ctx.expression()));
        }

        @Override
        public Object visitUseStatement(UseStatementContext ctx)
        {
            QualifiedName qname = getQualifiedName(ctx.qname());
            Expression expression = ctx.expression() != null ? getExpression(ctx.expression()) : null;
            if (expression != null && qname.getParts().size() == 1)
            {
                throw new ParseException("Cannot assign default catalog a value", ctx.start);
            }
            else if (expression == null && qname.getParts().size() > 1)
            {
                throw new ParseException("Must provide an assignment value to a catalog property.", ctx.start);
            }
            return new UseStatement(qname, expression);
        }

        @Override
        public Object visitDescribeStatement(DescribeStatementContext ctx)
        {
            if (ctx.tableName() != null)
            {
                String catalog = lowerCase(ctx.tableName().catalog != null ? ctx.tableName().catalog.getText() : null);
                return new DescribeTableStatement(catalog, getQualifiedName(ctx.tableName().qname()));
            }

            return new DescribeSelectStatement((SelectStatement) visit(ctx.selectStatement()));
        }

        @Override
        public Object visitShowStatement(ShowStatementContext ctx)
        {
            String catalog = getIdentifier(ctx.catalog);
            ShowStatement.Type type = ShowStatement.Type.valueOf(upperCase(ctx.getChild(ctx.getChildCount() - 1).getText()));
            return new ShowStatement(type, catalog, ctx.start);
        }

        @Override
        public Object visitTopSelect(TopSelectContext ctx)
        {
            return ((SelectStatement) visit(ctx.selectStatement())).getSelect();
        }

        @Override
        public Object visitSelectStatement(SelectStatementContext ctx)
        {
            List<SelectItem> selectItems = ctx.selectItem().stream().map(s -> (SelectItem) visit(s)).collect(toList());

            TableSourceJoined joinedTableSource = ctx.tableSourceJoined() != null ? (TableSourceJoined) visit(ctx.tableSourceJoined()) : null;
            Expression topExpression = ctx.topCount() != null ? (Expression) visit(ctx.topCount()) : null;
            if (joinedTableSource != null && joinedTableSource.getTableSource() instanceof SubQueryTableSource)
            {
                throw new ParseException("Top table source cannot be a populating table source.", ctx.tableSourceJoined().start);
            }
            else if (joinedTableSource == null)
            {
                if (ctx.where != null)
                {
                    throw new ParseException("Cannot have a WHERE clause without a FROM.", ctx.where.start);
                }
                else if (!ctx.groupBy.isEmpty())
                {
                    throw new ParseException("Cannot have a GROUP BY clause without a FROM.", ctx.groupBy.get(0).start);
                }
                else if (!ctx.sortItem().isEmpty())
                {
                    throw new ParseException("Cannot have a ORDER BY clause without a FROM.", ctx.sortItem().get(0).start);
                }
                else if (topExpression != null)
                {
                    throw new ParseException("Cannot have a TOP clause without a FROM.", ctx.TOP().getSymbol());
                }
            }

            Expression where = getExpression(ctx.where);
            List<Expression> groupBy = ctx.groupBy != null ? ctx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
            List<SortItem> orderBy = ctx.sortItem() != null ? ctx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();
            Select select = new Select(selectItems, joinedTableSource, topExpression, where, groupBy, orderBy);
            return new SelectStatement(select);
        }

        @Override
        public Object visitTopCount(TopCountContext ctx)
        {
            if (ctx.expression() != null)
            {
                return getExpression(ctx.expression());
            }
            return LiteralExpression.createLiteralNumericExpression(ctx.NUMERIC_LITERAL().getText());
        }

        @Override
        public Object visitTopExpression(TopExpressionContext ctx)
        {
            return getExpression(ctx.expression());
        }

        @Override
        public Object visitSelectItem(SelectItemContext ctx)
        {
            String identifier = getIdentifier(ctx.identifier());
            // Expression select item
            if (ctx.expression() != null)
            {
                Expression expression = getExpression(ctx.expression());
                return new ExpressionSelectItem(expression, identifier);
            }
            // Nested select item
            else if (ctx.nestedSelectItem() != null)
            {
                NestedSelectItem.Type type = ctx.OBJECT() != null ? NestedSelectItem.Type.OBJECT : NestedSelectItem.Type.ARRAY;
                List<SelectItem> selectItems = ctx.nestedSelectItem().selectItem().stream().map(s -> (SelectItem) visit(s)).collect(toList());
                Expression from = getExpression(ctx.nestedSelectItem().from);
                Expression where = getExpression(ctx.nestedSelectItem().where);
                List<Expression> groupBy = ctx.nestedSelectItem().groupBy != null
                    ? ctx.nestedSelectItem().groupBy
                            .stream()
                            .map(gb -> getExpression(gb))
                            .collect(toList())
                    : emptyList();
                List<SortItem> orderBy = ctx.nestedSelectItem().orderBy != null
                    ? ctx.nestedSelectItem().orderBy
                            .stream()
                            .map(si -> getSortItem(si))
                            .collect(toList())
                    : emptyList();

                if (type == NestedSelectItem.Type.ARRAY)
                {
                    Optional<SelectItem> item = selectItems.stream().filter(si -> !isBlank(si.getIdentifier()) && si.isExplicitIdentifier()).findAny();

                    if (item.isPresent())
                    {
                        int index = selectItems.indexOf(item.get());
                        SelectItemContext itemCtx = ctx.nestedSelectItem().selectItem(index);
                        throw new ParseException("Select items inside an ARRAY select cannot have aliaes. Item: " + item.get(), itemCtx.start);
                    }
                }
                else
                {
                    Optional<SelectItem> item = selectItems.stream().filter(si -> isBlank(si.getIdentifier())).findAny();

                    if (item.isPresent())
                    {
                        int index = selectItems.indexOf(item.get());
                        SelectItemContext itemCtx = ctx.nestedSelectItem().selectItem(index);
                        throw new ParseException("Select items inside an OBJECT select must have aliaes. Item: " + item.get(), itemCtx.start);
                    }
                }

                if (from == null)
                {
                    if (where != null)
                    {
                        throw new ParseException("Cannot have a WHERE clause without a FROM clause: " + selectItems, ctx.nestedSelectItem().where.start);
                    }
                    else if (!orderBy.isEmpty())
                    {
                        throw new ParseException("Cannot have an ORDER BY clause without a FROM clause: " + selectItems, ctx.nestedSelectItem().orderBy.get(0).start);
                    }
                    else if (!groupBy.isEmpty())
                    {
                        throw new ParseException("Cannot have an GROUP BY clause without a FROM clause: " + selectItems, ctx.nestedSelectItem().sortItem().get(0).start);
                    }
                }

                return new NestedSelectItem(type, selectItems, from, where, identifier, groupBy, orderBy);
            }
            // Wildcard select item
            else if (ctx.ASTERISK() != null)
            {
                String alias = null;
                if (ctx.alias != null)
                {
                    alias = getIdentifier(ctx.alias);
                }

                return new AsteriskSelectItem(alias);
            }

            throw new ParseException("Caould no create a select item.", ctx.start);
        }

        @Override
        public Object visitTableSourceJoined(TableSourceJoinedContext ctx)
        {
            //            String prevAlias = parentAlias;
            TableAlias prevParent = parentTableAlias;

            TableSource tableSource = (TableSource) visit(ctx.tableSource());

            //            parentAlias = tableSource.alias;
            parentTableAlias = currentTableAlias;

            List<AJoin> joins = ctx
                    .joinPart()
                    .stream()
                    .map(j ->
                    {
                        parentAlias = getIdentifier(j.tableSource().identifier());
                        //                        AJoin join = (AJoin) visit(j);
                        return (AJoin) visit(j);
                    })
                    .collect(toList());

            parentTableAlias = prevParent;
            //            parentAlias = prevAlias;

            return new TableSourceJoined(tableSource, joins);
        }

        @Override
        public Object visitJoinPart(JoinPartContext ctx)
        {
            if (ctx.JOIN() != null)
            {
                TableSource tableSource = (TableSource) visit(ctx.tableSource());
                Expression condition = getExpression(ctx.expression());
                Join.JoinType joinType = ctx.INNER() != null ? JoinType.INNER : JoinType.LEFT;
                return new Join(tableSource, joinType, condition);
            }

            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            ApplyType applyType = ctx.OUTER() != null ? ApplyType.OUTER : ApplyType.CROSS;
            return new Apply(tableSource, applyType);
        }

        private TableAlias getParentTableAlias()
        {
            return parentTableAlias;
        }

        @Override
        public Object visitTableSource(TableSourceContext ctx)
        {
            String alias = getIdentifier(ctx.identifier());

            List<Option> options = ctx.tableSourceOptions() != null ? ctx.tableSourceOptions().options.stream().map(to -> (Option) visit(to)).collect(toList()) : emptyList();
            if (ctx.functionCall() != null)
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                currentTableAlias = TableAlias.of(getParentTableAlias(), QualifiedName.of(functionCallInfo.function), defaultIfNull(parentAlias, defaultIfNull(alias, "")), ctx.functionCall().start);
                return new TableFunction(functionCallInfo.catalog, currentTableAlias, functionCallInfo.function, functionCallInfo.arguments/*, defaultIfNull(alias, "")*/, options,
                        functionCallInfo.functionId, ctx.functionCall().start);
            }
            else if (ctx.subQuery() != null)
            {
                if (isBlank(alias))
                {
                    throw new ParseException("Sub query must have an alias", ctx.subQuery().start);
                }

                SubQueryContext subQueryCtx = ctx.subQuery();

                TableSourceJoined tableSourceJoined = (TableSourceJoined) visit(subQueryCtx.tableSourceJoined());

                if (tableSourceJoined.getTableSource() instanceof SubQueryTableSource)
                {
                    throw new ParseException("Table source in populate query cannot be a populate table source", subQueryCtx.start);
                }

                Expression where = subQueryCtx.where != null ? getExpression(subQueryCtx.where) : null;
                List<Expression> groupBy = subQueryCtx.groupBy != null ? subQueryCtx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
                List<SortItem> orderBy = subQueryCtx.sortItem() != null ? subQueryCtx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();

                return new SubQueryTableSource(currentTableAlias, options, tableSourceJoined, where, groupBy, orderBy, ctx.subQuery().start);
            }

            QualifiedName table = getQualifiedName(ctx.tableName().qname());

            currentTableAlias = TableAlias.of(getParentTableAlias(), table, defaultIfNull(parentAlias, defaultIfNull(alias, "")), ctx.tableName().start);
            String catalog = ctx.tableName().catalog != null ? ctx.tableName().catalog.getText() : null;
            return new Table(catalog, currentTableAlias, options, ctx.tableName().start);
        }

        @Override
        public Object visitTableSourceOption(TableSourceOptionContext ctx)
        {
            QualifiedName option = getQualifiedName(ctx.qname());
            Expression valueExpression = getExpression(ctx.expression());
            return new Option(option, valueExpression);
        }

        @Override
        public Object visitColumnReference(ColumnReferenceContext ctx)
        {
            String identifier = getIdentifier(ctx.identifier());
            Integer lambdaId = lambdaParameters.get(identifier);
            return new QualifiedReferenceExpression(QualifiedName.of(identifier), lambdaId != null ? lambdaId.intValue() : -1);
        }

        @Override
        public Object visitVariableExpression(VariableExpressionContext ctx)
        {
            return new VariableExpression(getQualifiedName(ctx.variable().qname()));
        }

        @Override
        public Object visitFunctionCallExpression(FunctionCallExpressionContext ctx)
        {
            FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
            return new QualifiedFunctionCallExpression(functionCallInfo.catalog, functionCallInfo.function, functionCallInfo.arguments, functionCallInfo.functionId, ctx.functionCall().start);
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
            // Store left dereference
            Expression prevLeftDereference = leftDereference;
            leftDereference = null;
            String catalog = lowerCase(ctx.functionName().catalog != null ? ctx.functionName().catalog.getText() : null);
            String function = getIdentifier(ctx.functionName().function);

            List<Expression> arguments = ctx.arguments.stream().map(a -> getExpression(a)).collect(toList());
            int id = functionIdByName.computeIfAbsent(Pair.of(catalog, function), key -> functionId++);
            /* Wrap argument in a dereference expression if we are inside a dereference
            * Ie.
            * Left dereference:
            *  func(field)
            *
            * Right:
            *   field.func2()
            *
            * Rewrite to:
            *
            *  func2(func(field).field)
            */
            if (prevLeftDereference != null)
            {
                arguments.add(0, prevLeftDereference);
            }

            return new FunctionCallInfo(id, catalog, function, arguments);
        }

        @Override
        public Object visitFunctionArgument(FunctionArgumentContext ctx)
        {
            Expression expression = getExpression(ctx.expression);
            String name = getIdentifier(ctx.name);
            return name != null ? new NamedExpression(name, expression) : expression;
        }

        @Override
        public Object visitComparisonExpression(ComparisonExpressionContext ctx)
        {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            ComparisonExpression.Type type = null;

            switch (ctx.op.getType())
            {
                case PayloadBuilderQueryLexer.EQUALS:
                    type = Type.EQUAL;
                    break;
                case PayloadBuilderQueryLexer.NOTEQUALS:
                    type = Type.NOT_EQUAL;
                    break;
                case PayloadBuilderQueryLexer.LESSTHAN:
                    type = Type.LESS_THAN;
                    break;
                case PayloadBuilderQueryLexer.LESSTHANEQUAL:
                    type = Type.LESS_THAN_EQUAL;
                    break;
                case PayloadBuilderQueryLexer.GREATERTHAN:
                    type = Type.GREATER_THAN;
                    break;
                case PayloadBuilderQueryLexer.GREATERTHANEQUAL:
                    type = Type.GREATER_THAN_EQUAL;
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
            LogicalBinaryExpression.Type type = ctx.AND() != null ? LogicalBinaryExpression.Type.AND : LogicalBinaryExpression.Type.OR;

            return new LogicalBinaryExpression(type, left, right);
        }

        @Override
        public Object visitLogicalNot(LogicalNotContext ctx)
        {
            return new LogicalNotExpression(getExpression(ctx.expression()));
        }

        @Override
        public Object visitInExpression(InExpressionContext ctx)
        {
            return new InExpression(getExpression(ctx.expression(0)), ctx.expression().stream().skip(1).map(e -> getExpression(e)).collect(toList()), ctx.NOT() != null);
        }

        @Override
        public Object visitNestedExpression(NestedExpressionContext ctx)
        {
            return new NestedExpression(getExpression(ctx.expression()));
        }

        @Override
        public Object visitNullPredicate(NullPredicateContext ctx)
        {
            return new NullPredicateExpression(getExpression(ctx.expression()), ctx.NOT() != null);
        }

        @Override
        public Object visitDereference(DereferenceContext ctx)
        {
            //            Expression prevLeftDereference = leftDereference;
            Expression left = getExpression(ctx.left);
            leftDereference = left;

            Expression result;
            // Dereferenced field
            if (ctx.identifier() != null)
            {
                List<String> parts = new ArrayList<>();
                parts.add(getIdentifier(ctx.identifier()));
                if (left instanceof QualifiedReferenceExpression)
                {
                    QualifiedReferenceExpression qfe = ((QualifiedReferenceExpression) left);
                    parts.addAll(0, qfe.getQname().getParts());
                    result = new QualifiedReferenceExpression(new QualifiedName(parts), qfe.getLambdaId());
                }
                else
                {
                    result = new DereferenceExpression(left, new QualifiedReferenceExpression(new QualifiedName(parts), -1));
                }
            }
            // Dereferenced function call
            else
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                result = new QualifiedFunctionCallExpression(functionCallInfo.catalog, functionCallInfo.function, functionCallInfo.arguments, functionCallInfo.functionId, ctx.functionCall().start);
            }

            leftDereference = null;//prevLeftDereference;
            return result;
        }

        @Override
        public Object visitLambdaExpression(LambdaExpressionContext ctx)
        {
            List<String> identifiers = ctx.identifier().stream().map(i -> getIdentifierString(i.getText())).collect(toList());
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

            switch (ctx.op.getType())
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
                return ctx.booleanLiteral().TRUE() != null
                    ? LiteralBooleanExpression.TRUE_LITERAL
                    : LiteralBooleanExpression.FALSE_LITERAL;
            }
            else if (ctx.numericLiteral() != null)
            {
                return createLiteralNumericExpression(ctx.numericLiteral().getText());
            }
            else if (ctx.decimalLiteral() != null)
            {
                return createLiteralDecimalExpression(ctx.decimalLiteral().getText());
            }

            String text = ctx.stringLiteral().getText();
            text = text.substring(1, text.length() - 1);
            // Remove escaped single quotes
            text = text.replaceAll("''", "'");
            return new LiteralStringExpression(text);
        }

        private SortItem getSortItem(SortItemContext ctx)
        {
            Expression expression = getExpression(ctx.expression());
            SortItem.Order order = Order.ASC;
            SortItem.NullOrder nullOrder = NullOrder.UNDEFINED;

            int orderType = ctx.order != null ? ctx.order.getType() : -1;
            if (orderType == PayloadBuilderQueryLexer.DESC)
            {
                order = Order.DESC;
            }

            int nullOrderType = ctx.nullOrder != null ? ctx.nullOrder.getType() : -1;
            if (nullOrderType == PayloadBuilderQueryLexer.FIRST)
            {
                nullOrder = NullOrder.FIRST;
            }
            else if (nullOrderType == PayloadBuilderQueryLexer.LAST)
            {
                nullOrder = NullOrder.LAST;
            }

            return new SortItem(expression, order, nullOrder);
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
            if (string == null)
            {
                return null;
            }

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
            List<String> parts = ctx.identifier()
                    .stream()
                    .map(i -> getIdentifierString(i.getText()))
                    .collect(toList());

            return new QualifiedName(parts);
        }

        private static class FunctionCallInfo
        {
            int functionId;
            String catalog;
            String function;
            List<Expression> arguments;

            FunctionCallInfo(int functionId, String catalog, String function, List<Expression> arguments)
            {
                this.functionId = functionId;
                this.catalog = catalog;
                this.function = function;
                this.arguments = arguments;
            }
        }
    }
}
