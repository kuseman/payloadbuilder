package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.parser.Apply.ApplyType;
import com.viskan.payloadbuilder.parser.ComparisonExpression.Type;
import com.viskan.payloadbuilder.parser.Join.JoinType;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ArithmeticBinaryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ArithmeticUnaryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ColumnReferenceContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ComparisonExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.DereferenceContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.DescribeStatementContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.FunctionCallContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.FunctionCallExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.IfStatementContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.InExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.JoinPartContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LambdaExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LiteralContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LogicalBinaryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LogicalNotContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.NamedParameterExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.NestedExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.NullPredicateContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.PopulateQueryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.PrintStatementContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.QnameContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.QueryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.SelectItemContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.SelectStatementContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.SetStatementContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.SortItemContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TableSourceContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TableSourceJoinedContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TableSourceOptionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TopExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TopSelectContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.VariableExpressionContext;
import com.viskan.payloadbuilder.parser.SortItem.NullOrder;
import com.viskan.payloadbuilder.parser.SortItem.Order;

import static com.viskan.payloadbuilder.parser.LiteralExpression.createLiteralDecimalExpression;
import static com.viskan.payloadbuilder.parser.LiteralExpression.createLiteralNumericExpression;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

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
        /** Function identifier counter. Ticks up for each unique catalog/function name combo.
         * This to be able to cache function lookup by id */
        private int functionId;
        private final Map<Pair<String, String>, Integer> functionIdByName = new HashMap<>();
        
        /** Lambda parameters and slot id in current scope */
        private final Map<String, Integer> lambdaParameters = new HashMap<>();
        private Expression leftDereference;

        @Override
        public Object visitIfStatement(IfStatementContext ctx)
        {
            Expression condition = getExpression(ctx.condition);
            List<Statement> statements = ctx.statements().stream().map(s -> (Statement) visit(s)).collect(toList());
            List<Statement> elseStatements = ctx.elseStatements != null ? ctx.elseStatements.stream().map(s -> (Statement) visit(s)).collect(toList()) : emptyList();
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
            String scope = ctx.scope != null ? ctx.scope.getText() : null;
            return new SetStatement(scope, getQualifiedName(ctx.qname()), getExpression(ctx.expression()));
        }

        @Override
        public Object visitDescribeStatement(DescribeStatementContext ctx)
        {
            if (ctx.tableName() != null)
            {
                String catalog = lowerCase(ctx.tableName().catalog != null ? ctx.tableName().catalog.getText() : null);
                return new DescribeTableStatement(catalog, getQualifiedName(ctx.tableName().qname()));
            }
            else if (ctx.functionName() != null)
            {
                String catalog = lowerCase(ctx.functionName().catalog != null ? ctx.functionName().catalog.getText() : null);
                return new DescribeFunctionStatement(catalog, ctx.functionName().function.getText());
            }
            
            return new DescribeSelectStatement((SelectStatement) visit(ctx.selectStatement()));
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

            Optional<SelectItem> item = selectItems.stream().filter(si -> isBlank(si.getIdentifier())).findAny();

            if (item.isPresent())
            {
                int index = selectItems.indexOf(item.get());
                SelectItemContext itemCtx = ctx.selectItem(index);
                throw new ParseException("Select items on ROOT level must have aliaes. Item: " + item.get(), itemCtx.start);
            }

            TableSourceJoined joinedTableSource = ctx.tableSourceJoined() != null ? (TableSourceJoined) visit(ctx.tableSourceJoined()) : null;
            if (joinedTableSource != null && joinedTableSource.getTableSource() instanceof PopulateTableSource)
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
            }

            Expression where = getExpression(ctx.where);
            List<Expression> groupBy = ctx.groupBy != null ? ctx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
            List<SortItem> orderBy = ctx.sortItem() != null ? ctx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();
            Select select = new Select(selectItems, joinedTableSource, where, groupBy, orderBy);
            return new SelectStatement(select);
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
            Expression expression = getExpression(ctx.expression());
            if (expression != null)
            {
                return new ExpressionSelectItem(expression, identifier);
            }
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

            throw new ParseException("Caould no create a select item.", ctx.start);
        }

        @Override
        public Object visitTableSourceJoined(TableSourceJoinedContext ctx)
        {
            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            List<AJoin> joins = ctx.joinPart().stream().map(j -> (AJoin) visit(j)).collect(toList());
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

        @Override
        public Object visitTableSource(TableSourceContext ctx)
        {
            String alias = getIdentifier(ctx.identifier());
            List<TableOption> tableOptions = ctx.tableSourceOptions() != null ? ctx.tableSourceOptions().options.stream().map(to -> (TableOption) visit(to)).collect(toList()) : emptyList();
            if (ctx.functionCall() != null)
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                return new TableFunction(functionCallInfo.catalog,  functionCallInfo.function, functionCallInfo.arguments, defaultIfNull(alias, ""), tableOptions, functionCallInfo.functionId, ctx.functionCall().start);
            }
            else if (ctx.populateQuery() != null)
            {
                if (isBlank(alias))
                {
                    throw new ParseException("Populate query must have an alias", ctx.populateQuery().start);
                }

                PopulateQueryContext populateQueryCtx = ctx.populateQuery();
                TableSourceJoined tableSourceJoined = (TableSourceJoined) visit(populateQueryCtx.tableSourceJoined());

                if (tableSourceJoined.getTableSource() instanceof PopulateTableSource)
                {
                    throw new ParseException("Table source in populate query cannot be a populate table source", populateQueryCtx.start);
                }

                Expression where = populateQueryCtx.where != null ? getExpression(populateQueryCtx.where) : null;
                List<Expression> groupBy = populateQueryCtx.groupBy != null ? populateQueryCtx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
                List<SortItem> orderBy = populateQueryCtx.sortItem() != null ? populateQueryCtx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();

                return new PopulateTableSource(alias, tableSourceJoined, where, groupBy, orderBy, ctx.populateQuery().start);
            }

            String catalog = ctx.tableName().catalog != null ? ctx.tableName().catalog.getText() : null;
            QualifiedName table = getQualifiedName(ctx.tableName().qname());
            return new Table(catalog, table, defaultIfNull(alias, ""), tableOptions, ctx.tableName().start);
        }

        @Override
        public Object visitTableSourceOption(TableSourceOptionContext ctx)
        {
            QualifiedName option = getQualifiedName(ctx.qname());
            Expression valueExpression = getExpression(ctx.expression());
            return new TableOption(option, valueExpression);
        }
        
        @Override
        public Object visitColumnReference(ColumnReferenceContext ctx)
        {
            String identifier = getIdentifier(ctx.identifier());
            Integer lambdaId = lambdaParameters.get(identifier);
            return new QualifiedReferenceExpression(QualifiedName.of(identifier), lambdaId != null ? lambdaId.intValue() : -1);
        }

        @Override
        public Object visitNamedParameterExpression(NamedParameterExpressionContext ctx)
        {
            return new NamedParameterExpression(getIdentifier(ctx.namedParameter().identifier()));
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
        public Object visitFunctionCall(FunctionCallContext ctx)
        {
            Expression prevLeftDereference = leftDereference;
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
            //            /*
            //             * Function call.
            //             * Depending on the size of the parts in the qualified name
            //             * a correct lookup needs to be made.
            //             * Ie.
            //             *
            //             * field.func()
            //             *   Can either be a column reference with a dereferenced function
            //             *   or a catalog function with catalog name "func".
            //             *
            //             * field.field.CATALOG.func()
            //             *   Dereference with a catalog function. Because dereferenced functions is
            //             *   noting other that syntactic sugar this is equivalent to
            //             *   CATALOG.func(field.field) and hence the part before the catalog
            //             *   will be extracted and used as argument to function
            //             *
            //             * If we are inside a dereference (dot)
            //             *   Left: field1.func()
            //             *   This: field.func()
            //             *
            //             *   Should be transformed into:
            //             *   func(field1.func().field);
            //             *
            //             *   Or if there is a catalog match like:
            //             *   Left: field1.func()
            //             *   This: UTILS.func()
            //             *
            //             *   Should be transformed into:
            //             *   UTILS.func(field1.func());
            //             *
            //             * partN..partX.func()
            //             *
            //             * Last part before function name is either a catalog reference
            //             * or column referemce
            //             * qname.getParts().get(size - 2)
            //             *
            //             */
            //
            //            QualifiedName qname = getQualifiedName(ctx.qname());
            //            int size = qname.getParts().size();
            //
            //            /*
            //             * func()                       <- default
            //             * Utils.func()                 <- catalog
            //             * field.func()                 <- dereference
            //             * field.field.Utils.func()     <- dereference with catalog
            //             */
            //
            //            String functionName = qname.getLast();
            //            String potentialCatalog = size == 1 ? BuiltinCatalog.NAME : qname.getParts().get(size - 2);
            //
            //            Catalog catalog = catalogRegistry.getCatalog(potentialCatalog);
            //            boolean catalogHit = catalog != null;
            //            if (catalog == null)
            //            {
            //                // Assume built in catalog if none potential found
            //                catalog = catalogRegistry.getBuiltin();
            //            }
            //
            //            FunctionInfo functionInfo = catalog.getFunction(functionName);
            //            if (functionInfo == null)
            //            {
            //                throw new ParseException("Could not find a function named: " + (qname.getLast() + " in catalog: " + catalog.getName()), ctx.start);
            //            }
            //
            //            int extractTo = size - 1 - (catalogHit ? 1 : 0);
            //            List<Expression> arguments = ctx.expression().stream().map(a -> getExpression(a)).collect(toList());
            //
            //            Expression arg = null;
            //            // Extract qualified name without function name (last part)
            //            // And if there was a catalog hit, remove that also
            //            if (extractTo > 0)
            //            {
            //                QualifiedName leftPart = qname.extract(0, extractTo);
            //
            //                Integer lambdaId = lambdaParameters.get(leftPart.getFirst());
            //                arg = new QualifiedReferenceExpression(leftPart, lambdaId != null ? lambdaId.intValue() : -1);
            //            }
            //
            //            /* Wrap argument in a dereference expression if we are inside a dereference
            //             * Ie.
            //             * Left dereference:
            //             *  func(field)
            //             *
            //             * Right:
            //             *   field.func2()
            //             *
            //             * Rewrite to:
            //             *
            //             *  func2(func(field).field)
            //             */
            //            if (leftDereference != null)
            //            {
            //                arg = arg != null ? new DereferenceExpression(leftDereference, (QualifiedReferenceExpression) arg) : leftDereference;
            //            }
            //            if (arg != null)
            //            {
            //                arguments.add(0, arg);
            //            }
            //
            //            if (functionInfo.getInputTypes() != null)
            //            {
            //                List<Class<? extends Expression>> inputTypes = functionInfo.getInputTypes();
            //                size = inputTypes.size();
            //                if (arguments.size() != size)
            //                {
            //                    throw new ParseException("Function " + functionInfo.getName() + " expected " + inputTypes.size() + " parameters, found " + arguments.size(), ctx.start);
            //                }
            //                for (int i = 0; i < size; i++)
            //                {
            //                    Class<? extends Expression> inputType = inputTypes.get(i);
            //                    if (!inputType.isAssignableFrom(arguments.get(i).getClass()))
            //                    {
            //                        throw new ParseException(
            //                                "Function " + functionInfo.getName() + " expects " + inputType.getSimpleName() + " as parameter at index " + i + " but got "
            //                                    + arguments.get(i).getClass().getSimpleName(), ctx.start);
            //                    }
            //                }
            //            }
            //
            //            return new FunctionCall(functionInfo, arguments);
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
            Expression prevLeftDereference = leftDereference;
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

            leftDereference = prevLeftDereference;
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
