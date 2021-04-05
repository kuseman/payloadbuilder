package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.apache.commons.lang3.StringUtils.upperCase;
import static org.kuse.payloadbuilder.core.parser.LiteralExpression.createLiteralDecimalExpression;
import static org.kuse.payloadbuilder.core.parser.LiteralExpression.createLiteralNumericExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.Apply.ApplyType;
import org.kuse.payloadbuilder.core.parser.CaseExpression.WhenClause;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression.Type;
import org.kuse.payloadbuilder.core.parser.Join.JoinType;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.AnalyzeStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ArithmeticBinaryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ArithmeticUnaryContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.CaseExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ColumnReferenceContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.ComparisonExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.DereferenceContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.DescribeStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.DropTableStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionArgumentContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.FunctionCallExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.IfStatementContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.InExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.JoinPartContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.LambdaExpressionContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.LikeExpressionContext;
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
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.SubscriptContext;
import org.kuse.payloadbuilder.core.parser.PayloadBuilderQueryParser.TableNameContext;
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

/** Parser for a payload builder query */
public class QueryParser
{
    /** Parse query */
    public QueryStatement parseQuery(CatalogRegistry registry, String query)
    {
        return getTree(registry, query, p -> p.query(), true);
    }

    /** Parse select */
    public Select parseSelect(CatalogRegistry registry, String query)
    {
        return getTree(registry, query, p -> p.topSelect(), true);
    }

    /** Parse expression */
    public Expression parseExpression(CatalogRegistry registry, String expression)
    {
        return getTree(registry, expression, p -> p.topExpression(), false);
    }

    @SuppressWarnings("unchecked")
    private <T> T getTree(CatalogRegistry registry, String body, Function<PayloadBuilderQueryParser, ParserRuleContext> function, boolean validateQualifiedReferences)
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

        return (T) new AstBuilder(registry, validateQualifiedReferences).visit(tree);
    }

    /** Builds tree */
    private static class AstBuilder extends PayloadBuilderQueryBaseVisitor<Object>
    {
        /** Lambda parameters and slot id in current scope */
        private final Map<String, Integer> lambdaParameters = new HashMap<>();
        private Expression leftDereference;

        private TableAlias parentTableAlias;
        private final CatalogRegistry registry;
        private final boolean validateQualifiedReferences;
        private int tupleOrdinal;

        AstBuilder(CatalogRegistry registry, boolean validateQualifiedReferences)
        {
            this.registry = registry;
            this.validateQualifiedReferences = validateQualifiedReferences;
            clear(null);
        }

        private void clear(TableAlias parent)
        {
            tupleOrdinal = -1;
            TableAliasBuilder builder = TableAliasBuilder.of(this.tupleOrdinal++, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT");
            if (parent != null)
            {
                builder.parent(parent);
            }
            parentTableAlias = builder.build();
        }

        @Override
        public Object visitStatement(StatementContext ctx)
        {
            clear(null);
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
            List<Statement> statements = ctx.statements().children
                    .stream()
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
            return new SetStatement(getQualifiedName(ctx.qname()), getExpression(ctx.expression()), systemProperty);
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
        public Object visitAnalyzeStatement(AnalyzeStatementContext ctx)
        {
            return new AnalyzeStatement((SelectStatement) visit(ctx.selectStatement()));
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
        public Object visitDropTableStatement(DropTableStatementContext ctx)
        {
            String catalogAlias = ctx.tableName().catalog != null ? ctx.tableName().catalog.getText() : null;
            QualifiedName qname = getQualifiedName(ctx.tableName().qname());
            boolean lenient = ctx.EXISTS() != null;
            boolean tempTable = ctx.tableName().tempHash != null;
            return new DropTableStatement(catalogAlias, qname, lenient, tempTable, ctx.start);
        }

        //CSOFF
        @Override
        //CSON
        public Object visitSelectStatement(SelectStatementContext ctx)
        {
            TableSourceJoined from = ctx.tableSourceJoined() != null ? (TableSourceJoined) visit(ctx.tableSourceJoined()) : null;
            Expression topExpression = ctx.topCount() != null ? (Expression) visit(ctx.topCount()) : null;
            if (from == null)
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

            // Point parent to the from table source
            if (parentTableAlias.getChildAliases().size() > 0)
            {
                parentTableAlias = parentTableAlias.getChildAliases().get(0);
            }

            List<SelectItem> selectItems = ctx.selectItem().stream().map(s -> (SelectItem) visit(s)).collect(toList());

            boolean assignmentSelect = false;
            /** Verify/determine assignment select */
            if (selectItems.stream().anyMatch(s -> s.getAssignmentName() != null))
            {
                if (selectItems.stream().anyMatch(s -> s.getAssignmentName() == null))
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

                if (selectItems.stream().anyMatch(i -> isBlank(i.getIdentifier()) && i.isEmptyIdentifier()))
                {
                    throw new ParseException("All items must have an identifier when using a SELECT INTO statement", ctx.start);
                }

                TableName tableName = (TableName) visit(ctx.tableName());
                TableAlias.Type type = tableName.isTempTable() ? TableAlias.Type.TEMPORARY_TABLE : TableAlias.Type.TABLE;
                TableAlias alias = TableAlias.TableAliasBuilder.of(-1, type, getQualifiedName(ctx.into.qname()), "").build();
                List<Option> options = ctx.intoOptions != null ? ctx.intoOptions.options.stream().map(to -> (Option) visit(to)).collect(toList()) : emptyList();
                into = new Table("", alias, options, ctx.into.start);
            }

            Expression where = getExpression(ctx.where);
            List<Expression> groupBy = ctx.groupBy != null ? ctx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
            List<SortItem> orderBy = ctx.sortItem() != null ? ctx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();
            Select select = new Select(selectItems, from, into, topExpression, where, groupBy, orderBy);
            return new SelectStatement(select, assignmentSelect);
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
            String identifier = defaultIfBlank(getIdentifier(ctx.identifier()), "");
            // Expression select item
            if (ctx.expression() != null)
            {
                QualifiedName assignmentQname = ctx.variable() != null ? getQualifiedName(ctx.variable().qname()) : null;
                if (ctx.variable() != null && ctx.variable().system != null)
                {
                    throw new ParseException("Cannot assign to system variables", ctx.start);
                }
                Expression expression = getExpression(ctx.expression());
                return new ExpressionSelectItem(expression, identifier, assignmentQname, ctx.expression().start);
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
                    Optional<SelectItem> item = selectItems.stream().filter(si -> !si.isEmptyIdentifier()).findAny();

                    //CSOFF
                    if (item.isPresent())
                    //CSON
                    {
                        int index = selectItems.indexOf(item.get());
                        SelectItemContext itemCtx = ctx.nestedSelectItem().selectItem(index);
                        throw new ParseException("Select items inside an ARRAY select cannot have aliaes. Item: " + item.get(), itemCtx.start);
                    }
                }

                if (from == null)
                {
                    //CSOFF
                    if (where != null)
                    //CSON
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

                return new NestedSelectItem(type, selectItems, from, where, identifier, groupBy, orderBy, ctx.nestedSelectItem().start);
            }
            // Wildcard select item
            else if (ctx.ASTERISK() != null)
            {
                String alias = null;
                if (ctx.alias != null)
                {
                    alias = getIdentifier(ctx.alias);
                }
                boolean recursive = ctx.ASTERISK().size() > 1;
                return new AsteriskSelectItem(alias, recursive, ctx.start);
            }

            throw new ParseException("Caould no create a select item.", ctx.start);
        }

        @Override
        public Object visitTableSourceJoined(TableSourceJoinedContext ctx)
        {
            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            if (ctx.joinPart().size() > 0 && isBlank(tableSource.getTableAlias().getAlias()))
            {
                throw new ParseException("Alias is mandatory.", ctx.tableSource().start);
            }

            List<AJoin> joins = ctx
                    .joinPart()
                    .stream()
                    .map(j -> (AJoin) visit(j))
                    .collect(toList());

            return new TableSourceJoined(tableSource, joins);
        }

        @Override
        public Object visitJoinPart(JoinPartContext ctx)
        {
            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            if (isBlank(tableSource.getTableAlias().getAlias()))
            {
                throw new ParseException("Alias is mandatory", ctx.tableSource().start);
            }
            if (ctx.JOIN() != null)
            {
                Expression condition = getExpression(ctx.expression());
                Join.JoinType joinType = ctx.INNER() != null ? JoinType.INNER : JoinType.LEFT;
                return new Join(tableSource, joinType, condition);
            }

            ApplyType applyType = ctx.OUTER() != null ? ApplyType.OUTER : ApplyType.CROSS;
            return new Apply(tableSource, applyType);
        }

        @Override
        public Object visitTableSource(TableSourceContext ctx)
        {
            String alias = defaultIfBlank(getIdentifier(ctx.identifier()), "");

            List<Option> options = ctx.tableSourceOptions() != null ? ctx.tableSourceOptions().options.stream().map(to -> (Option) visit(to)).collect(toList()) : emptyList();
            if (ctx.functionCall() != null)
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                if (!(functionCallInfo.functionInfo instanceof TableFunctionInfo))
                {
                    throw new ParseException("Expected a TABLE function for " + functionCallInfo.functionInfo.toString(), ctx.start);
                }
                TableAlias tableAlias = TableAliasBuilder
                        .of(tupleOrdinal++, TableAlias.Type.FUNCTION, QualifiedName.of(functionCallInfo.functionInfo.getName()), defaultIfNull(alias, ""), ctx.functionCall().start)
                        .parent(getParentTableAlias())
                        .build();
                return new TableFunction(
                        functionCallInfo.catalogAlias,
                        tableAlias,
                        (TableFunctionInfo) functionCallInfo.functionInfo,
                        functionCallInfo.arguments,
                        options,
                        ctx.functionCall().start);
            }
            else if (ctx.selectStatement() != null)
            {
                Token token = ctx.selectStatement().start;

                if (isBlank(alias))
                {
                    throw new ParseException("Sub query must have an alias", token);
                }

                /*  When visiting a sub query we want to build a new table alias hierarchy
                    Since this is a complete query.
                    However since this is contained in another query we must start the tuple ordinal
                    at the previous one
                */
                TableAlias prevParent = parentTableAlias;

                TableAlias tableAlias = TableAliasBuilder
                        .of(tupleOrdinal++, TableAlias.Type.SUBQUERY, QualifiedName.of("SubQuery"), alias, token)
                        .parent(parentTableAlias)
                        .build();

                // Store previous parent table alias
                int prevTupleOrdinal = tupleOrdinal;
                // Start a new alias tree with subquery as parent
                clear(tableAlias);
                // Restore tuple ordinal counter since clear resets it
                tupleOrdinal = prevTupleOrdinal;

                if (ctx.selectStatement().into != null)
                {
                    throw new ParseException("SELECT INTO is not allowed in sub query context", token);
                }

                SelectStatement selectStatement = (SelectStatement) visit(ctx.selectStatement());

                if (selectStatement.isAssignmentSelect())
                {
                    throw new ParseException("Assignment selects is not allowed in sub query context", token);
                }

                parentTableAlias = prevParent;

                return new SubQueryTableSource(tableAlias, selectStatement.getSelect(), options, token);
            }

            TableName tableName = (TableName) visit(ctx.tableName());
            TableAlias.Type type = tableName.isTempTable() ? TableAlias.Type.TEMPORARY_TABLE : TableAlias.Type.TABLE;

            TableAlias tableAlias = TableAliasBuilder
                    .of(tupleOrdinal++, type, tableName.getQname(), defaultIfBlank(alias, ""), ctx.tableName().start)
                    .parent(getParentTableAlias())
                    .build();

            String catalog = ctx.tableName().catalog != null ? ctx.tableName().catalog.getText() : null;
            return new Table(catalog, tableAlias, options, ctx.tableName().start);
        }

        @Override
        public Object visitTableName(TableNameContext ctx)
        {
            String catalogAlias = ctx.catalog != null ? ctx.catalog.getText() : null;
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
            QualifiedReferenceExpression result = new QualifiedReferenceExpression(qname, lambdaId, ctx.start);
            validateQualifiedReference(ctx, result);
            return result;
        }

        @Override
        public Object visitVariableExpression(VariableExpressionContext ctx)
        {
            return new VariableExpression(getQualifiedName(ctx.variable().qname()), ctx.variable().system != null);
        }

        @Override
        public Object visitFunctionCallExpression(FunctionCallExpressionContext ctx)
        {
            FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
            if (!(functionCallInfo.functionInfo instanceof ScalarFunctionInfo))
            {
                throw new ParseException("Expected a SCALAR function for " + functionCallInfo.functionInfo.toString(), ctx.start);
            }
            return new QualifiedFunctionCallExpression((ScalarFunctionInfo) functionCallInfo.functionInfo, functionCallInfo.arguments, ctx.functionCall().start);
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

            Pair<String, FunctionInfo> functionInfo = registry.resolveFunctionInfo(catalog, function);
            if (functionInfo == null)
            {
                throw new ParseException("No function found named: " + function + " in catalog: " + (isBlank(catalog) ? "BuiltIn" : catalog), ctx.start);
            }
            validateFunction(functionInfo.getValue(), arguments, ctx.start);
            arguments = functionInfo.getValue().foldArguments(arguments);

            return new FunctionCallInfo(functionInfo.getKey(), functionInfo.getValue(), arguments);
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
        public Object visitLikeExpression(LikeExpressionContext ctx)
        {
            boolean not = ctx.NOT() != null;
            return new LikeExpression(getExpression(ctx.left), getExpression(ctx.right), not, getExpression(ctx.escape));
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
            Expression left = getExpression(ctx.left);
            leftDereference = left;

            Expression result;
            // Dereferenced field
            if (ctx.identifier() != null)
            {
                List<String> parts = new ArrayList<>();
                parts.add(getIdentifier(ctx.identifier()));
                result = new DereferenceExpression(left, new QualifiedReferenceExpression(new QualifiedName(parts), -1, ctx.start));
            }
            // Dereferenced function call
            else
            {
                FunctionCallInfo functionCallInfo = (FunctionCallInfo) visit(ctx.functionCall());
                if (!(functionCallInfo.functionInfo instanceof ScalarFunctionInfo))
                {
                    throw new ParseException("Expected a SCALAR function for " + functionCallInfo.functionInfo.toString(), ctx.start);
                }
                result = new QualifiedFunctionCallExpression((ScalarFunctionInfo) functionCallInfo.functionInfo, functionCallInfo.arguments, ctx.functionCall().start);
            }

            leftDereference = null;
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

            //CSOFF
            switch (ctx.op.getType())
            //CSOn
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

        /** Validate alias in QRE is pointing to an existing table alias */
        private void validateQualifiedReference(ParserRuleContext ctx, QualifiedReferenceExpression qfe)
        {
            if (!validateQualifiedReferences
                || isBlank(parentTableAlias.getAlias())
                || qfe.getQname().getParts().size() == 1
                || qfe.getLambdaId() >= 0)
            {
                return;
            }

            String aliasRef = qfe.getQname().getParts().get(0);

            // Pointer to current alias
            if (equalsIgnoreCase(aliasRef, parentTableAlias.getAlias()))
            {
                return;
            }

            // Pointer to child alias
            if (parentTableAlias.getChildAlias(aliasRef) != null)
            {
                return;
            }

            // Sibling reference
            if (parentTableAlias.getSiblingAlias(aliasRef) != null)
            {
                return;
            }

            // TODO: If join check we can traverse upwards
            // but only check parents children with a lower index then we came from
            /*
             * from tableA a
             * inner join
             * (
             *   select **
             *   from tableB b
             *   where c.id > 10            <--- Invalid reference since it's defined later that "this"
             * ) b
             *   on b..
             * inner join tableC c
             *   on c...
             *
             */

            // Traverse parents and try to find match
            TableAlias current = parentTableAlias.getParent();

            while (current != null)
            {
                if (equalsIgnoreCase(aliasRef, current.getAlias()))
                {
                    return;
                }
                // This might return false positives since
                // we might reference an invalid alias further down that we are not
                // this is ok in a Select item expression but not in a join condition
                else if (current.getSiblingAlias(aliasRef) != null)
                {
                    return;
                }

                current = current.getParent();
            }

            throw new ParseException("Invalid table source reference '" + aliasRef + "'", ctx.start);
        }

        private void validateFunction(FunctionInfo functionInfo, List<Expression> arguments, Token token)
        {
            if (functionInfo.getInputTypes() != null)
            {
                List<Class<? extends Expression>> inputTypes = functionInfo.getInputTypes();
                int size = inputTypes.size();
                if (arguments.size() != size)
                {
                    throw new ParseException("Function " + functionInfo.getName() + " expected " + inputTypes.size() + " parameters, found " + arguments.size(), token);
                }
                for (int i = 0; i < size; i++)
                {
                    Class<? extends Expression> inputType = inputTypes.get(i);
                    if (!inputType.isAssignableFrom(arguments.get(i).getClass()))
                    {
                        throw new ParseException(
                                "Function " + functionInfo.getName() + " expects " + inputType.getSimpleName() + " as parameter at index " + i + " but got "
                                    + arguments.get(i).getClass().getSimpleName(),
                                token);
                    }
                }
            }
            if (functionInfo.requiresNamedArguments() && (arguments.size() <= 0 || arguments.stream().anyMatch(a -> !(a instanceof NamedExpression))))
            {
                if (arguments.stream().anyMatch(a -> !(a instanceof NamedExpression)))
                {
                    throw new ParseException(
                            "Function " + functionInfo.getName() + " expects named parameters", token);
                }
            }
        }

        private TableAlias getParentTableAlias()
        {
            return parentTableAlias;
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
            String catalogAlias;
            FunctionInfo functionInfo;
            List<Expression> arguments;

            FunctionCallInfo(String catalogAlias, FunctionInfo functionInfo, List<Expression> arguments)
            {
                this.catalogAlias = catalogAlias;
                this.functionInfo = functionInfo;
                this.arguments = arguments;
            }
        }
    }
}
