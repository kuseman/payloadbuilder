package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.FunctionInfo;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.catalog._default.DefaultCatalog;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ArithmeticBinaryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ArithmeticUnaryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ColumnReferenceContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.ComparisonExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.DereferenceContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.FunctionCallContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.FunctionCallExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.InExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.JoinPartContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LambdaExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LiteralContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LogicalBinaryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.LogicalNotContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.NestedExpressionContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.NullPredicateContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.PopulatingJoinPartContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.QnameContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.QueryContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.SelectItemContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.SortItemContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TableSourceContext;
import com.viskan.payloadbuilder.parser.PayloadBuilderQueryParser.TableSourceJoinedContext;
import com.viskan.payloadbuilder.parser.tree.Apply;
import com.viskan.payloadbuilder.parser.tree.Apply.ApplyType;
import com.viskan.payloadbuilder.parser.tree.ArithmeticBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.ArithmeticUnaryExpression;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression.Type;
import com.viskan.payloadbuilder.parser.tree.DereferenceExpression;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.ExpressionSelectItem;
import com.viskan.payloadbuilder.parser.tree.FunctionCall;
import com.viskan.payloadbuilder.parser.tree.InExpression;
import com.viskan.payloadbuilder.parser.tree.Join;
import com.viskan.payloadbuilder.parser.tree.Join.JoinType;
import com.viskan.payloadbuilder.parser.tree.JoinItem;
import com.viskan.payloadbuilder.parser.tree.JoinedTableSource;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralBooleanExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralDecimalExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralNullExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralNumericExpression;
import com.viskan.payloadbuilder.parser.tree.LiteralStringExpression;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.LogicalNotExpression;
import com.viskan.payloadbuilder.parser.tree.NestedExpression;
import com.viskan.payloadbuilder.parser.tree.NestedSelectItem;
import com.viskan.payloadbuilder.parser.tree.NullPredicateExpression;
import com.viskan.payloadbuilder.parser.tree.PopulatingJoin;
import com.viskan.payloadbuilder.parser.tree.QualifiedFunctionCallExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceSelectItem;
import com.viskan.payloadbuilder.parser.tree.Query;
import com.viskan.payloadbuilder.parser.tree.SelectItem;
import com.viskan.payloadbuilder.parser.tree.SortItem;
import com.viskan.payloadbuilder.parser.tree.SortItem.NullOrder;
import com.viskan.payloadbuilder.parser.tree.SortItem.Order;
import com.viskan.payloadbuilder.parser.tree.Table;
import com.viskan.payloadbuilder.parser.tree.TableFunction;
import com.viskan.payloadbuilder.parser.tree.TableSource;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class QueryParser
{
    /** Parser query */
    public Query parseQuery(CatalogRegistry catalogRegistry, String query)
    {
        return getTree(catalogRegistry, query, p -> p.query());
    }

    /** Parse expression */
    public Expression parseExpression(CatalogRegistry catalogRegistry, String expression)
    {
        return getTree(catalogRegistry, expression, p -> p.expression());
    }

    @SuppressWarnings("unchecked")
    private <T> T getTree(CatalogRegistry catalogRegistry, String body, Function<PayloadBuilderQueryParser, ParserRuleContext> function)
    {
        BaseErrorListener errorListener = new BaseErrorListener()
        {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
            {
                throw new RuntimeException("Error parsing query " + body + ". Position: " + charPositionInLine + " " + msg);
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

        return (T) new AstBuilder(catalogRegistry).visit(tree);
    }

    /** Builds tree */
    private static class AstBuilder extends PayloadBuilderQueryBaseVisitor<Object>
    {
        private final CatalogRegistry catalogRegistry;
        /** Lambda parameters and slot id in current scope */
        private final Map<String, Integer> lambdaParameters = new HashMap<>();
        private Expression leftDereference;
        private Expression prevLeftDereference;

        public AstBuilder(CatalogRegistry catalogRegistry)
        {
            this.catalogRegistry = catalogRegistry;
        }

        @Override
        public Object visitQuery(QueryContext ctx)
        {
            List<SelectItem> selectItems = ctx.selectItem().stream().map(s -> (SelectItem) visit(s)).collect(toList());
            JoinedTableSource joinedTableSource = ctx.tableSourceJoined() != null ? (JoinedTableSource) visit(ctx.tableSourceJoined()) : null;
            Expression where = getExpression(ctx.where);
            List<Expression> groupBy = ctx.groupBy != null ? ctx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
            List<SortItem> orderBy = ctx.sortItem() != null ? ctx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();
            return new Query(selectItems, joinedTableSource, where, groupBy, orderBy);
        }
        
        

        @Override
        public Object visitSelectItem(SelectItemContext ctx)
        {
            String identifier = getIdentifier(ctx.identifier());
            Expression expression = getExpression(ctx.expression());
            if (expression != null)
            {
                if (expression instanceof QualifiedReferenceExpression)
                {
                    return new QualifiedReferenceSelectItem(((QualifiedReferenceExpression) expression).getQname(), identifier);
                }
                return new ExpressionSelectItem((Expression) visit(ctx.expression()), identifier);
            }
            else if (ctx.nestedSelectItem() != null)
            {
                NestedSelectItem.Type type = ctx.OBJECT() != null ? NestedSelectItem.Type.OBJECT : NestedSelectItem.Type.ARRAY;
                List<SelectItem> selectItems = ctx.nestedSelectItem().selectItem().stream().map(s -> (SelectItem) visit(s)).collect(toList());
                QualifiedName from = getQualifiedName(ctx.nestedSelectItem().qname());
                Expression where = getExpression(ctx.nestedSelectItem().where);

                return new NestedSelectItem(type, selectItems, from, where, identifier);
            }

            throw new IllegalStateException("Caould no create a select item.");
        }

        @Override
        public Object visitTableSourceJoined(TableSourceJoinedContext ctx)
        {
            TableSource tableSource = (TableSource) visit(ctx.tableSource());
            List<JoinItem> joins = ctx.joinItem().stream().map(j -> (JoinItem) visit(j)).collect(toList());
            
            return new JoinedTableSource(tableSource, joins);
        }

        @Override
        public Object visitPopulatingJoinPart(PopulatingJoinPartContext ctx)
        {
            List<JoinItem> joins = ctx.joinItem().stream().map(j -> (JoinItem) visit(j)).collect(toList());
            List<Expression> groupBy = ctx.groupBy != null ? ctx.groupBy.stream().map(si -> getExpression(si)).collect(toList()) : emptyList();
            List<SortItem> orderBy = ctx.sortItem() != null ? ctx.sortItem().stream().map(si -> getSortItem(si)).collect(toList()) : emptyList();
            Expression having = ctx.having != null ? getExpression(ctx.having) : null;
            return new PopulatingJoin(orderBy, groupBy, joins, having);
        }
        
        @Override
        public Object visitJoinPart(JoinPartContext ctx)
        {
            JoinedTableSource joinedTableSource = (JoinedTableSource) visit(ctx.tableSourceJoined());
            
            if (ctx.JOIN() != null)
            {
                Expression condition = ctx.expression() != null ? (Expression) visit(ctx.expression()) : null;
                Join.JoinType joinType = ctx.INNER() != null ? JoinType.INNER : JoinType.LEFT;
                return new Join(joinedTableSource, joinType, condition);
            }
            
            ApplyType applyType = ctx.OUTER() != null ? ApplyType.OUTER : ApplyType.CROSS;
            return new Apply(joinedTableSource, applyType);
        }
        
        @Override
        public Object visitTableSource(TableSourceContext ctx)
        {
            String alias = getIdentifier(ctx.identifier());
            if (ctx.functionCall() != null)
            {
                FunctionCall functionCall = (FunctionCall) visit(ctx.functionCall());
                if (functionCall.getFunctionInfo().getType() != FunctionInfo.Type.TABLE)
                {
                    throw new IllegalArgumentException("Expected a table function but got: " + functionCall.getFunctionInfo());
                }
                return new TableFunction((TableFunctionInfo) functionCall.getFunctionInfo(), functionCall.getArguments(), alias);
                
            }
            
            return new Table(getQualifiedName(ctx.qname()), alias);
        }
        
        @Override
        public Object visitColumnReference(ColumnReferenceContext ctx)
        {
            QualifiedName qname = getQualifiedName(ctx.qname());
            Integer lambdaId = lambdaParameters.get(qname.getFirst());
            return new QualifiedReferenceExpression(qname, lambdaId != null ? lambdaId.intValue() : -1);
        }

        @Override
        public Object visitFunctionCallExpression(FunctionCallExpressionContext ctx)
        {
            FunctionCall functionCall = (FunctionCall) visit(ctx.functionCall());
            if (functionCall.getFunctionInfo().getType() != FunctionInfo.Type.SCALAR)
            {
                throw new IllegalArgumentException("Expected a scalar function but got: " + functionCall.getFunctionInfo());
            }
            return new QualifiedFunctionCallExpression((ScalarFunctionInfo) functionCall.getFunctionInfo(), functionCall.getArguments());
        }

        @Override
        public Object visitFunctionCall(FunctionCallContext ctx)
        {
          /*
           * Function call.
           * Depending on the size of the parts in the qualified name
           * a correct lookup needs to be made.
           * Ie.
           *
           * field.func()
           *   Can either be a column reference with a dereferenced function
           *   or a catalog function with catalog name "func".
           *
           * field.field.CATALOG.func()
           *   Dereference with a catalog function. Because dereferenced functions is
           *   noting other that syntactic sugar this is equivalent with
           *   CATALOG.func(field.field) and hence the part before the catalog
           *   will be extracted and used as argument to function
           *
           * If we are inside a dereference (dot)
           *   Left: field1.func()
           *   This: field.func()
           *
           *   Should be transformed into:
           *   func(field1.func().field);
           *
           *   Or if there is a catalog match like:
           *   Left: field1.func()
           *   This: UTILS.func()
           *
           *   Should be transformed into:
           *   UTILS.func(field1.func());
           *
           * partN..partX.func()
           *
           * Last part before function name is either a catalog reference
           * or column referemce
           * qname.getParts().get(size - 2)
           *
           */
            
            QualifiedName qname = getQualifiedName(ctx.qname());
            int size = qname.getParts().size();

            /*
             * func()                       <- default
             * Utils.func()                 <- catalog
             * field.func()                 <- dereference
             * field.field.Utils.func()     <- dereference with catalog
             */

            String functionName = qname.getLast();
            String potentialCatalog = size == 1 ? DefaultCatalog.NAME : qname.getParts().get(size - 2);

            Catalog catalog = catalogRegistry.getCatalog(potentialCatalog);
            boolean catalogHit = catalog != null;
            if (catalog == null)
            {
                // Assume default catalog if none potential found
                catalog = catalogRegistry.getDefault();
            }

            FunctionInfo functionInfo = catalog.getFunction(functionName);
            if (functionInfo == null)
            {
                throw new IllegalArgumentException("Could not find a function named: " + (qname.getLast() + " in catalog: " + catalog.getName()));
            }

            int extractTo = size - 1 - (catalogHit ? 1 : 0);
            List<Expression> arguments = ctx.expression().stream().map(a -> getExpression(a)).collect(toList());

            Expression arg = null;
            // Extract qualified name without function name (last part)
            // And if there was a catalog hit, remove that also
            if (extractTo > 0)
            {
                QualifiedName leftPart = qname.extract(0, extractTo);

                Integer lambdaId = lambdaParameters.get(leftPart.getFirst());
                arg = new QualifiedReferenceExpression(leftPart, lambdaId != null ? lambdaId.intValue() : -1);
            }

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
            if (leftDereference != null)
            {
                arg = arg != null ? new DereferenceExpression(leftDereference, (QualifiedReferenceExpression) arg) : leftDereference;
            }
            if (arg != null)
            {
                arguments.add(0, arg);
            }

            if (functionInfo.getInputTypes() != null)
            {
                List<Class<? extends Expression>> inputTypes = functionInfo.getInputTypes();
                size = inputTypes.size();
                if (arguments.size() != size)
                {
                    throw new IllegalArgumentException("Function " + functionInfo.getName() + " expected " + inputTypes.size() + " parameters, found " + arguments.size());
                }
                for (int i = 0; i < size; i++)
                {
                    Class<? extends Expression> inputType = inputTypes.get(i);
                    if (!inputType.isAssignableFrom(arguments.get(i).getClass()))
                    {
                        throw new IllegalArgumentException(
                                "Function " + functionInfo.getName() + " expects " + inputType.getSimpleName() + " as parameter at index " + i + " but got "
                                    + arguments.get(i).getClass().getSimpleName());
                    }
                }
            }

            return new FunctionCall(functionInfo, arguments);
        }

        @Override
        public Object visitComparisonExpression(ComparisonExpressionContext ctx)
        {
            Expression left = (Expression) visit(ctx.left);
            Expression right = (Expression) visit(ctx.right);
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
            Expression left = (Expression) visit(ctx.left);
            Expression right = (Expression) visit(ctx.right);
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
            return new InExpression(getExpression(ctx.expression(0)), ctx.expression().stream().skip(1).map(e -> getExpression(e)).collect(toList()));
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
            prevLeftDereference = leftDereference;
            Expression left = getExpression(ctx.left);
            leftDereference = left;

            if (!(left instanceof QualifiedReferenceExpression || left instanceof QualifiedFunctionCallExpression || left instanceof DereferenceExpression))
            {
                throw new IllegalArgumentException("Can only dereference qualified references or functions.");
            }

            Expression result;
            // Dereferenced column
            if (ctx.qname() != null)
            {
                result = new DereferenceExpression(left, new QualifiedReferenceExpression(getQualifiedName(ctx.qname()), -1));
            }
            // Dereferenced function call
            else
            {
                result = (Expression) visit(ctx.functionCall());
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
                    throw new IllegalArgumentException("Lambda identifier " + i + " is already in scope.");
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
            Expression expression = (Expression) visit(ctx.expression());
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
            Expression left = (Expression) visit(ctx.left);
            Expression right = (Expression) visit(ctx.right);
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
                return new LiteralNumericExpression(ctx.numericLiteral().getText());
            }
            else if (ctx.decimalLiteral() != null)
            {
                return new LiteralDecimalExpression(ctx.decimalLiteral().getText());
            }

            String text = ctx.stringLiteral().getText();
            text = text.substring(1, text.length() - 1);
            return new LiteralStringExpression(text);
        }

        //        private FunctionInfo getFunction(FunctionCallContext ctx, boolean scalar)
        //        {
        //            QualifiedName qname = getQualifiedName(ctx.qname());
        //            int size = qname.getParts().size();
        //
        //            /*
        //             * func()                       <- default
        //             * Utils.func()                 <- catalog
        //             * field.func()                 <- dereference
        //             * field.field.Utils.func()     <- dereference with catalog
        //             *
        //             * If there is a leftDereference
        //             *
        //             * Left: field.func()
        //             *
        //             * func()                       <- default
        //             * Utils.func()                 <- catalog
        //             * field.func()                 <- dereference
        //             * field.field.Utils.func()     <- dereference with catalog
        //             *
        //             */
        //
        //            String functionName = qname.getLast();
        //            String potentialCatalog = size == 1 ? DefaultCatalog.NAME : qname.getParts().get(size - 2);
        //
        //            Catalog catalog = catalogRegistry.getCatalog(potentialCatalog);
        //            boolean catalogHit = catalog != null;
        //            if (catalog == null)
        //            {
        //                // Assume default catalog if none potential found
        //                catalog = catalogRegistry.getDefault();
        //            }
        //
        //            FunctionInfo functionInfo = scalar ? catalog.getScalarFunction(functionName) : catalog.getTableFunction(functionName);
        //            if (functionInfo == null)
        //            {
        //                throw new IllegalArgumentException("Could not find a function named: " + (qname.getLast() + " in catalog: " + catalog.getName()));
        //            }
        //            return functionInfo;
        //        }

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
            }
            // Remove quoted quotes
            return text.replaceAll("\"\"", "\"");
        }

        private Expression getExpression(ParserRuleContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }

            return (Expression) visit(ctx);
        }

        private QualifiedName getQualifiedName(QnameContext ctx)
        {
            if (ctx == null)
            {
                return null;
            }
            return new QualifiedName(ctx.identifier().stream().map(i -> getIdentifierString(i.getText())).collect(toList()));
        }
    }
}
