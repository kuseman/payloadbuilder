package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Any function. Check if any of inputs is true */
class AMatchFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    private final MatchType matchType;

    AMatchFunction(Catalog catalog, MatchType matchType)
    {
        super(catalog, matchType.name, FunctionType.SCALAR);
        this.matchType = matchType;
    }

    @Override
    public List<LambdaBinding> getLambdaBindings()
    {
        return LAMBDA_BINDINGS;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Boolean);
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        Type type = value.type()
                .getType();

        ValueVector lambdaResult = null;

        if (type == Type.ValueVector
                || type == Type.TupleVector)
        {
            lambdaResult = new ValueVector()
            {
                LambdaUtils.RowTupleVector inputTupleVector = new LambdaUtils.RowTupleVector(input);

                @Override
                public ResolvedType type()
                {
                    return ResolvedType.valueVector(ResolvedType.of(Type.Boolean));
                }

                @Override
                public int size()
                {
                    return value.size();
                }

                @Override
                public boolean isNullable()
                {
                    return false;
                }

                @Override
                public boolean isNull(int row)
                {
                    return false;
                }

                @Override
                public Object getValue(int row)
                {
                    ValueVector vector;
                    if (type == Type.ValueVector)
                    {
                        inputTupleVector.setRowCount(1);
                        vector = (ValueVector) value.getValue(row);
                    }
                    else
                    {
                        TupleVector tupleVector = (TupleVector) value.getValue(row);
                        inputTupleVector.setRowCount(tupleVector.getRowCount());
                        vector = ValueVector.literalObject(ResolvedType.tupleVector(tupleVector.getSchema()), tupleVector, 1);
                    }

                    ((ExecutionContext) context).getStatementContext()
                            .setLambdaValue(le.getLambdaIds()[0], vector);

                    inputTupleVector.setRow(row);
                    return le.getExpression()
                            .eval(inputTupleVector, context);
                }
            };
        }
        else
        {
            ((ExecutionContext) context).getStatementContext()
                    .setLambdaValue(le.getLambdaIds()[0], value);

            lambdaResult = le.getExpression()
                    .eval(input, context);
        }

        final ValueVector result = lambdaResult;
        Type resultType = result.type()
                .getType();

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Boolean);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public boolean getBoolean(int row)
            {
                if (resultType == Type.ValueVector)
                {
                    ValueVector vv = (ValueVector) result.getValue(row);
                    int size = vv.size();
                    for (int i = 0; i < size; i++)
                    {
                        boolean rowResult = vv.getPredicateBoolean(i);

                        if (matchType == MatchType.ALL
                                && !rowResult)
                        {
                            return false;
                        }
                        else if (matchType == MatchType.NONE
                                && rowResult)
                        {
                            return false;
                        }
                        else if (matchType == MatchType.ANY
                                && rowResult)
                        {
                            return true;
                        }
                    }
                    return matchType.defaultResult;
                }
                else
                {
                    boolean boolResult = result.getPredicateBoolean(row);

                    return ((matchType == MatchType.ALL
                            || matchType == MatchType.ANY)
                            && boolResult)
                            || (matchType == MatchType.NONE
                                    && !boolResult);
                }
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }

    //
    // @Override
    // public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    // {
    // Object argResult = arguments.get(0)
    // .eval(context);
    // if (argResult == null)
    // {
    // return null;
    // }
    // StatementContext ctx = (StatementContext) context.getStatementContext();
    // LambdaExpression le = (LambdaExpression) arguments.get(1);
    // int lambdaId = le.getLambdaIds()[0];
    //
    // Iterator<Object> it = CollectionUtils.getIterator(argResult);
    // while (it.hasNext())
    // {
    // ctx.setLambdaValue(lambdaId, it.next());
    // Object obj = le.getExpression()
    // .eval(context);
    //
    // if (!(obj instanceof Boolean))
    // {
    // throw new IllegalArgumentException("Expected boolean result but got: " + obj + " from " + le.getExpression());
    // }
    //
    // boolean result = (boolean) obj;
    // if (type == MatchType.ALL
    // && !result)
    // {
    // return false;
    // }
    // else if (type == MatchType.NONE
    // && result)
    // {
    // return false;
    // }
    // else if (type == MatchType.ANY
    // && result)
    // {
    // return true;
    // }
    // }
    //
    // return type.defaultResult;
    // }

    // @Override
    // public ExpressionCode generateCode(
    // CodeGeneratorContext context,
    // ExpressionCode parentCode,
    // List<Expression> arguments)
    // {
    // ExpressionCode inputCode = arguments.get(0).generateCode(context, parentCode);
    // ExpressionCode code = ExpressionCode.code(context, inputCode);
    // code.addImport("se.kuseman.payloadbuilder.core.utils.CollectionUtils");
    // code.addImport("java.util.Iterator");
    // code.addImport("org.apache.commons.collections.iterators.TransformIterator");
    // code.addImport("org.apache.commons.collections.Transformer");
    //
    // LambdaExpression le = (LambdaExpression) arguments.get(1);
    //
    // context.addLambdaParameters(le.getIdentifiers());
    // ExpressionCode lambdaCode = le.getExpression().generateCode(context, parentCode);
    // context.removeLambdaParameters(le.getIdentifiers());
    //
    // String template =
    // "%s"
    // + "boolean %s = true;\n"
    // + "Iterator %s = null;\n"
    // + "if (!%s)\n"
    // + "{\n"
    // + " %s = new TransformIterator(IteratorUtils.getIterator(%s), new Transformer()\n"
    // + " {\n"
    // + " public Object transform(Object object)\n"
    // + " {\n"
    // + " Object %s = object;\n"
    // + " %s"
    // + " return %s;\n"
    // + " }\n"
    // + " });\n"
    // + " %s = false;\n"
    // + "}\n";
    //
    // code.setCode(String.format(template,
    // inputCode.getCode(),
    // code.getIsNull(),
    // code.getResVar(),
    // inputCode.getIsNull(),
    // code.getResVar(), inputCode.getResVar(),
    // le.getIdentifiers().get(0),
    // lambdaCode.getCode(),
    // lambdaCode.getResVar(),
    // code.getIsNull()));
    //
    // return code;
    // }

    /** Match type */
    enum MatchType
    {
        ALL("all", true),
        ANY("any", false),
        NONE("none", true);

        private final boolean defaultResult;
        private final String name;

        MatchType(String name, boolean defaultResult)
        {
            this.name = name;
            this.defaultResult = defaultResult;
        }
    }
}
