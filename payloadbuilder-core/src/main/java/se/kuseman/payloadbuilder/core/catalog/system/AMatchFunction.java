package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IBooleanVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils.LambdaResultConsumer;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Any function. Check if any of inputs is true */
class AMatchFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    private final MatchType matchType;

    AMatchFunction(MatchType matchType)
    {
        super(matchType.name, FunctionType.SCALAR);
        this.matchType = matchType;
    }

    @Override
    public List<LambdaBinding> getLambdaBindings()
    {
        return LAMBDA_BINDINGS;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Column.Type.Boolean);
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        Type type = value.type()
                .getType();

        if (!LambdaUtils.supportsForEachLambdaResult(type))
        {
            return getSingleValueResult(context, input, le, value);
        }

        int rowCount = input.getRowCount();
        IBooleanVectorBuilder builder = context.getVectorBuilderFactory()
                .getBooleanVectorBuilder(rowCount);

        LambdaResultConsumer consumer = (inputResult, lambdaResult, inputWasListType) ->
        {
            if (lambdaResult == null)
            {
                builder.putNull();
                return;
            }
            else if (!inputWasListType)
            {
                if (lambdaResult.isNull(0))
                {
                    builder.putNull();
                }
                else
                {
                    boolean result = lambdaResult.getBoolean(0);
                    builder.put(((matchType == MatchType.ALL
                            || matchType == MatchType.ANY)
                            && result)
                            || (matchType == MatchType.NONE
                                    && !result));
                }
                return;
            }

            boolean result = matchType.defaultResult;
            int size = lambdaResult.size();
            if (size == 0)
            {
                builder.put(result);
                return;
            }

            boolean allNull = true;
            for (int i = 0; i < size; i++)
            {
                if (lambdaResult.isNull(i))
                {
                    continue;
                }

                allNull = false;
                boolean rowResult = lambdaResult.getBoolean(i);

                if (matchType == MatchType.ANY
                        && rowResult)
                {
                    result = true;
                    break;
                }
                else if (matchType == MatchType.NONE
                        && rowResult)
                {
                    result = false;
                    break;
                }
                else if (matchType == MatchType.ALL
                        && !rowResult)
                {
                    result = false;
                    break;
                }
            }
            if (allNull)
            {
                builder.putNull();
            }
            else
            {
                builder.put(result);
            }
        };

        LambdaUtils.forEachLambdaResult(context, input, value, le, consumer);

        return builder.build();
    }

    private ValueVector getSingleValueResult(IExecutionContext context, TupleVector input, LambdaExpression le, ValueVector value)
    {
        int rowCount = input.getRowCount();
        IBooleanVectorBuilder builder = context.getVectorBuilderFactory()
                .getBooleanVectorBuilder(rowCount);

        ((ExecutionContext) context).getStatementContext()
                .setLambdaValue(le.getLambdaIds()[0], value);

        ValueVector lambdaResult = le.getExpression()
                .eval(input, context);

        // NOTE! In single value result there is no usage of match type default result
        // since we are either null or have a value
        for (int i = 0; i < rowCount; i++)
        {
            if (lambdaResult.isNull(i))
            {
                builder.putNull();
            }
            else
            {
                boolean result = lambdaResult.getBoolean(i);
                builder.put(((matchType == MatchType.ALL
                        || matchType == MatchType.ANY)
                        && result)
                        || (matchType == MatchType.NONE
                                && !result));
            }
        }
        return builder.build();
    }

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
