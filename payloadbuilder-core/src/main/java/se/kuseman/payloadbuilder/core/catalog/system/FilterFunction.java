package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils.LambdaResultConsumer;
import se.kuseman.payloadbuilder.core.execution.vector.TupleVectorBuilder;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Filter input argument with a lambda */
class FilterFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    FilterFunction()
    {
        super("filter", FunctionType.SCALAR);
    }

    @Override
    public List<LambdaBinding> getLambdaBindings()
    {
        return LAMBDA_BINDINGS;
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        // Result type of filter is the same as input ie. argument 0
        return arguments.get(0)
                .getType();
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        final Type type = value.type()
                .getType();

        // Filter each individual vector
        if (LambdaUtils.supportsForEachLambdaResult(type))
        {
            IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                    .getObjectVectorBuilder(value.type(), input.getRowCount());

            LambdaResultConsumer consumer = (inputResult, lambdaResult, inputWasListType) ->
            {
                if (lambdaResult == null)
                {
                    builder.put(null);
                    return;
                }
                else if (!inputWasListType)
                {
                    if (lambdaResult.getPredicateBoolean(0))
                    {
                        builder.put(inputResult);
                    }
                    return;
                }

                if (inputResult instanceof ValueVector)
                {
                    builder.put(createFilteredVector(context, (ValueVector) inputResult, lambdaResult));
                    return;
                }
                else if (inputResult instanceof TupleVector)
                {
                    TupleVector table = (TupleVector) inputResult;

                    int cardinality = lambdaResult.getCardinality();
                    if (cardinality == 0)
                    {
                        builder.put(TupleVector.of(table.getSchema(), emptyList()));
                        return;
                    }

                    TupleVectorBuilder b = new TupleVectorBuilder(((ExecutionContext) context).getBufferAllocator(), cardinality);
                    b.append(table, lambdaResult);
                    builder.put(b.build());
                }
                else
                {
                    if (lambdaResult.getPredicateBoolean(0))
                    {
                        builder.put(inputResult);
                    }
                }
            };

            LambdaUtils.forEachLambdaResult(context, input, value, le, consumer);
            return builder.build();
        }

        // Filter the whole input
        return createFilteredVector(context, input, le, value);
    }

    private ValueVector createFilteredVector(IExecutionContext context, TupleVector input, LambdaExpression lambdaExpression, ValueVector value)
    {
        // First evaluate the input value to create a filter vector
        ((ExecutionContext) context).getStatementContext()
                .setLambdaValue(lambdaExpression.getLambdaIds()[0], value);

        ValueVector filter = lambdaExpression.getExpression()
                .eval(input, context);

        return createFilteredVector(context, value, filter);
    }

    private ValueVector createFilteredVector(IExecutionContext context, ValueVector value, ValueVector filter)
    {
        int rowCount = value.size();
        int resultSize = filter.getCardinality();

        if (resultSize == 0)
        {
            return ValueVector.empty(value.type());
        }

        IValueVectorBuilder builder = context.getVectorBuilderFactory()
                .getValueVectorBuilder(value.type(), resultSize);

        for (int i = 0; i < rowCount; i++)
        {
            if (filter.getPredicateBoolean(i))
            {
                builder.put(value, i);
            }
        }
        return builder.build();
    }
}
