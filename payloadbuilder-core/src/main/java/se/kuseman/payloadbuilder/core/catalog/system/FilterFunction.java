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
import se.kuseman.payloadbuilder.api.execution.vector.ITupleVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils.LambdaResultConsumer;
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
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(value.type(), input.getRowCount());

            LambdaResultConsumer consumer = new LambdaResultConsumer()
            {
                int index;

                @Override
                public void accept(Object inputResult, ValueVector lambdaResult, boolean inputWasListType, int row)
                {
                    if (lambdaResult == null)
                    {
                        resultVector.setNull(index);
                        index++;
                        return;
                    }
                    else if (!inputWasListType)
                    {
                        if (lambdaResult.getPredicateBoolean(0))
                        {
                            resultVector.setAny(index, inputResult);
                            index++;
                        }
                        return;
                    }

                    if (inputResult instanceof ValueVector vector)
                    {
                        resultVector.setArray(index, createFilteredVector(context, vector, lambdaResult));
                        index++;
                        return;
                    }
                    else if (inputResult instanceof TupleVector table)
                    {
                        int cardinality = lambdaResult.getCardinality();
                        if (cardinality == 0)
                        {
                            resultVector.setTable(index, TupleVector.of(table.getSchema(), emptyList()));
                            index++;
                            return;
                        }

                        ITupleVectorBuilder b = context.getVectorFactory()
                                .getTupleVectorBuilder(cardinality);
                        b.append(table, lambdaResult);
                        resultVector.setTable(index, b.build());
                        index++;
                    }
                    else
                    {
                        if (lambdaResult.getPredicateBoolean(0))
                        {
                            resultVector.setAny(index, inputResult);
                            index++;
                        }
                    }
                }
            };

            LambdaUtils.forEachLambdaResult(context, input, value, le, consumer);
            return resultVector;
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

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(value.type(), resultSize);
        int index = 0;
        for (int i = 0; i < rowCount; i++)
        {
            if (filter.getPredicateBoolean(i))
            {
                resultVector.copy(index++, value, i);
            }
        }
        return resultVector;
    }
}
