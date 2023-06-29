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
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils.LambdaResultConsumer;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Flat map function. Flat maps input turning nested value vectors into flat vectors */
class FlatMapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    FlatMapFunction()
    {
        super("flatmap", FunctionType.SCALAR);
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
        // Flat map's type is like Map's type except if resulting lambda type is Array
        // it's flattened it it's subtype

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        ResolvedType lambdaType = le.getExpression()
                .getType();

        ResolvedType inputType = arguments.get(0)
                .getType();

        if (inputType.getType() == Type.Array
                || inputType.getType() == Type.Table)
        {
            return ResolvedType.array(lambdaType.getType() == Column.Type.Array ? lambdaType.getSubType()
                    : lambdaType);
        }
        else if (inputType.getType() == Type.Any)
        {
            return ResolvedType.of(Type.Any);
        }

        return lambdaType;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        LambdaExpression le = (LambdaExpression) arguments.get(1);

        final ResolvedType lambdaType = le.getExpression()
                .getType();

        Type inputType = value.type()
                .getType();

        if (LambdaUtils.supportsForEachLambdaResult(inputType))
        {
            ResolvedType functionType = getType(arguments);
            ResolvedType resultType = functionType.getType() == Type.Array ? functionType.getSubType()
                    : functionType;

            IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                    .getObjectVectorBuilder(functionType, input.getRowCount());

            LambdaResultConsumer mapper = (inputResult, lambdaResult, inputWasListType) ->
            {
                if (lambdaResult == null)
                {
                    builder.put(null);
                    return;
                }
                else if (!inputWasListType)
                {
                    builder.copy(lambdaResult);
                    return;
                }

                // TODO: size will be off if there are alot of inner vectors
                int size = lambdaResult.size();
                IValueVectorBuilder vectorBuilder = context.getVectorBuilderFactory()
                        .getValueVectorBuilder(resultType, size);

                // Loop vector rows and add to builder
                // If vector contains vectors then flatten one level adding it's values to builder

                for (int i = 0; i < size; i++)
                {
                    if (lambdaResult.isNull(i))
                    {
                        vectorBuilder.put(lambdaResult, i);
                        continue;
                    }

                    // Flatten the inner vector
                    if (lambdaType.getType() == Column.Type.Array)
                    {
                        vectorBuilder.copy(lambdaResult.getArray(i));
                        continue;
                    }
                    // Runtime check of value
                    else if (lambdaType.getType() == Column.Type.Any)
                    {
                        Object vectorValue = VectorUtils.convert(lambdaResult.valueAsObject(i));

                        // Flatten the inner vector
                        if (vectorValue instanceof ValueVector)
                        {
                            vectorBuilder.copy((ValueVector) vectorValue);
                            continue;
                        }
                    }
                    vectorBuilder.put(lambdaResult, i);
                }

                builder.put(vectorBuilder.build());
            };

            LambdaUtils.forEachLambdaResult(context, input, value, le, mapper);

            return builder.build();
        }

        ((ExecutionContext) context).getStatementContext()
                .setLambdaValue(le.getLambdaIds()[0], value);

        ValueVector result = le.getExpression()
                .eval(input, context);

        return result;
    }
}
