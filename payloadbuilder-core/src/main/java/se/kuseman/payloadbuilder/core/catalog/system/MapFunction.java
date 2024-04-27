package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.singletonList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils;
import se.kuseman.payloadbuilder.core.execution.LambdaUtils.LambdaResultConsumer;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;

/** Map function. Maps input into another form */
class MapFunction extends ScalarFunctionInfo implements LambdaFunction
{
    private static final List<LambdaBinding> LAMBDA_BINDINGS = singletonList(new LambdaBinding(1, 0));

    MapFunction()
    {
        super("map", FunctionType.SCALAR);
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
        LambdaExpression le = (LambdaExpression) arguments.get(1);

        Type inputType = arguments.get(0)
                .getType()
                .getType();

        ResolvedType lambdaType = le.getExpression()
                .getType();

        if (inputType == Type.Array
                || inputType == Type.Table)
        {
            return ResolvedType.array(lambdaType);
        }
        else if (inputType == Type.Any)
        {
            return ResolvedType.of(Type.Any);
        }

        return lambdaType;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final LambdaExpression le = (LambdaExpression) arguments.get(1);
        final Type type = value.type()
                .getType();
        if (LambdaUtils.supportsForEachLambdaResult(type))
        {
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(getType(arguments), input.getRowCount());
            LambdaResultConsumer consumer = (inputResult, lambdaResult, inputWasListType, row) ->
            {
                if (inputWasListType)
                {
                    resultVector.setAny(resultVector.size(), lambdaResult);
                }
                else
                {
                    resultVector.copy(resultVector.size(), lambdaResult);
                }
            };

            LambdaUtils.forEachLambdaResult(context, input, value, le, consumer);
            return resultVector;
        }

        // Simple type just eval the lambda with inputs
        ((ExecutionContext) context).getStatementContext()
                .setLambdaValue(le.getLambdaIds()[0], value);

        return le.getExpression()
                .eval(input, context);
    }
}
