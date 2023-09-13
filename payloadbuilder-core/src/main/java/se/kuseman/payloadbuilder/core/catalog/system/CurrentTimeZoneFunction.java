package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.TimeZone;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Function that returns the current systems time zone */
class CurrentTimeZoneFunction extends ScalarFunctionInfo
{
    CurrentTimeZoneFunction()
    {
        super("current_timezone", FunctionType.SCALAR);
    }

    @Override
    public Arity arity()
    {
        return Arity.ZERO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        return ValueVector.literalString(TimeZone.getDefault()
                .getID(), input.getRowCount());
    }
}
