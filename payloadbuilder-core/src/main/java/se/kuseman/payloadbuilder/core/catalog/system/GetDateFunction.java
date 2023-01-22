package se.kuseman.payloadbuilder.core.catalog.system;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Returns current date */
class GetDateFunction extends ScalarFunctionInfo
{
    private final boolean utc;

    GetDateFunction(Catalog catalog, boolean utc)
    {
        super(catalog, utc ? "getutcdate"
                : "getdate", FunctionType.SCALAR);
        this.utc = utc;
    }

    @Override
    public String getDescription()
    {
        return "Returns current " + (utc ? "UTC "
                : "")
               + " Date. "
               + System.lineSeparator()
               + "NOTE! That same value is used during the whole execution.";
    }

    @Override
    public int arity()
    {
        return 0;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.DateTime);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ZonedDateTime now = ((StatementContext) context.getStatementContext()).getNow();
        if (!utc)
        {
            now = now.withZoneSameInstant(ZoneOffset.systemDefault());
        }

        return ValueVector.literalObject(ResolvedType.of(Type.DateTime), now, input.getRowCount());
    }
}
