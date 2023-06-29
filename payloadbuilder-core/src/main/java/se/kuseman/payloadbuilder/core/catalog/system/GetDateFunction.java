package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Returns current date */
class GetDateFunction extends ScalarFunctionInfo
{
    private final boolean utc;

    GetDateFunction(boolean utc)
    {
        super(utc ? "getutcdate"
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
    public Arity arity()
    {
        return Arity.ZERO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.DateTime);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        StatementContext ctx = ((StatementContext) context.getStatementContext());
        long now = ctx.getNow();
        if (utc)
        {
            now = ctx.getNowUtc();
        }

        final EpochDateTime nowValue = EpochDateTime.from(now);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.DateTime);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public EpochDateTime getDateTime(int row)
            {
                return nowValue;
            }
        };
    }
}
