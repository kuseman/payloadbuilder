package se.kuseman.payloadbuilder.core.catalog.system;

import java.time.ZoneOffset;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Returns current date */
class GetDateFunction extends ScalarFunctionInfo
{
    private final boolean utc;

    GetDateFunction(Catalog catalog, boolean utc)
    {
        super(catalog, utc ? "getutcdate"
                : "getdate");
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
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        if (utc)
        {
            return context.getStatementContext()
                    .getNow();
        }

        return context.getStatementContext()
                .getNow()
                .withZoneSameInstant(ZoneOffset.systemDefault())
                .toLocalDateTime();
    }
}
