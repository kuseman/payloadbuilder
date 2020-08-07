package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.time.ZoneOffset;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Returns current date */
class GetDateFunction extends ScalarFunctionInfo
{
    private final boolean utc;
    GetDateFunction(Catalog catalog, boolean utc)
    {
        super(catalog, utc ? "getutcdate" : "getdate", Type.SCALAR);
        this.utc = utc;
    }
    
    @Override
    public String getDescription()
    {
        return "Returns current " + (utc ? "UTC " : "") + " Date. " + System.lineSeparator() +
                "NOTE! That same value is used during the whole execution.";
    }
    
    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList();
    }
    
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        if (utc)
        {
            return context.getNow().withZoneSameInstant(ZoneOffset.UTC);
        }
        
        return context.getNow().toLocalDateTime();
    }
}
