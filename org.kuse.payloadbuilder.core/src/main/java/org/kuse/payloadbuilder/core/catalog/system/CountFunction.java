package org.kuse.payloadbuilder.core.catalog.system;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.Operator.TupleList;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Counts input */
class CountFunction extends ScalarFunctionInfo
{
    CountFunction(Catalog catalog)
    {
        super(catalog, "count");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        int count = 0;
        if (obj instanceof TupleList)
        {
            return ((TupleList) obj).size();
        }
        else if (obj instanceof TupleIterator)
        {
            TupleIterator it = (TupleIterator) obj;
            while (it.hasNext())
            {
                it.next();
                count++;
            }
        }
        else if (obj instanceof Collection)
        {
            count = ((Collection<Object>) obj).size();
        }
        else if (obj instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) obj;
            while (it.hasNext())
            {
                it.next();
                count++;
            }
        }
        else if (obj instanceof Map)
        {
            count = ((Map) obj).size();
        }
        else if (obj != null)
        {
            // Everything else is 1
            count = 1;
        }
        return count;
    }
}
