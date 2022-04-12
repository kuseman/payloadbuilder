package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Distinct input */
class DistinctFunction extends ScalarFunctionInfo
{
    DistinctFunction(Catalog catalog)
    {
        super(catalog, "distinct");
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Distinct result aliases is the input arguments aliases
        return argumentAliases.get(0);
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        Set<Object> result = null;
        if (obj instanceof Iterator)
        {
            result = new HashSet<>();
            Iterator<Object> it = (Iterator<Object>) obj;
            while (it.hasNext())
            {
                result.add(it.next());
            }
        }
        else if (obj instanceof Set)
        {
            result = (Set<Object>) obj;
        }
        else if (obj instanceof Collection)
        {
            result = new HashSet<>((Collection<Object>) obj);
        }

        return result;
    }
}
