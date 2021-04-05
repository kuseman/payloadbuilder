package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

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
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
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
