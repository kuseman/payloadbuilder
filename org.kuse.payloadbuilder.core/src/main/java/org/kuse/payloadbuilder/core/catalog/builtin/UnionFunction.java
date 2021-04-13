package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.IteratorUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Union function. Unions all arguments into a single collection */
class UnionFunction extends ScalarFunctionInfo
{
    UnionFunction(Catalog catalog, boolean all)
    {
        super(catalog, "union" + (all ? "all" : ""));
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Result of a union is the result of all arguments aliases
        return argumentAliases
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        int size = arguments.size();
        if (size <= 0)
        {
            return emptyList();
        }

        int length = arguments.size();
        Iterator<Object>[] iterators = new Iterator[length];
        for (int i = 0; i < length; i++)
        {
            Object o = arguments.get(i).eval(context);
            if (o instanceof Iterable)
            {
                iterators[i] = ((Iterable<Object>) o).iterator();
            }
            else if (o instanceof Iterator)
            {
                iterators[i] = (Iterator<Object>) o;
            }
            else
            {
                iterators[i] = IteratorUtils.singletonIterator(o);
            }
        }
        return IteratorUtils.chainedIterator(iterators);
    }
}
