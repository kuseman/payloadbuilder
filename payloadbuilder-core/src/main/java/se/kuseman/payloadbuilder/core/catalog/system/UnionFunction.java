package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IteratorUtils;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Union function. Unions all arguments into a single collection */
class UnionFunction extends ScalarFunctionInfo
{
    UnionFunction(Catalog catalog, boolean all)
    {
        super(catalog, "union" + (all ? "all"
                : ""));
    }

    @Override
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        // Result of a union is the result of all arguments aliases
        return argumentAliases.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
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
            Object o = arguments.get(i)
                    .eval(context);
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
