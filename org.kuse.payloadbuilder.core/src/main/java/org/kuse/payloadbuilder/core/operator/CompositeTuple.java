package org.kuse.payloadbuilder.core.operator;

import static java.util.Spliterators.spliteratorUnknownSize;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Tuple that is composed of other tuples */
class CompositeTuple extends ArrayList<Tuple> implements Tuple
{
    CompositeTuple(Tuple outer, Tuple inner)
    {
        add(outer);
        add(inner);
    }

    CompositeTuple(CompositeTuple outer, Tuple inner)
    {
        addAll(outer);
        add(inner);
    }

    CompositeTuple(Tuple outer, CompositeTuple inner)
    {
        add(outer);
        addAll(inner);
    }

    List<Tuple> getTuples()
    {
        return this;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        int size = size();
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = get(i);
            if (tuple.containsAlias(alias))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        String part = qname.getParts().get(partIndex);

        int size = size();
        for (int i = 0; i < size; i++)
        {
            Tuple tuple = get(i);
            if (tuple.containsAlias(part))
            {
                // This is the last part in the qualified name => return the tuple itself
                if (partIndex == qname.getParts().size() - 1)
                {
                    return tuple;
                }
                return tuple.getValue(qname, partIndex + 1);
            }
        }

        return get(0).getValue(qname, partIndex);
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        return stream()
                .flatMap(tuple -> StreamSupport.stream(spliteratorUnknownSize(tuple.getQualifiedNames(), 0), false))
                .iterator();
    }
}
