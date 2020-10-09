package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Tuple that is a result from a sub query */
class SubQueryTuple implements Tuple
{
    private final Tuple tuple;
    private final String alias;

    SubQueryTuple(Tuple tuple, String alias)
    {
        this.tuple = tuple;
        this.alias = requireNonNull(alias, "alias");
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return this.alias.equalsIgnoreCase(alias);
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        int index = partIndex;
        if (qname.getParts().size() > partIndex && containsAlias(qname.getParts().get(partIndex)))
        {
            index++;
        }

        return tuple.getValue(qname, index);
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        return tuple.getQualifiedNames();
    }
}
