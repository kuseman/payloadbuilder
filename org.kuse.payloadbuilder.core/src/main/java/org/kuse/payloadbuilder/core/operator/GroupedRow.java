package org.kuse.payloadbuilder.core.operator;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.iterators.TransformIterator;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Grouped row. Result of a {@link GroupByOperator} */
class GroupedRow implements Tuple
{
    private final List<Tuple> tuples;
    /** Set of columns in group expressions, these columns should not return an aggregated value */
    private final Map<String, QualifiedName> columnReferences;

    GroupedRow(List<Tuple> tuples, Map<String, QualifiedName> columnReferences)
    {
        if (isEmpty(tuples))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.tuples = tuples;
        this.columnReferences = columnReferences;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return tuples.get(0).containsAlias(alias);
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        String column = qname.getLast();
        QualifiedName columnRef = columnReferences.get(column);
        if (columnRef != null)
        {
            return tuples.get(0).getValue(columnRef, 0);
        }

        return new TransformIterator(tuples.iterator(), tuple -> ((Tuple) tuple).getValue(qname, partIndex));
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        // Use first row here.
        // TODO: Might need to distinct all grouped rows qnames since there might be different ones further down
        return tuples.get(0).getQualifiedNames();
    }
}
