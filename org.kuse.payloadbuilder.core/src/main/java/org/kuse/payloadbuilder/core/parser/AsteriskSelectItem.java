package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ObjectUtils;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.operator.Tuple.TupleColumn;

/** Projection for wildcards. With or without alias */
public class AsteriskSelectItem extends SelectItem implements Projection
{
    private static final List<Integer> ALL_ASTERISK_ORDINALS = singletonList(-1);
    private final String alias;
    /**
     * If this select items is alias-based and is contained in a multi alias context we need to traverse values for all
     */
    private List<Integer> aliasTupleOrdinals;
    private final boolean recursive;

    public AsteriskSelectItem(String alias, boolean recursive, Token token)
    {
        super("", false, token);
        this.alias = alias;
        this.recursive = recursive;
    }

    /** Temporary setter until parser/analyzer is done in a 2 pahse fashion */
    public void setAliasTupleOrdinals(List<Integer> aliasTupleOrdinals)
    {
        if (this.aliasTupleOrdinals != null)
        {
            throw new IllegalArgumentException("Cannot set already set property aliasTupleOrdinals");
        }
        requireNonNull(aliasTupleOrdinals, "aliasTupleOrdinals");
        if (aliasTupleOrdinals.isEmpty())
        {
            throw new IllegalArgumentException("Empty alias ordinals");
        }
        this.aliasTupleOrdinals = unmodifiableList(aliasTupleOrdinals);
    }

    public String getAlias()
    {
        return alias;
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        Tuple tuple = context.getTuple();
        List<Integer> tupleOrdinals = ObjectUtils.defaultIfNull(aliasTupleOrdinals, ALL_ASTERISK_ORDINALS);
        for (int tupleOrdinal : tupleOrdinals)
        {
            Iterator<TupleColumn> it = tuple.getColumns(tupleOrdinal);
            while (it.hasNext())
            {
                TupleColumn column = it.next();
                writer.writeFieldName(column.getColumn());

                Tuple ordinalTuple = tuple.getTuple(column.getTupleOrdinal());
                Object value = ordinalTuple != null ? ordinalTuple.getValue(column.getColumn()) : null;
                writer.writeValue(value);
            }
        }
    }

    @Override
    public int hashCode()
    {
        return alias != null ? alias.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof AsteriskSelectItem)
        {
            AsteriskSelectItem that = (AsteriskSelectItem) obj;
            return Objects.equals(alias, that.alias);
        }
        return false;
    }
}
