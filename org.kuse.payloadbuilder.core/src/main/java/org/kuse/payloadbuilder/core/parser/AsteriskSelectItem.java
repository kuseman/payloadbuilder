package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Tuple;

/** Projection for wildcards. With or without alias */
public class AsteriskSelectItem extends SelectItem implements Projection
{
    private final String alias;
    /**
     * If this select items is alias-based and is contained in a multi alias context we need to traverse values for all
     */
    private List<Integer> aliasTupleOrdinals;

    public AsteriskSelectItem(String alias, Token token)
    {
        super("", false, token);
        this.alias = alias;
    }

    /** Temporary setter until parser/analyzer is done in a 2 pahse fashion */
    @Deprecated
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

    @Override
    public boolean isAsterisk()
    {
        return true;
    }

    public List<Integer> getAliasTupleOrdinals()
    {
        return aliasTupleOrdinals;
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

        if (aliasTupleOrdinals == null)
        {
            writeTupleValues(writer, tuple);
        }
        else
        {
            for (int tupleOrdinal : aliasTupleOrdinals)
            {
                writeTupleValues(writer, tuple.getTuple(tupleOrdinal));
            }
        }
    }

    private void writeTupleValues(OutputWriter writer, Tuple tuple)
    {
        if (tuple == null)
        {
            return;
        }
        int count = tuple.getColumnCount();
        for (int i = 0; i < count; i++)
        {
            writer.writeFieldName(tuple.getColumn(i));
            writer.writeValue(tuple.getValue(i));
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
