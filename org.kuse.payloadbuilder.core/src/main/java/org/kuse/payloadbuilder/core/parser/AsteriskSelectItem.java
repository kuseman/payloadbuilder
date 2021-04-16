package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.Token;

/** Projection for wildcards. With or without alias */
public class AsteriskSelectItem extends SelectItem
{
    private final String alias;
    /** Tuple ordinals that this asterisk is resolved into */
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
