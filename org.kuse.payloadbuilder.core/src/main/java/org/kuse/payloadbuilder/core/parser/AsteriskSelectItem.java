package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.Token;

/** Projection for wildcards. With or without alias */
public class AsteriskSelectItem extends SelectItem
{
    private final String alias;
    /** Tuple ordinals that this asterisk is resolved into */
    private final List<Integer> aliasTupleOrdinals;

    /** 1:st phase constructor */
    public AsteriskSelectItem(String alias, Token token)
    {
        super("", false, token);
        this.alias = alias;
        this.aliasTupleOrdinals = null;
    }

    /** 2:nd phase constructor */
    public AsteriskSelectItem(String alias, Token token, List<Integer> aliasTupleOrdinals)
    {
        super("", false, token);
        this.alias = alias;
        this.aliasTupleOrdinals = requireNonNull(aliasTupleOrdinals, "aliasTupleOrdinals");
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

    /** Get alias tuple ordinals */
    public List<Integer> getAliasTupleOrdinals()
    {
        if (aliasTupleOrdinals == null)
        {
            throw new IllegalArgumentException("Asterisk select not resolved yet");
        }
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
