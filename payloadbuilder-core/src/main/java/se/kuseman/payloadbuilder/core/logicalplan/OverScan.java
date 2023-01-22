package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Outer scan. Used in nested loops as inner operator to return the current outer value */
public class OverScan implements ILogicalPlan
{
    private final Schema schema;
    private final String overAlias;
    private final int ordinal;
    private final Token token;

    public OverScan(Schema schema, String overAlias, int ordinal, Token token)
    {
        this.schema = requireNonNull(schema, "schema");
        this.overAlias = requireNonNull(overAlias, "overAlias");
        this.ordinal = ordinal;
        this.token = token;
    }

    public String getOverAlias()
    {
        return overAlias;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(overAlias);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (obj instanceof OverScan)
        {
            OverScan that = (OverScan) obj;
            return overAlias.equals(that.overAlias)
                    && ordinal == that.ordinal
                    && schema.equals(that.schema);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Over scan, alias:" + overAlias
               + (ordinal >= 0 ? " (" + ordinal + ")"
                       : "");
    }
}
