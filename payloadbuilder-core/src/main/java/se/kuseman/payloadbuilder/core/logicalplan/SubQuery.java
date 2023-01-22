package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.catalog.Schema;

/**
 * Sub query table source.
 * 
 * <pre>
 * 
 *  Select *
 *  from
 *  (
 *      select *
 *      from table
 *  ) x
 * 
 * </pre>
 */
public class SubQuery extends TableSource
{
    private final ILogicalPlan input;
    private final Token token;

    public SubQuery(ILogicalPlan input, String alias, Token token)
    {
        super("", alias);
        this.input = requireNonNull(input, "input");
        this.token = token;
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public Schema getSchema()
    {
        // Schema of a sub query is the sub queries schema
        return input.getSchema();
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SubQuery)
        {
            SubQuery that = (SubQuery) obj;
            return super.equals(that)
                    && input.equals(that.input);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "SubQuery (" + getAlias() + ")";
    }
}
