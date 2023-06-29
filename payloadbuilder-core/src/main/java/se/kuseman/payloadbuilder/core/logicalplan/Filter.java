package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** A filter that filters input according to a predicate */
public class Filter implements ILogicalPlan
{
    private final ILogicalPlan input;
    private final IExpression predicate;
    /** Table source that is set if this filter is a pushdown filter for a specific table source */
    private final TableSourceReference tableSource;

    public Filter(ILogicalPlan input, TableSourceReference tableSource, IExpression predicate)
    {
        this.input = requireNonNull(input, "input");
        this.tableSource = tableSource;
        this.predicate = requireNonNull(predicate, "predicate");
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    /** Return predicate for this filter */
    public IExpression getPredicate()
    {
        return predicate;
    }

    public TableSourceReference getTableSource()
    {
        return tableSource;
    }

    @Override
    public Schema getSchema()
    {
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
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof Filter)
        {
            Filter that = (Filter) obj;
            return input.equals(that.input)
                    && predicate.equals(that.predicate)
                    && Objects.equals(tableSource, that.tableSource);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Filter: " + predicate.toVerboseString();
    }
}
