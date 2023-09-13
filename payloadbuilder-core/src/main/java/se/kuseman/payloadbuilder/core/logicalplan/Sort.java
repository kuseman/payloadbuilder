package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.common.SortItem;

/** Logical sort. Sorts input by sort items */
public class Sort implements ILogicalPlan
{
    private final ILogicalPlan input;
    private final List<SortItem> sortItems;

    public Sort(ILogicalPlan input, List<SortItem> sortItems)
    {
        this.input = requireNonNull(input, "input");
        this.sortItems = requireNonNull(sortItems, "sortItems");
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public List<SortItem> getSortItems()
    {
        return sortItems;
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
        if (obj instanceof Sort)
        {
            Sort that = (Sort) obj;
            return input.equals(that.input)
                    && sortItems.equals(that.sortItems);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Sort: " + sortItems.stream()
                .map(i -> i.getExpression()
                        .toVerboseString() + " " + i.getOrder())
                .collect(joining(", "));
    }
}
