package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ObjectUtils.getIfNull;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;

/** An aggregate either implicit or explicit */
public class Aggregate implements ILogicalPlan
{
    private final ILogicalPlan input;
    /** The expressions that is utilized when doing the aggregate. Empty if this is an implicit aggregate. */
    private final List<IExpression> aggregateExpressions;
    /** The output expressions from this aggregate */
    private final List<IAggregateExpression> projectionExpressions;
    private TableSourceReference parentTableSource;

    public Aggregate(ILogicalPlan input, List<IExpression> aggregateExpressions, List<IAggregateExpression> projectionExpressions, TableSourceReference parentTableSource)
    {
        this.input = requireNonNull(input, "input");
        this.aggregateExpressions = getIfNull(aggregateExpressions, emptyList());
        this.projectionExpressions = requireNonNull(projectionExpressions, "projectionExpressions");
        this.parentTableSource = parentTableSource;
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public List<IExpression> getAggregateExpressions()
    {
        return aggregateExpressions;
    }

    public List<IAggregateExpression> getProjectionExpressions()
    {
        return projectionExpressions;
    }

    public TableSourceReference getParentTableSource()
    {
        return parentTableSource;
    }

    @Override
    public Schema getSchema()
    {
        if (projectionExpressions.isEmpty())
        {
            return input.getSchema();
        }

        return SchemaUtils.getSchema(parentTableSource, projectionExpressions, true);
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
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof Aggregate that)
        {
            return input.equals(that.input)
                    && aggregateExpressions.equals(that.aggregateExpressions)
                    && projectionExpressions.equals(that.projectionExpressions)
                    && Objects.equals(parentTableSource, that.parentTableSource);
        }
        return false;
    }

    @Override
    public String toString()
    {
        if (aggregateExpressions.isEmpty())
        {
            return "Distinct";
        }

        return "Aggregate: On: " + aggregateExpressions.stream()
                .map(IExpression::toVerboseString)
                .collect(joining(", "))
               + ", Project: "
               + projectionExpressions.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "));
    }
}
