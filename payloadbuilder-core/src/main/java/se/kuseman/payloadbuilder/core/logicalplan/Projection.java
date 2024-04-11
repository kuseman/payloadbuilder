package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Projects input with a list of expressions */
public class Projection implements ILogicalPlan
{
    /** Downstream plan */
    private final ILogicalPlan input;
    /** Projection expressions */
    private final List<IExpression> expressions;
    /** True if input column should be appended to output. Used if thie projection is used to computed new values to input */
    private final boolean appendInputColumns;

    public Projection(ILogicalPlan input, List<IExpression> expressions, boolean appendInputColumns)
    {
        this.appendInputColumns = appendInputColumns;
        this.input = requireNonNull(input, "input");
        this.expressions = requireNonNull(expressions, "expressions");
    }

    public List<IExpression> getExpressions()
    {
        return expressions;
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public boolean isAppendInputColumns()
    {
        return appendInputColumns;
    }

    @Override
    public Schema getSchema()
    {
        return SchemaUtils.getSchema(input.getSchema(), expressions, appendInputColumns, false);
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
        else if (obj instanceof Projection)
        {
            Projection that = (Projection) obj;
            return input.equals(that.input)
                    && expressions.equals(that.expressions)
                    && appendInputColumns == that.appendInputColumns;
        }
        return false;
    }

    @Override
    public String toString()
    {
        // Use verbose string in plan printing
        return (appendInputColumns ? "Compute: "
                : "Projection: ") + expressions.stream()
                        .map(i -> i.toVerboseString())
                        .collect(joining(", "));
    }
}
