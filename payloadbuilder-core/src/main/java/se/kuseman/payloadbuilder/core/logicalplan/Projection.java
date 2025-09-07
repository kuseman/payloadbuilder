package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;

/** Projects input with a list of expressions */
public class Projection implements ILogicalPlan
{
    /** Downstream plan */
    private final ILogicalPlan input;
    /** Projection expressions */
    private final List<IExpression> expressions;
    private final TableSourceReference parentTableSource;

    public Projection(ILogicalPlan input, List<IExpression> expressions, TableSourceReference parentTableSource)
    {
        this.input = requireNonNull(input, "input");
        this.expressions = requireNonNull(expressions, "expressions");
        this.parentTableSource = parentTableSource;
        if (expressions.isEmpty())
        {
            throw new IllegalArgumentException("Expressions cannot be empty");
        }
    }

    public List<IExpression> getExpressions()
    {
        return expressions;
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public TableSourceReference getParentTableSource()
    {
        return parentTableSource;
    }

    /** Returns true if this projection is a single non qualified asterisk projection. */
    public boolean isAsteriskProjection()
    {
        // Pre-resolved projection then we have a simple "select *"
        if (expressions.size() == 1
                && expressions.get(0) instanceof AsteriskExpression ae
                && ae.getQname()
                        .size() == 0)
        {
            return true;
        }

        // Resolved and expanded projection, then we are equal to input
        if (input.getSchema()
                .equals(SchemaUtils.getSchema(parentTableSource, expressions, false)))
        {
            return true;
        }

        return false;
    }

    @Override
    public Schema getSchema()
    {
        /* Asterisk select => return input schema */
        if (isAsteriskProjection())
        {
            return input.getSchema();
        }

        return SchemaUtils.getSchema(parentTableSource, expressions, false);
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
        else if (obj instanceof Projection that)
        {
            return input.equals(that.input)
                    && expressions.equals(that.expressions)
                    && Objects.equals(parentTableSource, that.parentTableSource);
        }
        return false;
    }

    @Override
    public String toString()
    {
        // Use verbose string in plan printing
        return "Projection: " + expressions.stream()
                .map(i -> i.toVerboseString())
                .collect(joining(", "));
    }
}
