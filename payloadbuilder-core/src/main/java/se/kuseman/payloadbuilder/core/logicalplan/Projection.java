package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.StringUtils;
import se.kuseman.payloadbuilder.core.expression.HasAlias;

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
        Schema schema = input.getSchema();
        List<Column> columns = new ArrayList<>(expressions.size() + (appendInputColumns ? schema.getSize()
                : 0));
        for (IExpression expression : expressions)
        {
            String name = "";
            String outputName = "";
            if (expression instanceof HasAlias)
            {
                HasAlias.Alias alias = ((HasAlias) expression).getAlias();
                name = alias.getAlias();
                outputName = alias.getOutputAlias();
                if (StringUtils.isBlank(name))
                {
                    outputName = expression.toString();
                }
            }
            else
            {
                outputName = expression.toString();
            }
            ResolvedType type = expression.getType();
            ColumnReference columnReference = expression.getColumnReference();
            columns.add(new Column(name, outputName, type, columnReference, expression.isInternal()));
        }

        if (appendInputColumns)
        {
            columns.addAll(schema.getColumns());
        }

        return new Schema(columns);
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
            boolean b = input.equals(that.input)
                    && expressions.equals(that.expressions)
                    && appendInputColumns == that.appendInputColumns;

            if (!b)
            {
                System.out.println(this);
                System.out.println(that);

            }

            return b;
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
