package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference.Type;
import se.kuseman.payloadbuilder.core.parser.Location;

/** A logical scan of an expression returning a {@link Column.Type#Table} value */
public class ExpressionScan extends TableSource
{
    private final Schema schema;
    private final IExpression expression;
    private final Location location;

    public ExpressionScan(TableSourceReference tableSource, Schema schema, IExpression expression, Location location)
    {
        super(tableSource);
        this.schema = requireNonNull(schema, "schema");
        this.expression = requireNonNull(expression, "expression");
        this.location = location;
        if (tableSource.getType() != Type.EXPRESSION)
        {
            throw new IllegalArgumentException("Wrong table source type");
        }

    }

    public IExpression getExpression()
    {
        return expression;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
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
        else if (obj instanceof ExpressionScan that)
        {
            return super.equals(obj)
                    && schema.equals(that.schema)
                    && expression.equals(that.expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Scan expression: " + expression.toVerboseString() + " " + tableSource;
    }
}
