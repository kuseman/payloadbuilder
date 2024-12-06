package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * An unresolved column expression. Used during planning phase to resolve to actual columns in schemas
 */
public class UnresolvedColumnExpression implements IExpression, HasAlias
{
    private final QualifiedName qname;
    private final int lambdaId;
    private final Location location;

    public UnresolvedColumnExpression(QualifiedName qname, int lambdaId, Location location)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.location = location;
    }

    public QualifiedName getColumn()
    {
        return qname;
    }

    @Override
    public QualifiedName getQualifiedColumn()
    {
        return qname;
    }

    public int getLambdaId()
    {
        return lambdaId;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public Alias getAlias()
    {
        return new Alias(qname.getLast(), "");
    }

    @Override
    public boolean isConstant()
    {
        return false;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Any);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        if (visitor instanceof ICoreExpressionVisitor)
        {
            return ((ICoreExpressionVisitor<T, C>) visitor).visit(this, context);
        }

        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new IllegalArgumentException("An unresolved column expression should not be evaluated");
    }

    @Override
    public int hashCode()
    {
        return qname.hashCode();
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
        else if (obj instanceof UnresolvedColumnExpression)
        {
            UnresolvedColumnExpression that = (UnresolvedColumnExpression) obj;
            return lambdaId == that.lambdaId
                    && qname.equalsIgnoreCase(that.qname);
        }
        return false;
    }

    @Override
    public boolean semanticEquals(se.kuseman.payloadbuilder.api.expression.IExpression expression)
    {
        if (equals(expression))
        {
            return true;
        }
        else if (expression instanceof UnresolvedColumnExpression)
        {
            UnresolvedColumnExpression that = (UnresolvedColumnExpression) expression;
            if (qname.equalsIgnoreCase(that.qname))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toString();
    }
}
