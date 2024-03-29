package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Marker class that used used in projections. Is expanded before executing
 * 
 * <pre>
 * An asterisk expression comes in many different forms.
 * They are allowed at:
 *  - SELECT * FROM table                         &lt;-- unqualified in select
 *  - SELECT a.* FROM table a                     &lt;-- qualified in select
 *  - SELECT COUNT(*) FROM table                  &lt;-- Special argument to some functions. Resolved runtime
 *  - SELECT (SELECT * FOR OBJECTARRAY OVER v) a  &lt;-- unqualified in sub query
 *  - SELECT a.map.* FROM table                   &lt;-- qualified in select that expands a nested typ (Not implementd yet)
 * </pre>
 */
public class AsteriskExpression implements IExpression
{
    /** Qualifier before the asterisk */
    private final QualifiedName qname;
    private final Location location;

    /** Create a unqualified asterisk */
    public AsteriskExpression(Location location)
    {
        this(QualifiedName.of(), location);
    }

    /** Create a qualified asterisk */
    public AsteriskExpression(QualifiedName qname, Location location)
    {
        this.qname = requireNonNull(qname, "qname");
        this.location = location;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public ResolvedType getType()
    {
        throw new RuntimeException("An asterisk expression has no type");
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new RuntimeException("An asterisk expression should not be evaluated");
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
    public int hashCode()
    {
        return Objects.hashCode(qname);
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
        else if (obj instanceof AsteriskExpression)
        {
            AsteriskExpression that = (AsteriskExpression) obj;
            return Objects.equals(qname, that.qname);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (qname != null ? qname + "."
                : "")
               + "*";
    }
}
