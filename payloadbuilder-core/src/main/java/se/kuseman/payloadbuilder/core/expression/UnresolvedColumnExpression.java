package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/**
 * An unresolved column expression. Used during planning phase to resolve to actual columns in schemas
 */
public class UnresolvedColumnExpression implements IExpression, HasAlias
{
    private final QualifiedName qname;
    private final int lambdaId;
    private final Token token;

    public UnresolvedColumnExpression(QualifiedName qname, int lambdaId, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.token = token;
    }

    public QualifiedName getColumn()
    {
        return qname;
    }

    public int getLambdaId()
    {
        return lambdaId;
    }

    public Token getToken()
    {
        return token;
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
        return ResolvedType.of(Type.Any); // throw new IllegalArgumentException("An unresolved column expression have no type");
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
            return qname.toDotDelimited()
                    .equalsIgnoreCase(that.qname.toDotDelimited());
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
            if (qname.toDotDelimited()
                    .equalsIgnoreCase(that.qname.toDotDelimited()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toDotDelimited();
    }
}
