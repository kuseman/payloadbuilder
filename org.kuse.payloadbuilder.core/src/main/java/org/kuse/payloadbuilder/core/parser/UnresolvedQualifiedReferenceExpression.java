package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** A unresolved qualified expression */
public class UnresolvedQualifiedReferenceExpression extends Expression implements HasIdentifier
{
    private final QualifiedName qname;
    /**
     * <pre>
     * If this references a lambda parameter, this points to it's unique id in current scope.
     * Used to retrieve the current lambda value from evaluation context
     * </pre>
     */
    private final int lambdaId;
    private final Token token;

    UnresolvedQualifiedReferenceExpression(QualifiedName qname, int lambdaId, Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
        this.token = token;
    }

    public QualifiedName getQname()
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
    public String identifier()
    {
        return qname.getLast();
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        throw new IllegalStateException("Cannot evaluate " + getClass().getName());
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        throw new IllegalStateException("Cannot generate code for " + getClass().getName());
    }

    @Override
    public int hashCode()
    {
        return qname.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof UnresolvedQualifiedReferenceExpression)
        {
            UnresolvedQualifiedReferenceExpression that = (UnresolvedQualifiedReferenceExpression) obj;
            return qname.equals(that.qname)
                && lambdaId == that.lambdaId;
        }
        // Special case used in testing where the resolve part isn't tested
        // but rather object structure with operators etc, then we only test that the qualifiers is equals
        else if (obj instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression that = (QualifiedReferenceExpression) obj;
            return qname.equals(that.getQname())
                && lambdaId == that.getLambdaId();
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toDotDelimited();
    }
}
