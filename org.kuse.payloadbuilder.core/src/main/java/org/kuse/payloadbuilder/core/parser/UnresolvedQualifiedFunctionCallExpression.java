package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Scalar function call */
public class UnresolvedQualifiedFunctionCallExpression extends Expression
{
    private final String catalogAlias;
    private final String name;
    private final List<Expression> arguments;
    private final Token token;

    UnresolvedQualifiedFunctionCallExpression(
            String catalogAlias,
            String name,
            List<Expression> arguments,
            Token token)
    {
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.name = requireNonNull(name, "name");
        this.arguments = requireNonNull(arguments, "arguments");
        this.token = token;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getName()
    {
        return name;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    public Token getToken()
    {
        return token;
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
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + catalogAlias.hashCode();
        hashCode = hashCode * 37 + name.hashCode();
        hashCode = hashCode * 37 + arguments.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof UnresolvedQualifiedFunctionCallExpression)
        {
            UnresolvedQualifiedFunctionCallExpression that = (UnresolvedQualifiedFunctionCallExpression) obj;
            return catalogAlias.equals(that.catalogAlias)
                && name.equals(that.name)
                && arguments.equals(that.arguments);
        }
        // Special case used in testing where the resolve part isn't tested
        // but rather object structure with operators etc, then we only test that the qualifiers is equals
        else if (obj instanceof QualifiedFunctionCallExpression)
        {
            QualifiedFunctionCallExpression that = (QualifiedFunctionCallExpression) obj;
            return catalogAlias.equals(that.getCatalogAlias())
                && name.equals(that.getFunctionInfo().getName())
                && arguments.equals(that.getArguments());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return isBlank(catalogAlias) ? ""
            : (catalogAlias + "#")
                + name
                + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ")";
    }
}
