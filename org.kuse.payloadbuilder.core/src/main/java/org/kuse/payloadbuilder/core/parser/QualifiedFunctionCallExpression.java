package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.TableMeta.DataType;

/** Scalar function call */
public class QualifiedFunctionCallExpression extends Expression
{
    private final String catalogAlias;
    private final ScalarFunctionInfo functionInfo;
    private final List<Expression> arguments;
    private final Token token;

    public QualifiedFunctionCallExpression(
            String catalogAlias,
            ScalarFunctionInfo functionInfo,
            List<Expression> arguments,
            Token token)
    {
        this.catalogAlias = catalogAlias;
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.token = token;
        this.arguments = requireNonNull(arguments, "arguments");
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public ScalarFunctionInfo getFunctionInfo()
    {
        return functionInfo;
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
    public boolean isConstant()
    {
        return functionInfo.isConstant(arguments);
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        return functionInfo.eval(context, arguments);
    }

    @Override
    public boolean isCodeGenSupported()
    {
        return functionInfo.isCodeGenSupported(arguments);
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        return functionInfo.generateCode(context, arguments);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public DataType getDataType()
    {
        return functionInfo.getDataType(arguments);
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + functionInfo.hashCode();
        hashCode = hashCode * 37 + arguments.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedFunctionCallExpression)
        {
            QualifiedFunctionCallExpression that = (QualifiedFunctionCallExpression) obj;
            return functionInfo.equals(that.functionInfo)
                && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return functionInfo.getCatalog().getName() + "#"
            + functionInfo.getName()
            + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ")";
    }
}
