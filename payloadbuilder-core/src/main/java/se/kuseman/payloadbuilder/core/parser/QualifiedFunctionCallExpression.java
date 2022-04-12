package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.expression.IQualifiedFunctionCallExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Scalar function call */
public class QualifiedFunctionCallExpression extends Expression implements IQualifiedFunctionCallExpression
{
    private final String catalogAlias;
    private final ScalarFunctionInfo functionInfo;
    private final List<Expression> arguments;
    private final Token token;

    public QualifiedFunctionCallExpression(String catalogAlias, ScalarFunctionInfo functionInfo, List<Expression> arguments, Token token)
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

    @Override
    public ScalarFunctionInfo getFunctionInfo()
    {
        return functionInfo;
    }

    @Override
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
    public Object eval(IExecutionContext context)
    {
        return functionInfo.eval(context, catalogAlias, arguments);
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
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + functionInfo.hashCode();
        hashCode = hashCode * 37 + arguments.hashCode();
        return hashCode;
        // CSON
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
        return functionInfo.getCatalog()
                .getName()
               + "#"
               + functionInfo.getName()
               + "("
               + arguments.stream()
                       .map(a -> a.toString())
                       .collect(joining(", "))
               + ")";
    }
}
