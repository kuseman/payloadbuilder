package se.kuseman.payloadbuilder.core.parser;

import org.apache.commons.lang3.NotImplementedException;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.codegen.ProjectionCode;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Base class for expressions */
// CSOFF
public abstract class Expression implements IExpression
// CSON
{
    /** Visitor accept definition */
    public abstract <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context);

    @Override
    public DataType getDataType()
    {
        return DataType.ANY;
    }

    /** Fold's expression */
    public Expression fold()
    {
        return this;
    }

    /** Returns true if this expression is constant or not */
    @Override
    public boolean isConstant()
    {
        return false;
    }

    /** Returns true if this expression supports code generation */
    @Override
    public boolean isCodeGenSupported()
    {
        return false;
    }

    /**
     * Generate code for this expression
     *
     * @param context Context used during code generation
     **/
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        context.addImport("se.kuseman.payloadbuilder.core.parser.Expression");

        int index = context.addReference(this);
        ExpressionCode code = context.getExpressionCode();
        code.setCode(String.format("boolean %s = true;\n" // nullVar
                                   + "Object %s = ((Expression) references[%d]).eval(context);\n" // resVar, index
                                   + "%s = %s == null;\n", // nullVar, resVar
                code.getNullVar(), code.getResVar(), index, code.getNullVar(), code.getResVar()));

        return code;
    }

    /**
     * Generate projection code for this expression Default is to simply evaluate the expression and write it
     */
    public ProjectionCode generateProjectionCode(CodeGeneratorContext context)
    {
        ProjectionCode code = context.getProjectionCode();

        // Evaluate the expression if code is not supported for this expression
        if (!isCodeGenSupported())
        {
            context.addImport("se.kuseman.payloadbuilder.core.parser.Expression");
            String resVar = context.newVar("r");
            int index = context.addReference(this);
            code.setCode(String.format("Object %s = ((Expression) references[%d]).eval(context);\n" // resVar, index
                                       + "writer.writeValue(%s);\n", // resVar
                    resVar, index, resVar));

            return code;
        }

        context.addImport("se.kuseman.payloadbuilder.core.operator.ComplexValue");
        ExpressionCode expressionCode = generateCode(context);

        StringBuilder sb = new StringBuilder();
        sb.append(expressionCode.getCode());

        code.setCode(String.format("%s" // expression code
                                   + "if (%s) {\n" // expression nullVar
                                   + "  writer.writeValue(null);\n"
                                   + "}\n"
                                   + "%s" // DataType == ANY
                                   + "else {\n"
                                   + "  writer.write%s(%s);\n" // method, expression resVar
                                   + "}\n",
                expressionCode.getCode(), expressionCode.getNullVar(),

                getDataType() == DataType.ANY ? String.format("else if (%s instanceof ComplexValue) {\n" // expression resVar
                                                              + "  ((ComplexValue) %s).write(writer, context);\n"
                                                              + "}\n", // expression resVar
                        expressionCode.getResVar(), expressionCode.getResVar())
                        : "",

                getWriteMethodSuffix(), expressionCode.getResVar()));
        return code;
    }

    /**
     * Evaluate this expression with context
     *
     * @param context Context used during evaluation
     */
    @Override
    public Object eval(IExecutionContext context)
    {
        throw new NotImplementedException("Not implemented. eval: " + getClass().getSimpleName());
    }

    /** Return write method to use on {@link OutputWriter} for provided datatype */
    private String getWriteMethodSuffix()
    {
        DataType dataType = getDataType();
        switch (dataType)
        {
            case INT:
                return "Int";
            case LONG:
                return "Long";
            case FLOAT:
                return "Float";
            case DOUBLE:
                return "Double";
            case BOOLEAN:
                return "Bool";
            case ANY:
                return "Value";
            default:
                throw new IllegalArgumentException("Unknwon data type " + dataType);
        }
    }
}
