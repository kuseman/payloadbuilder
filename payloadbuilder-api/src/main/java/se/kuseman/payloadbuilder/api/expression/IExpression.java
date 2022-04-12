package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Definition of an expression */
public interface IExpression
{
    /** Evaluate expression */
    Object eval(IExecutionContext context);

    /** Returns true if this expression is constant */
    default boolean isConstant()
    {
        return false;
    }

    /** Returns true if this expression supports code generation */
    default boolean isCodeGenSupported()
    {
        return false;
    }

    /**
     * Generate code for this expression
     **/
    default ExpressionCode generateCode(CodeGeneratorContext context)
    {
        throw new RuntimeException("Not implemented");
    }

    /** Return the qualified name that this expression refers to or null if no qualified name */
    default QualifiedName getQualifiedName()
    {
        return null;
    }

    /** Get data type of this expression */
    default DataType getDataType()
    {
        return DataType.ANY;
    }

}
