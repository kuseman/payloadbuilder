package org.kuse.payloadbuilder.core.operator;

import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ProjectionCode;

/** Definition of a projection */
public interface Projection extends DescribableNode
{
    Projection NO_OP_PROJECTION = (writer, context) ->
    {
    };

    /** Write value for provided writer and current execution context */
    void writeValue(OutputWriter writer, ExecutionContext context);

    /** Returns true if this projection is of asterisk type */
    default boolean isAsterisk()
    {
        return false;
    }

    /**
     * To string with indent. Used when printing operator tree
     *
     * @param indent Indent count
     */
    default String toString(int indent)
    {
        return toString();
    }

    /**
     * Generate code for this projection
     * @param context Context used during code generation
     **/
    default ProjectionCode generateCode(CodeGeneratorContext context)
    {
        throw new NotImplementedException("code gen: " + getClass().getSimpleName());
    }
}
