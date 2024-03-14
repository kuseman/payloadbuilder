package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.vector.TupleVectorBuilder;

/**
 * Implementation of function object_array. Creates an array of objects from provided arguments.
 */
class ObjectArrayFunctionImpl
{
    static ResolvedType getAggregateType(List<IExpression> arguments)
    {
        // This is the aggregate but the arguments are considered non aggregate for the schema
        Schema schema = SchemaUtils.getSchema(Schema.EMPTY, arguments, false);
        return ResolvedType.table(schema);
    }

    static ResolvedType getOperatorType(Schema input)
    {
        return ResolvedType.table(input);
    }

    /** Operator eval */
    static ValueVector evalOperator(TupleVector input, IExecutionContext context)
    {
        int rowCount = input.getRowCount();
        if (rowCount == 0)
        {
            return ValueVector.literalNull(getOperatorType(input.getSchema()), 1);
        }

        TupleVectorBuilder builder = new TupleVectorBuilder(((ExecutionContext) context).getBufferAllocator(), 1);
        builder.append(input);

        // Tuple vector logically implements Array<Object> so simply return that
        return ValueVector.literalTable(builder.build(), 1);
    }
}
