package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Definition of an aggregate expression */
public interface IAggregateExpression extends IExpression
{
    /**
     * Aggregate input groups. Input value vector is of type {@link Type#TupleVector} one for each group. Returning value should have the size equal to the input size with each ordinal as the
     * resulting aggregated value.
     * 
     * <pre>
     * Ie. For a sum aggregation with expression "sum(col1)"
     * 
     * Input
     *   Tuplevector 1
     *     col1:  [1,2,3]
     *     col2:  [4,5,6]
     *   Tuplevector 2
     *     col1:  [7,8,9]
     *     col2:  [10,11,12]
     * 
     * Result          1+2+3,  7+8+9
     *   ValueVector: [6,      24]
     * 
     * </pre>
     */
    ValueVector eval(ValueVector groups, IExecutionContext context);

    @Override
    default ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new IllegalArgumentException("Scalar eval should NOT be called on an aggregate expression");
    }
}
