package se.kuseman.payloadbuilder.api.expression;

import static java.util.Collections.emptyList;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** Definition of an expression */
public interface IExpression
{
    /** Evaluate expression against input */
    ValueVector eval(TupleVector input, IExecutionContext context);

    /**
     * Evaluate expression. NOTE! This method should only be called on expression not needing any intput vector. Ie. from a {@link IPredicate}
     */
    default ValueVector eval(IExecutionContext context)
    {
        return eval(TupleVector.CONSTANT, context);
    }

    /** Get type of this expression */
    ResolvedType getType();

    /** Is this expression constant */
    default boolean isConstant()
    {
        List<IExpression> children = getChildren();
        if (children.isEmpty())
        {
            return false;
        }
        return children.stream()
                .allMatch(IExpression::isConstant);
    }

    /** Accept visitor */
    <T, C> T accept(IExpressionVisitor<T, C> visitor, C context);

    /** Fold this expression. Eliminate constants etc. */
    default IExpression fold()
    {
        return this;
    }

    /** Get child expressions if any */
    default List<IExpression> getChildren()
    {
        return emptyList();
    }

    /** Returns true if this expression is internal and used between plans etc. and should not be written to output */
    default boolean isInternal()
    {
        return false;
    }

    /** Return a column reference for this expression if any */
    default ColumnReference getColumnReference()
    {
        return null;
    }

    /** Return a qualified column name for this expression if any exists otherwise null */
    default QualifiedName getQualifiedColumn()
    {
        return null;
    }

    /** Return a verbose string that can be used in plan printing etc. for easier debugging. */
    default String toVerboseString()
    {
        return toString();
    }

    /**
     * Returns true if this expression is semantic equal to provided expression. That is the meaning is equal but not necessary object equality equal. ie. "col1 + col2" = "col2 + col2"
     */
    default boolean semanticEquals(IExpression expression)
    {
        return equals(expression);
    }
}
