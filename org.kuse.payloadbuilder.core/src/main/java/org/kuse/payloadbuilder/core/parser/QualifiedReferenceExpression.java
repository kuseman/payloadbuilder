package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/**
 * Expression of a qualified name type. Column reference with a nested path. Ie. field.subField.value
 **/
public class QualifiedReferenceExpression extends Expression
{
    private final QualifiedName qname;
    /**
     * <pre>
     * If this references a lambda parameter, this points to it's unique id in current scope.
     * Used to retrieve the current lambda value from evaluation context
     * </pre>
     */
    private final int lambdaId;

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId)
    {
        this.qname = requireNonNull(qname, "qname");
        this.lambdaId = lambdaId;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public int getLambdaId()
    {
        return lambdaId;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        // This is a lambda reference, pick current value from context
        if (lambdaId >= 0)
        {
            Object value = context.getLambdaValue(lambdaId);
            if (value == null)
            {
                return null;
            }

            List<String> parts = qname.getParts();
            if (parts.size() > 1)
            {
                //CSOFF
                if (value instanceof Tuple)
                //CSON
                {
                    // Skip first part here since that is the lambda part
                    return ((Tuple) value).getValue(qname, 1);
                }
                else if (value instanceof Map)
                {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> map = (Map<Object, Object>) value;
                    return MapUtils.traverse(map, 1, parts);
                }

                throw new IllegalArgumentException("Cannot dereference value: " + value);
            }

            return value;
        }

        Tuple tuple = context.getTuple();
        // No tuple set in context
        if (tuple == null)
        {
            return null;
        }

        return tuple.getValue(qname, 0);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return qname.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression that = (QualifiedReferenceExpression) obj;
            return qname.equals(that.qname)
                && lambdaId == that.lambdaId;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toString();
    }
}
