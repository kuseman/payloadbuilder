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
                if (value instanceof Tuple)
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
        //
        //        return getValue(row, qname.getParts());
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

//    /** Get value for provided row and parts */
//    private Object getValue(Row row, List<String> parts)
//    {
//        TableAlias current = row.getTableAlias();
//        Row resultRow = row;
//        int size = parts.size();
//        int partIndex = 0;
//        String part;
//        // aliasA.aliasB.col2
//
//        // Traverse alias path
//        while (resultRow != null && partIndex < size - 1)
//        {
//            part = parts.get(partIndex);
//
//            // 1. Alias match, move on
//            if (Objects.equals(part, current.getAlias()))
//            {
//                partIndex++;
//                continue;
//            }
//
//            // 2. Child alias, extract first row from child collection
//            TableAlias alias = current.getChildAlias(part);
//            if (alias != null)
//            {
//                partIndex++;
//                current = alias;
//                List<Row> childRows = resultRow.getChildRows(alias);
//                resultRow = !childRows.isEmpty()
//                    ? childRows.get(0)
//                    : null;
//                continue;
//            }
//
//            // 3. Parent alias match upwards
//            // Parent traversal is only possible on first part of qname
//            if (partIndex == 0)
//            {
//                // NOTE! getParent is used here and not getParents
//                // since we are inside a multipart qname then we use
//                // the singular method since that one includes a predicate parent
//                // used in joins to avoid adding/indexing into lists too much.
//                Row temp = resultRow.getParent();
//                while (temp != null)
//                {
//                    if (Objects.equals(part, temp.getTableAlias().getAlias()))
//                    {
//                        break;
//                    }
//                    // Child access on parent
//                    alias = temp.getTableAlias().getChildAlias(part);
//                    if (alias != null)
//                    {
//                        List<Row> childRows = temp.getChildRows(alias);
//                        temp = childRows.isEmpty() ? null : childRows.get(0);
//                        break;
//                    }
//
//                    temp = temp.getParent();
//                }
//
//                // A parent was found
//                if (temp != null)
//                {
//                    resultRow = temp;
//                    current = resultRow.getTableAlias();
//                    partIndex++;
//                    continue;
//                }
//            }
//
//            // If we came here, there cannot be any more alias traversal matches
//            break;
//        }
//
//        if (resultRow == null)
//        {
//            return null;
//        }
//
//        part = parts.get(partIndex);
//        current = resultRow.getTableAlias().getChildAlias(part);
//        if (current != null)
//        {
//            return resultRow.getChildRows(current);
//        }
//
//        List<Row> tempRows = resultRow.getParents();
//        TableAlias temp = resultRow.getTableAlias().getParent();
//        while (temp != null && !tempRows.isEmpty())
//        {
//            if (Objects.equals(part, temp.getAlias()))
//            {
//                return tempRows;
//            }
//
//            temp = temp.getParent();
//            tempRows = tempRows.get(0).getParents();
//        }
//
//        Object result = resultRow.getObject(part);
//        if (result != null && partIndex < size - 1)
//        {
//            if (result instanceof Map)
//            {
//                @SuppressWarnings("unchecked")
//                Map<Object, Object> map = (Map<Object, Object>) result;
//                return MapUtils.traverse(map, partIndex + 1, parts);
//            }
//
//            throw new IllegalArgumentException("Cannot dereference value: " + result);
//        }
//
//        return result;
//    }

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
