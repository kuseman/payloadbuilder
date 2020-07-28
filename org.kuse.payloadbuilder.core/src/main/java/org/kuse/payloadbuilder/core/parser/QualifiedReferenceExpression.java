package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Row;
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
            
            if (qname.getParts().size() > 1)
            {
                List<String> subParts = qname.getParts().subList(1, qname.getParts().size());
                
                if (value instanceof Row)
                {
                    return getValue((Row) value, subParts);
                }
                else if (value instanceof Map)
                {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> map = (Map<Object, Object>) value;
                    return MapUtils.traverse(map, subParts);
                }
                
                throw new IllegalArgumentException("Cannot dereference value: " + value);
            }
            
            return value;
        }
        
        Row row = context.getRow();
        // No row set in context
        if (row == null)
        {
            return null;
        }
        
        return getValue(row, qname.getParts());
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
    
    /** Get value for provided row and parts */
    private Object getValue(Row row, List<String> parts)
    {
        TableAlias current = row.getTableAlias();
        Row resultRow = row;
        int partIndex = 0;
        
        while (resultRow != null && partIndex < parts.size() - 1)
        {
            String part = parts.get(partIndex);

            // 1. Alias match, move on
            if (Objects.equals(part, current.getAlias()))
            {
                partIndex ++;
                continue;
            }

            // 2. Child alias
            TableAlias alias = current.getChildAlias(part);
            if (alias != null)
            {
                partIndex ++;
                current = alias;
                List<Row> childAlias = resultRow.getChildRows(alias.getParentIndex());
                resultRow = !childAlias.isEmpty() ? (Row) CollectionUtils.get(childAlias, 0) : null;
                continue;
            }

            if (current.getParent() == null)
            {
                break;
            }
            
            // 3. Parent alias match upwards
            resultRow = resultRow.getParent();
            current = resultRow != null ? resultRow.getTableAlias() : null;
        }

        if (resultRow == null)
        {
            return null;
        }
        
        String lastPart = parts.get(partIndex);
        // Child rows pointer
        current = resultRow.getTableAlias().getChildAlias(lastPart);
        if (current != null)
        {
            return resultRow.getChildRows(current.getParentIndex());
        }
        // Parent rows pointer
        else if (resultRow.getTableAlias().getParent() != null && Objects.equals(lastPart, resultRow.getTableAlias().getParent().getAlias()))
        {
            return resultRow.getParents();
        }
        
        Object result = resultRow.getObject(parts.get(partIndex));
        if (result instanceof Map && partIndex < parts.size() -1)
        {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) result;
            return MapUtils.traverse(map, parts.subList(partIndex + 1, parts.size()));
        }
        
        return result;
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
