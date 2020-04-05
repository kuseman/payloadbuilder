package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.utils.MapUtils;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Expression of a qualified name type. Column reference with a nested path. Ie. field.subField.value
 **/
public class QualifiedReferenceExpression extends Expression
{
    private final QualifiedName qname;
    /**
     * Unique id for this reference expression. Used to for storing {@link QualifiedReferenceContainer} in evaluation context
     */
    private final int uniqueId;
    /**
     * If this references a lambda parameter, this points to it's unique id in current scope. Used to retrieve the current lambda value from
     * evaluation context
     */
    private final int lambdaId;

    public QualifiedReferenceExpression(QualifiedName qname, int lambdaId)
    {
        this.qname = requireNonNull(qname, "qname");
        this.uniqueId = 0;
        this.lambdaId = lambdaId;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public int getUniqueId()
    {
        return uniqueId;
    }

    public int getLambdaId()
    {
        return lambdaId;
    }
    
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

            // 3. Parent alias match upwards
            current = current.getParent();
            resultRow = !resultRow.getParents().isEmpty() ? (Row) CollectionUtils.get(resultRow.getParents(), 0) : null;
        }

        if (resultRow == null)
        {
            return null;
        }
        
        current = resultRow.getTableAlias().getChildAlias(parts.get(partIndex));
        if (current != null)
        {
            return resultRow.getChildRows(current.getParentIndex());
        }
        
        return resultRow.getObject(parts.get(partIndex));
    }

    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        // This is a lambda reference, pick current value from context
        if (lambdaId >= 0)
        {
            Object value = evaluationContext.getLambdaValue(lambdaId);
            
            if (qname.getParts().size() > 1)
            {
                if (value instanceof Row)
                {
                    return getValue((Row) value, qname.getParts().subList(1, qname.getParts().size()));
                }
                
                throw new IllegalArgumentException("Cannot dereference value: " + value);
            }
            
            return value;
        }

        return getValue(row, qname.getParts());
        //        QualifiedReferenceContainer container = evaluationContext.getContainer(qname, uniqueId);
        //        return container.getValue(row);

        //        
        //        
        //        int size = qname.getParts().size();
        //        int partStart = 0;
        //        
        //        TableAlias alias = row.getTableAlias();
        //        
        //        // Prio 1. Alias of the current rows alias
        //        if (alias.getAlias().equals(qname.getFirst()))
        //        {
        //            partStart++;
        //        }
        //        
        ////        else
        ////        {
        ////        alias = alias.getChildAlias(qname.getAlias());
        ////        if (alias != null)
        ////        {
        ////            List<Row> childRows = row.getChildRows(alias.getParentIndex());
        ////            // TODO: traverse child rows
        ////
        ////            throw new NotImplementedException("Implement child rows traversal");
        ////        }
        ////        else
        ////        {
        //            // TODO: Not a child select check parent
        //            // else traverse Map if any
        //
        //            Object current = row.getObject(qname.getParts().get(partStart));
        //            if (current instanceof Map)
        //            {
        //                return MapUtils.traverse((Map<Object, Object>) current, qname.getParts().subList(partStart + 1, size)); 
        //            }
        //            else
        //            {
        //                throw new IllegalArgumentException("Cannot traverse object: " + current);
        //            }
        ////        }
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        // TODO: Fix column reference
        //       1. Cache ordinal
        //       2. Nested path
        //       3. deeper path than 2

        ExpressionCode code = ExpressionCode.code(context);

        // Lambda reference
        if (context.containsLambda(qname.getFirst()))
        {
            code.setCode(String.format(
                    "Object %s = %s;\n"
                        + "boolean %s = %s == null;\n",
                    code.getResVar(), qname.getFirst(), code.getIsNull(), code.getResVar()));
            return code;
        }

        //        List<String> parts = qname.getParts();
        String column = qname.getParts().get(0);

        //        if (context.biPredicate)
        //        {
        //            String alias = qname.getAlias();
        //            // Blank alias or inner alias => inner row else outer
        //            rowName = isBlank(alias) || Objects.equals(alias, context.tableAlias.getAlias()) ? "r_in" : "r_out";
        //            if (parts.size() > 1)
        //            {
        //                // Only one nest level supported for now
        //                column = parts.get(1);
        //            }
        //
        //        }

        //        String inputVariableName = rowName + "_" + column;
        //        if (context.addedVars.add(inputVariableName))
        //        {
        //            String stm = "Input " + inputVariableName + " = new Input(\"" + column + "\");" + System.lineSeparator();
        //            context.appendVariable(stm);
        //        }

        //        if (context.desiredType != null)
        //        {
        //            if (Number.class.isAssignableFrom(context.desiredType))
        //            {
        //                context.appendBody(inputVariableName + ".getNumber(" + rowName + ")");
        //                return null;
        //            }
        //            else if (boolean.class.isAssignableFrom(context.desiredType))
        //            {
        //                context.appendBody(inputVariableName + ".getBoolean(" + rowName + ")");
        //                return null;
        //            }
        //        }

        code.setCode(String.format(
                "Object %s = %s.getObject(\"%s\");\n"
                    + "boolean %s = %s == null;\n",
                code.getResVar(), context.getRowVarName(), column, code.getIsNull(), code.getResVar()));
        return code;
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
            QualifiedReferenceExpression e = (QualifiedReferenceExpression) obj;
            return qname.equals(e.qname)
                    &&
                    lambdaId == e.lambdaId;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname.toString();
    }

    /**
     * Container used to resolve a qualified reference Can cache traversal paths etc. 1. Traverse Row 2. Traverse Iterator 3. Traverse Map
     **/
    public static class QualifiedReferenceContainer
    {
        /** Cached information for {@link Row} input */
        private int parentSteps;
        private int[] childRowsPath;
        private int ordinal;

        private final QualifiedName qname;

        public QualifiedReferenceContainer(QualifiedName qname)
        {
            this.qname = qname;
        }

        @SuppressWarnings("unchecked")
        private Object getValue(Object input, int partStart)
        {
            if (input instanceof Row)
            {
                return getValue((Row) input);
            }
            else if (input instanceof Map)
            {
                // Assumes the whole part chain is combined maps
                return MapUtils.traverse((Map<Object, Object>) input, qname.getParts().subList(partStart, qname.getParts().size()));
            }
            else if (input instanceof Iterator)
            {
                return getValue((Iterator) input);

            }
            return null;
        }

        /**
         * Get value for arbitrary object input Can be a Map iterator etc.
         */
        Object getValue(Object input)
        {
            return getValue(input, 0);

        }

        /** Get value for itetator */
        Object getValue(Iterator<Object> iterator)
        {
            /*
             * Iterator input.
             * Example:
             *
             * filter(articleAttribute, aa -> aa.sku_id > 0).price
             * |-------------------------------------------| |----|
             * Dereference left----^
             * Qualified right --------------------------------^
             *
             * Filter returns an iterator
             * And is given to this qualified expression with parts "price"
             *
             */
            if (iterator.hasNext())
            {
                Object o = iterator.next();
                return getValue(o);
            }

            return null;
        }

        /** Get value for for */
        Object getValue(Row row)
        {
            if (row == null)
            {
                return null;
            }
            // TODO: Cache traversal steps
            //       Make sure that cached TableAlias belongs to rows table alias        

            // 1. Alias it self
            if (row.getTableAlias().getAlias().equals(qname.getFirst()))
            {
                return getValue(row.getObject(qname.getFirst()), 1);
            }

            // 2. Child alias
            TableAlias childAlias = row.getTableAlias().getChildAlias(qname.getFirst());
            if (childAlias != null)
            {
                return "child alias " + qname.getFirst();
            }

            // 3. Parent
            // TODO:

            // 4. Column
            int ordinal = ArrayUtils.indexOf(row.getTableAlias().getColumns(), qname.getFirst());
            return row.getObject(ordinal);
        }

        /** Clear state. */
        public void clear()
        {
        }
    }
}
