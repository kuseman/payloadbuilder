package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.utils.MapUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Expression of a qualified name type.
 * Column reference with a nested path.
 * Ie. field.subField.value 
 **/
public class QualifiedReferenceExpression extends Expression
{
    protected final QualifiedName qname;

    public QualifiedReferenceExpression(QualifiedName qname)
    {
        this.qname = requireNonNull(qname, "qname");
    }

    public QualifiedName getQname()
    {
        return qname;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Object eval(Object evaluationContext, Row row)
    {
        int size = qname.getParts().size();
        int partStart = 0;
        
        TableAlias alias = row.getTableAlias();
        
        // Prio 1. Alias of the current rows alias
        if (alias.getAlias().equals(qname.getFirst()))
        {
            partStart++;
        }
        
//        else
//        {
//        alias = alias.getChildAlias(qname.getAlias());
//        if (alias != null)
//        {
//            List<Row> childRows = row.getChildRows(alias.getParentIndex());
//            // TODO: traverse child rows
//
//            throw new NotImplementedException("Implement child rows traversal");
//        }
//        else
//        {
            // TODO: Not a child select check parent
            // else traverse Map if any

            Object current = row.getObject(qname.getParts().get(partStart));
            if (current instanceof Map)
            {
                return MapUtils.traverse((Map<Object, Object>) current, qname.getParts().subList(partStart + 1, size)); 
            }
            else
            {
                throw new IllegalArgumentException("Cannot traverse object: " + current);
            }
//        }
    }

    @Override
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
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

        String rowName = "row";
        List<String> parts = qname.getParts();
        String column = qname.getParts().get(0);

        if (context.biPredicate)
        {
            String alias = qname.getAlias();
            // Blank alias or inner alias => inner row else outer
            rowName = isBlank(alias) || Objects.equals(alias, context.tableAlias.getAlias()) ? "r_in" : "r_out";
            if (parts.size() > 1)
            {
                // Only one nest level supported for now
                column = parts.get(1);
            }

        }

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
                code.getResVar(), rowName, column, code.getIsNull(), code.getResVar()));
        return code;
    }

    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return qname.toString();
    }
    
    /** Container used to resolve a qualified reference
     * Can cache traversal paths etc.
     * 1. Traverse Row
     * 2. Traverse Iterator
     * 3. Traverse Map
     **/
    public class QualifiedReferenceContainer
    {
        // Number of steps to traverse up 
        private int parentSteps;
    }
}
