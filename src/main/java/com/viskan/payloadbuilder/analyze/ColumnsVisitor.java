package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.AExpressionVisitor;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedFunctionCallExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * Extracts columns used for each alias in a expression
 *
 * <pre>
 * Example join condition:
 *
 * ON  a.col1 = b.col1
 * AND a.col2 = b.col2
 *
 * We want to have result:
 *
 * a: [col1, col2]
 * b: [col1, col2]
 * 
 * Example select field with lambda expression:
 * 
 * aa.filter(sku -> sku.attr1_id == 0).count();
 * 
 * aa: [attr1_id]
 * 
 * </pre>
 */
public class ColumnsVisitor extends AExpressionVisitor<Void, ColumnsVisitor.Context>
{
    private static final ColumnsVisitor VISITOR = new ColumnsVisitor();

    private ColumnsVisitor()
    {
    }

    /** Context used during visitor */
    static class Context
    {
        // Current alias in scope
        TableAlias parentAlias;
        // Previous alias found by a QualifiedExpression
        TableAlias qualifiedAlias;
        
        Map<TableAlias, List<String>> columnsByQualifedName = new THashMap<>();
        TIntObjectMap<TableAlias> lambdaAliasById = new TIntObjectHashMap<>();
    }

    /**
     * Extracts columns be alias for provided expression
     **/
    public static Map<TableAlias, List<String>> getColumnsByAlias(TableAlias alias, Expression expression)
    {
        Context ctx = new Context();
        ctx.parentAlias = alias;
        expression.accept(VISITOR, ctx);
        return ctx.columnsByQualifedName;
    }

    @Override
    public Void visit(QualifiedReferenceExpression expression, Context context)
    {
        QualifiedName qname = expression.getQname();
        List<String> parts = new ArrayList<>(qname.getParts());
        TableAlias tableAlias;
        
        // Lambda reference => resolve table alias
        if (expression.getLambdaId() >= 0)
        {
            tableAlias = context.lambdaAliasById.get(expression.getLambdaId());
            
            // No alias connected to this lambda, return columns are unknown
            if (tableAlias == null)
            {
                return null;
            }
            
            // Remove first part since it's resolved to an alias due to lambda
            parts.remove(0);
        }
        // Else pick context alias
        else
        {
            tableAlias = context.parentAlias;
        }
        
        // More parts left to dig
        if (parts.size() > 0)
        {
            TableAlias prev = tableAlias;
            tableAlias = getFromQualifiedName(tableAlias, parts);
            
            // Table alias did not change, ie. we did not find a an alias
            if (prev != tableAlias)
            {
                // Set the found alias to context
                context.qualifiedAlias = tableAlias;
            }
        }
        
        // No parts left, return
        if (parts.isEmpty())
        {
            return null;
        }
        
        String column = parts.get(0);
        List<String> columns = context.columnsByQualifedName.computeIfAbsent(tableAlias, key -> new ArrayList<>());
        columns.add(column);
        return null;
    }
    
    @Override
    public Void visit(QualifiedFunctionCallExpression expression, Context context)
    {
        if (expression.getArguments().isEmpty())
        {
            return null;
        }

        if (expression.getArguments().stream().anyMatch(e -> e instanceof LambdaExpression))
        {
            /*
             * aa.filter(sku -> sku.sku_id > 0)
             * 
             * - First visit aa and resolve alias aa
             * - Then bind lamda identifier "sku" to alias
             *   aa to then resolve sku_id to alias aa
             */
            
            // First visit the 1:st argument to function to identify
            // the alias scope for this lambda function
            expression.getArguments().get(0).accept(this, context);
            
            // Connect lambda expressions to table alias set by first argument expression
            for (Expression arg : expression.getArguments())
            {
                if (arg instanceof LambdaExpression)
                {
                    LambdaExpression le = (LambdaExpression) arg;
                    for (int id : le.getLambdaIds())
                    {
                        context.lambdaAliasById.put(id, context.qualifiedAlias);
                    }
                    
                    le.getExpression().accept(this, context);
                    
                    // Clear lambda aliases
                    for (int id : le.getLambdaIds())
                    {
                        context.lambdaAliasById.remove(id);
                    }
                }
            }
            
            return null;
        }

        return super.visit(expression, context);
    }

    private TableAlias getFromQualifiedName(TableAlias parent, List<String> parts)
    {
        TableAlias result = parent;
        while (result != null)
        {
            if (parts.size() == 0)
            {
                break;
            }
            
            String part = parts.get(0);

            // 1. Alias pointing to current, move to next part
            if (Objects.equals(part, result.getAlias()))
            {
                parts.remove(0);
                continue;
            }

            // 2. Child alias, dig down and move on
            TableAlias alias = result.getChildAlias(part);
            if (alias != null)
            {
                result = alias;
                parts.remove(0);
                continue;
            }
            
            // Nothing found for current part, break loop
            break;
        }
        
        return result;
    }
}
