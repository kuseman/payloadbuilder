package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.LambdaFunction;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.parser.tree.AExpressionVisitor;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LambdaExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedFunctionCallExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import avro.shaded.com.google.common.base.Objects;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;

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
 * </pre>
 */
public class ColumnsVisitor extends AExpressionVisitor<Set<TableAlias>, ColumnsVisitor.Context>
{
    private static final ColumnsVisitor VISITOR = new ColumnsVisitor();

    private ColumnsVisitor()
    {
    }

    /** Context used during visitor */
    static class Context
    {
        /**
         * The aliases that was the result after processing the expression.
         *
         * <pre>
         * Ie.
         * Parent alias: s
         * Expression: "aa"
         * Will yield aa as resulting alias
         *
         * Parent alias: s
         * Expression: "aa.map(x -> x.sku_id)"
         * Will yield s as result because the output it not an alias
         *
         * Parent alias: s
         * Expression: "aa.concat(ra.flatMap(x -> x.aa))"
         * Will yield [s/aa, s/ra/aa] as result
         * </pre>
         **/
        Set<TableAlias> parentAliases = new THashSet<>();
        /** Columns by alias found */
        Map<TableAlias, Set<String>> columnsByAlias;
        /** Lambda bindings. Holds which lambda id points to which alias */
        TIntObjectMap<Set<TableAlias>> lambdaAliasById = new TIntObjectHashMap<>();
    }

    /**
     * Extracts columns per alias for provided expression Reusing a map to populate result
     **/
    public static Set<TableAlias> getColumnsByAlias(Map<TableAlias, Set<String>> map, TableAlias alias, Expression expression)
    {
        Context ctx = new Context();
        // Start with input alias
        ctx.parentAliases.add(alias);
        ctx.columnsByAlias = map;
        Set<TableAlias> result = expression.accept(VISITOR, ctx);
        return result.isEmpty() ? ctx.parentAliases : result;
    }
    
    @Override
    protected Set<TableAlias> defaultResult(Context context)
    {
        return context.parentAliases;
    }

    @Override
    public Set<TableAlias> visit(QualifiedReferenceExpression expression, Context context)
    {
        QualifiedName qname = expression.getQname();
        List<String> parts = new ArrayList<>(qname.getParts());
        Set<TableAlias> tableAliases = context.parentAliases;

        // Lambda reference => resolve table alias
        if (expression.getLambdaId() >= 0)
        {
            tableAliases = context.lambdaAliasById.get(expression.getLambdaId());
            
            // No alias connected to this lambda, return columns are unknown
            if (tableAliases == null)
            {
                return null;
            }
            
            // Remove first part since it's resolved to an alias due to lambda
            parts.remove(0);
        }

        // Nothing left to process
        if (parts.isEmpty())
        {
            return null;
        }
        
        Set<TableAlias> output = new THashSet<>();
        for (TableAlias alias : tableAliases)
        {
            List<String> tempParts = new ArrayList<>(parts);
            TableAlias tempAlias = getFromQualifiedName(alias, tempParts);

            if (tempParts.isEmpty())
            {
                output.add(tempAlias);
                continue;
            }

            Set<String> columns = context.columnsByAlias.computeIfAbsent(tempAlias, key -> new THashSet<>());
            String column = tempParts.get(0);
            columns.add(column);
        }
        
        return output;
    }

    @Override
    public Set<TableAlias> visit(QualifiedFunctionCallExpression expression, Context context)
    {
        ScalarFunctionInfo functionInfo = expression.getFunctionInfo();
        
        // Store parent aliases before resolving this function call
        Set<TableAlias> parentAliases = context.parentAliases;
        // Bind lambda parameters
        if (functionInfo instanceof LambdaFunction)
        {
            List<Pair<Expression, LambdaExpression>> lambdaBindings = ((LambdaFunction) functionInfo).getLambdaBindings(expression.getArguments());
            for (Pair<Expression, LambdaExpression> pair : lambdaBindings)
            {
                Set<TableAlias> lambdaAliases = pair.getLeft().accept(this, context);
                if (isEmpty(lambdaAliases))
                {
                    continue;
                }
                
                for (int id : pair.getRight().getLambdaIds())
                {
                    context.lambdaAliasById.put(id, lambdaAliases);
                }
            }
        }

        // Visit all arguments
        expression.getArguments().forEach(a -> a.accept(this, context));
        
        // Resolve alias from function
        Set<TableAlias> result = functionInfo.resolveAlias(parentAliases, expression.getArguments(), e -> e.accept(this, context));
        if (isEmpty(result))
        {
            result = parentAliases;
        }
            
        context.parentAliases = result;
        return result;
    }

    /** Find relative alias to provided according to parts.
     * Note! Removes found parts from list 
     **/
    private TableAlias getFromQualifiedName(TableAlias parent, List<String> parts)
    {
        TableAlias result = parent;
        while (result != null && parts.size() > 0)
        {
            String part = parts.get(0);

            // 1. Alias match, move on
            if (Objects.equal(part, result.getAlias()))
            {
                parts.remove(0);
                continue;
            }

            // 2. Child alias
            TableAlias alias = result.getChildAlias(part);
            if (alias != null)
            {
                parts.remove(0);
                result = alias;
                continue;
            }

            // 3. Parent alias match upwards
            alias = result.getParent();
            while (alias != null)
            {
                if (Objects.equal(part, alias.getAlias()))
                {
                    break;
                }
                alias = alias.getParent();
            }
            if (alias != null)
            {
                parts.remove(0);
                result = alias;
                continue;
            }

            // No match here, then break, no alias in hierarchy => unknown column
            break;
        }

        return result;
    }
}
