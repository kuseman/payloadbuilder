/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.operator;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.LambdaExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedFunctionCallExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

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
class ColumnsVisitor extends AExpressionVisitor<Set<TableAlias>, ColumnsVisitor.Context>
{
    private static final ColumnsVisitor VISITOR = new ColumnsVisitor();

    private ColumnsVisitor()
    {
    }

    /** Context used during visitor */
    static class Context
    {
        QuerySession session;
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
    public static Set<TableAlias> getColumnsByAlias(
            QuerySession session,
            Map<TableAlias, Set<String>> map,
            TableAlias alias,
            Expression expression)
    {
        Context ctx = new Context();
        ctx.session = session;
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

            // Push sub query columns into first child
            if (tempAlias.getType() == Type.SUBQUERY && tempAlias.getChildAliases().size() > 0)
            {
                tempAlias = tempAlias.getChildAliases().get(0);
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
        /*  map(flatmap(aa, x -> x.ap), x -> x.price_sales)
             Lambda binding of map = arg0 => arg1
             Visit
        */

        ScalarFunctionInfo functionInfo = expression.getFunctionInfo(context.session);

        // Store parent aliases before resolving this function call
        Set<TableAlias> parentAliases = context.parentAliases;
        // Bind lambda parameters
        List<Expression> arguments = new ArrayList<>(expression.getArguments());
        if (functionInfo instanceof LambdaFunction)
        {
            List<Pair<Expression, LambdaExpression>> lambdaBindings = ((LambdaFunction) functionInfo).getLambdaBindings(expression.getArguments());
            for (Pair<Expression, LambdaExpression> pair : lambdaBindings)
            {
                arguments.remove(pair.getLeft());
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

        // Visit non visited arguments
        arguments.forEach(a -> a.accept(this, context));

        // Resolve alias from function
        Set<TableAlias> result = functionInfo.resolveAlias(parentAliases, expression.getArguments(), e -> e.accept(this, context));
        if (isEmpty(result))
        {
            result = parentAliases;
        }

        context.parentAliases = result;
        return result;
    }

    /**
     * Find relative alias to provided according to parts. Note! Removes found parts from list
     **/
    private TableAlias getFromQualifiedName(TableAlias parent, List<String> parts)
    {
        TableAlias result = parent;
        TableAlias current = parent;

        while (current != null && parts.size() > 0)
        {
            String part = parts.get(0);

            // 1. Alias match, move on
            if (Objects.equals(part, current.getAlias()))
            {
                result = current;
                parts.remove(0);
                continue;
            }

            // 2. Child alias
            TableAlias alias = current.getChildAlias(part);
            if (alias != null)
            {
                parts.remove(0);
                result = alias;
                current = alias;
                continue;
            }

            // 3. Parent alias match upwards
            current = current.getParent();
        }

        return result;
    }
}
