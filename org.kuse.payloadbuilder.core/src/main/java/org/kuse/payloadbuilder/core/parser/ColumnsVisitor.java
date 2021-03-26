package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ObjectUtils;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction.LambdaBinding;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

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
        return getColumnsByAlias(session, map, singleton(alias), expression);
    }

    /**
     * Extracts columns per alias for provided expression Reusing a map to populate result
     **/
    public static Set<TableAlias> getColumnsByAlias(
            QuerySession session,
            Map<TableAlias, Set<String>> map,
            Set<TableAlias> aliases,
            Expression expression)
    {
        Context ctx = new Context();
        ctx.session = session;
        // Start with input alias
        ctx.parentAliases.addAll(aliases);
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
        Set<TableAlias> tableAliases = context.parentAliases;
        // Reference already resolved, skip
        if (expression.isInitiallyResolved())
        {
            return tableAliases;
        }

        QualifiedName qname = expression.getQname();
        List<String> parts = new ArrayList<>(qname.getParts());

        // Lambda reference => resolve table alias
        if (expression.getLambdaId() >= 0)
        {
            tableAliases = context.lambdaAliasById.get(expression.getLambdaId());

            // Remove first part since it's resolved to an alias via lambda identifier
            parts.remove(0);

            // No alias connected to this lambda
            // which means we can only try to resolve the lambda value with parts
            // at runtime
            if (tableAliases == null)
            {
                expression.setResolvePaths(singletonList(new ResolvePath(-1, -1, parts)));
                return null;
            }

            // Nothing left to process
            if (parts.isEmpty())
            {
                // This is a lambda access ie. we have an identity lambda of form 'x -> x'
                // Runtime this means simply return the value we encounter in the lambda
                expression.setResolvePaths(singletonList(new ResolvePath(-1, -1, emptyList())));
                return tableAliases;
            }
        }

        // If we have multiple aliases at this stage
        // this means we have some form of function the concatenates multiple
        // aliases ie. 'unionall(aa, ap)'
        // And then we need to check the sourceTupleOrdinal before we
        // can know which targetTupleOrdinal we should use

        List<ResolvePath> resolvePaths = new ArrayList<>(tableAliases.size());
        boolean needSourceTupleOrdinal = tableAliases.size() > 1;
        Set<TableAlias> output = new THashSet<>();
        for (TableAlias alias : tableAliases)
        {
            List<String> tempParts = new ArrayList<>(parts);
            TableAlias pathAlias = getFromQualifiedName(alias, tempParts);

            int sourceTupleOrdinal = needSourceTupleOrdinal ? alias.getTupleOrdinal() : -1;
            int targetTupleOrdinal = pathAlias.getTupleOrdinal();

            resolvePaths.add(new ResolvePath(sourceTupleOrdinal, targetTupleOrdinal, tempParts));
            /*
             * No parts left that means we have a tuple access ie. no columns/fields
             */
            if (tempParts.isEmpty())
            {
                output.add(pathAlias);
                continue;
            }

            // Push sub query columns into first child
            if (pathAlias.getType() == Type.SUBQUERY && pathAlias.getChildAliases().size() > 0)
            {
                pathAlias = pathAlias.getChildAliases().get(0);
            }

            Set<String> columns = context.columnsByAlias.computeIfAbsent(pathAlias, key -> new THashSet<>());
            String column = tempParts.get(0);
            columns.add(column);
        }

        expression.setResolvePaths(resolvePaths);

        return output;
    }

    @Override
    public Set<TableAlias> visit(QualifiedFunctionCallExpression expression, Context context)
    {
        /*
         *  Lambda function
         *
         *  Parent aliases (ROOT)
         *
         *  map(aa, x -> x.id)
         *      Lambda binding aa -> x
         *      Alias result -> arg 1
         *
         *  1.visit aa
         *    Result alias => aa
         *    bind x to aa
         *  2.visit x -> x.id
         *    Result alias => ROOT
         *  3.Alias result = arg 1 => [ROOT]
         *
         *  Multi alias function
         *
         *  Parent aliases (ROOT)
         *
         *  concat(aa, ap)
         *      Alias result => all args
         *
         *  1.visit aa
         *    Result alias => aa
         *  2.visit ap
         *    Result alias => ap
         *  3.Alias result => [aa, ap]
         *
         */

        ScalarFunctionInfo functionInfo = expression.getFunctionInfo();

        // Store parent aliases before resolving this function call
        Set<TableAlias> parentAliases = context.parentAliases;
        List<Expression> arguments = new ArrayList<>(expression.getArguments());
        // Fill resulting aliases per argument list with
        List<Set<TableAlias>> argumentAliases = new ArrayList<>(Collections.nCopies(arguments.size(), null));
        if (functionInfo instanceof LambdaFunction)
        {
            bindLambdaArguments(context, expression.getToken(), arguments, argumentAliases, functionInfo);
        }

        // Visit non visited arguments
        int size = arguments.size();
        for (int i = 0; i < size; i++)
        {
            Expression arg = arguments.get(i);
            if (arg == null)
            {
                // Lambda argument already processed
                continue;
            }
            // Restore parent aliases before every argument process
            context.parentAliases = parentAliases;
            argumentAliases.set(i, ObjectUtils.defaultIfNull(arg.accept(this, context), emptySet()));
        }

        // Resolve alias from function
        Set<TableAlias> result = functionInfo.resolveAlias(parentAliases, argumentAliases);
        if (isEmpty(result))
        {
            result = parentAliases;
        }

        context.parentAliases = result;
        return result;
    }

    /**
     * <pre>
     * Bind lambda arguments
     * Connect the calculated aliases to the lambda identifier ids
     *
     * ie. map(aa, x -> x.id)
     *
     * Calculate the result alias of 'aa' and connect 'x'-s unique lamda id
     * to it. This to be able to property resolve the qualifier 'x.id' later
     * </pre>
     */
    private void bindLambdaArguments(
            Context context,
            Token token,
            List<Expression> arguments,
            List<Set<TableAlias>> argumentAliases,
            ScalarFunctionInfo functionInfo)
    {
        List<LambdaBinding> lambdaBindings = ((LambdaFunction) functionInfo).getLambdaBindings();
        for (LambdaBinding binding : lambdaBindings)
        {
            // Clear the lambda target argument so it's not processed again
            // further down, cannot remove since we need to keep the argument order in place
            Expression lambdaExpression = arguments.get(binding.getLambdaArg());
            Expression targetExpression = arguments.get(binding.getToArg());
            arguments.set(binding.getToArg(), null);
            // Resolve aliases from target
            Set<TableAlias> lambdaAliases = targetExpression.accept(this, context);
            argumentAliases.set(binding.getToArg(), lambdaAliases);
            if (isEmpty(lambdaAliases))
            {
                continue;
            }

            if (!(lambdaExpression instanceof LambdaExpression))
            {
                throw new ParseException("Expected a lambda expression at argument index: " + binding.getLambdaArg(), token);
            }

            LambdaExpression le = (LambdaExpression) lambdaExpression;

            for (int id : le.getLambdaIds())
            {
                context.lambdaAliasById.put(id, lambdaAliases);
            }
        }
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
            // TODO: not valid in join-predicates
            TableAlias alias = current.getChildAlias(part);
            if (alias != null)
            {
                parts.remove(0);
                result = alias;
                current = alias;
                continue;
            }

            // 3. Sibling alias
            alias = current.getSiblingAlias(part);
            if (alias != null)
            {
                parts.remove(0);
                result = alias;
                current = alias;
                continue;
            }

            // 4. Parent alias match upwards
            current = current.getParent();
        }

        return result;
    }
}
