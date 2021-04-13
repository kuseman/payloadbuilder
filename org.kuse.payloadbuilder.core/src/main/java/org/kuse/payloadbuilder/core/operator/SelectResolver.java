package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.Token;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction.LambdaBinding;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.AJoin;
import org.kuse.payloadbuilder.core.parser.ASelectVisitor;
import org.kuse.payloadbuilder.core.parser.AsteriskSelectItem;
import org.kuse.payloadbuilder.core.parser.DereferenceExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionSelectItem;
import org.kuse.payloadbuilder.core.parser.LambdaExpression;
import org.kuse.payloadbuilder.core.parser.LiteralIntegerExpression;
import org.kuse.payloadbuilder.core.parser.NestedSelectItem;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedFunctionCallExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectItem;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SubQueryTableSource;
import org.kuse.payloadbuilder.core.parser.SubscriptExpression;
import org.kuse.payloadbuilder.core.parser.TableFunction;
import org.kuse.payloadbuilder.core.parser.TableSourceJoined;

import gnu.trove.set.hash.TLinkedHashSet;

/**
 * Resolver for a select. - Resolves all expression references to their correct target ordinals. - Aggregates select items. Ie a asterisk parent will
 * be replaced by it's sibling sub queries etc.
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
public class SelectResolver extends ASelectVisitor<Void, SelectResolver.Context>
{
    private static final SelectResolver VISITOR = new SelectResolver();
    private static final ExpressionVisitor EXPRESSION_VISITOR = new ExpressionVisitor();

    private SelectResolver()
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
         * Expression: "aa.unionall(ra.flatMap(x -> x.aa))"
         * Will yield [s/aa, s/ra/aa] as result
         * </pre>
         **/
        private Set<TableAlias> parentAliases = new LinkedHashSet<>();
        /** Columns by alias found */
        private final Map<TableAlias, Set<String>> columnsByAlias = new HashMap<>();
        /** Lambda bindings. Holds which lambda id points to which alias */
        private final Map<Integer, Set<TableAlias>> lambdaAliasById = new HashMap<>();
        /** Aggregated select items by tuple ordinal */
        private final Map<Integer, List<SelectItem>> selectItemsByTupleOrdinal = new HashMap<>();
        /** Gathered/aggregated select item during visit */
        private List<SelectItem> selectItems = new ArrayList<>();
        /** Flag to know when root context */
        private boolean isRootSelect = true;
        /** Map with computed expressions by target tuple ordinal and identifier */
        private final Map<Integer, Map<SelectItem, Expression>> computedExpressionsByTupleOrdinal = new HashMap<>();
        /** Sort by identifiers that might need a computed value pushed down */
        private Set<String> sortByIdentifiers = emptySet();
        /** Sort by ordinals that might need a computed value pushed down */
        private Set<Integer> sortByOrdinals = emptySet();
        /** Current select item ordinal */
        private int selectItemOrdinal;

        /** List used during test to populate resolved qualifiers */
        private List<QualifiedReferenceExpression> resolvedQualifiers;

        Map<Integer, Map<SelectItem, Expression>> getComputedExpressions()
        {
            return unmodifiableMap(computedExpressionsByTupleOrdinal);
        }

        Map<Integer, List<SelectItem>> getSelectItems()
        {
            return unmodifiableMap(selectItemsByTupleOrdinal);
        }

        Map<TableAlias, Set<String>> getColumnsByAlias()
        {
            return unmodifiableMap(columnsByAlias);
        }

        public List<QualifiedReferenceExpression> getResolvedQualifiers()
        {
            return resolvedQualifiers;
        }
    }

    /** Resolve select */
    @SuppressWarnings("deprecation")
    public static Context resolve(Select select)
    {
        Context context = new Context();
        select.accept(VISITOR, context);
        context.columnsByAlias.entrySet().forEach(e -> e.getKey().setColumns(e.getValue().toArray(EMPTY_STRING_ARRAY)));
        return context;
    }

    /** Method used by test to resolve expressions */
    public static Context resolve(Expression expression, TableAlias alias)
    {
        Context context = new Context();
        context.resolvedQualifiers = new ArrayList<>();
        context.parentAliases = asSet(alias);
        EXPRESSION_VISITOR.visit(expression, context);
        return context;
    }

    @Override
    public Void visit(Select select, Context context)
    {
        Set<TableAlias> selectAliases = emptySet();
        TableSourceJoined tsj = select.getFrom();
        if (tsj != null)
        {
            selectAliases = asSet(tsj.getTableSource().getTableAlias());

            int tupleOrdinal = tsj.getTableSource().getTableAlias().getTupleOrdinal();
            // Set select items
            context.selectItemsByTupleOrdinal.put(tupleOrdinal, select.getSelectItems());
            context.parentAliases = selectAliases;

            // Dig down and resolve table source
            tsj.getTableSource().accept(this, context);

            for (AJoin join : tsj.getJoins())
            {
                Set<TableAlias> joinTableAliaes = asSet(join.getTableSource().getTableAlias());
                context.parentAliases = joinTableAliaes;

                // First visit join table source
                join.getTableSource().accept(this, context);

                // Set correct context before processing condition
                context.parentAliases = joinTableAliaes;

                // Then resolve condition
                Expression condition = join.getCondition();
                if (condition != null)
                {
                    condition.accept(EXPRESSION_VISITOR, context);
                }
            }
        }

        // Set correct context before processing where/sort/group/select items
        context.parentAliases = selectAliases;

        if (select.getWhere() != null)
        {
            select.getWhere().accept(EXPRESSION_VISITOR, context);
        }

        if (!select.getGroupBy().isEmpty())
        {
            select.getGroupBy().forEach(e -> e.accept(EXPRESSION_VISITOR, context));
        }

        proccessOrderBy(context, select);

        if (select.getTopExpression() != null)
        {
            select.getTopExpression().accept(EXPRESSION_VISITOR, context);
        }

        context.selectItems = new ArrayList<>();
        // Order by ordinal is 1 based
        int index = 1;
        for (SelectItem item : select.getSelectItems())
        {
            context.selectItemOrdinal = index++;
            item.accept(this, context);
        }

        // Set aggregated select items in context
        if (tsj != null)
        {
            context.selectItemsByTupleOrdinal.put(tsj.getTableSource().getTableAlias().getTupleOrdinal(), context.selectItems);
        }

        return null;
    }

    @Override
    public Void visit(SubQueryTableSource tableSource, Context context)
    {
        boolean isRootSelect = context.isRootSelect;
        context.isRootSelect = false;
        // Set select items for sub query
        context.selectItemsByTupleOrdinal.put(tableSource.getTableAlias().getTupleOrdinal(), tableSource.getSelect().getSelectItems());
        super.visit(tableSource, context);

        // Overwrite the original select items with the generated ones
        context.selectItemsByTupleOrdinal.put(tableSource.getTableAlias().getTupleOrdinal(), context.selectItems);

        context.isRootSelect = isRootSelect;
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        tableFunction.getArguments().forEach(a -> EXPRESSION_VISITOR.visit(a, context));
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Void visit(AsteriskSelectItem selectItem, Context context)
    {
        List<Integer> tupleOrdinals = new ArrayList<>();
        boolean foundAlias = false;
        for (TableAlias alias : context.parentAliases)
        {
            // Traverse all sibling alias
            boolean add = true;
            List<TableAlias> siblinbAliases = alias.getSiblingAliases();
            for (TableAlias siblingAlias : siblinbAliases)
            {
                if (selectItem.getAlias() != null && !equalsIgnoreCase(selectItem.getAlias(), siblingAlias.getAlias()))
                {
                    continue;
                }

                foundAlias = true;
                // If the current top alias is a sub query or the current child alias is
                // then aggregate it's select items
                if (siblingAlias.getType() == TableAlias.Type.SUBQUERY)
                {
                    List<SelectItem> selectItems = context.selectItemsByTupleOrdinal.get(siblingAlias.getChildAliases().get(0).getChildAliases().get(0).getTupleOrdinal());
                    context.selectItems.addAll(selectItems);
                }
                else
                {
                    siblingAlias.setAsteriskColumns();
                    tupleOrdinals.add(siblingAlias.getTupleOrdinal());
                    if (add)
                    {
                        context.selectItems.add(selectItem);
                        add = false;
                    }
                }
            }
        }

        if (selectItem.getAlias() != null && !foundAlias)
        {
            throw new ParseException("No alias found with name: " + selectItem.getAlias(), selectItem.getToken());
        }

        if (!tupleOrdinals.isEmpty())
        {
            selectItem.setAliasTupleOrdinals(tupleOrdinals);
        }

        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Void visit(ExpressionSelectItem selectItem, Context context)
    {
        selectItem.getExpression().accept(EXPRESSION_VISITOR, context);

        /* We need to extract computed expression for a computed operator in these cases
         * - We are in a sub query with a computed expression as select item
         * - There is a sort item referencing a computed column
         * - There is a sort item referencing a computed column by it's ordinal
         */

        boolean sortItemExists = context.sortByIdentifiers.contains(lowerCase(selectItem.getIdentifier()));
        boolean sortOrdinalExists = context.sortByOrdinals.contains(context.selectItemOrdinal);

        if (selectItem.isComputed()
            &&
            (!context.isRootSelect
                || sortItemExists
                || sortOrdinalExists))
        {
            // Computed value, will push pulled up later on so replace this
            // with a reference that points to current alias parent
            /*
             * select *
             * from                             ordinal = 0
             * (
             *      select col1 + col2 calc
             *      from table                  ordinal = 1
             * ) x
             *
             * col1 + col2 should be replaced with a single calc item
             * resolved to target ordinal 1 since that i where a computed operator will be placed later on
             *
             * select *
             * from                             ordinal = 0
             * (
             *      select object(col1 from unionall(a, b)) calc
             *      from tableA a                  ordinal = 1
             *      inner join tableB b
             *        on b....
             * ) x
             *
             */

            for (TableAlias alias : context.parentAliases)
            {
                // If this is not a root select then we are in a sub query
                // and it's parent is the ROOT alias so traverse one step more to the real parent
                int targetTupleOrdinal = alias.getTupleOrdinal();

                // Store the computed select item in context
                Map<SelectItem, Expression> map = context.computedExpressionsByTupleOrdinal
                        .computeIfAbsent(targetTupleOrdinal, k -> new LinkedHashMap<>());
                int index = map.size();
                map.put(selectItem, selectItem.getExpression());

                ResolvePath path = new ResolvePath(-1, targetTupleOrdinal, emptyList(), index);

                QualifiedReferenceExpression qre = new QualifiedReferenceExpression(QualifiedName.of(selectItem.getIdentifier()), -1, selectItem.getToken());
                qre.setResolvePaths(asList(path));

                context.selectItems.add(new ExpressionSelectItem(qre, selectItem.getIdentifier(), null, selectItem.getToken()));
                // Should only be one parent alias here so drop out
                break;
            }
        }
        else
        {
            context.selectItems.add(selectItem);
        }
        return null;
    }

    @Override
    public Void visit(NestedSelectItem nestedSelectItem, Context context)
    {
        Expression from = nestedSelectItem.getFrom();
        if (from != null)
        {
            context.parentAliases = EXPRESSION_VISITOR.visit(from, context);
        }

        List<SelectItem> prevItems = context.selectItems;
        context.selectItems = new ArrayList<>();

        for (SelectItem s : nestedSelectItem.getSelectItems())
        {
            s.accept(this, context);
        }

        context.selectItems = prevItems;

        if (nestedSelectItem.getWhere() != null)
        {
            EXPRESSION_VISITOR.visit(nestedSelectItem.getWhere(), context);
        }

        if (!nestedSelectItem.getGroupBy().isEmpty())
        {
            nestedSelectItem.getGroupBy().forEach(gb -> EXPRESSION_VISITOR.visit(gb, context));
        }

        if (!nestedSelectItem.getOrderBy().isEmpty())
        {
            nestedSelectItem.getOrderBy().forEach(si -> EXPRESSION_VISITOR.visit(si.getExpression(), context));
        }
        context.selectItems.add(nestedSelectItem);
        return null;
    }

    /**
     * Visits order by items. Collects potential order by items that might be present in an expression select item as column or ordinal
     */
    private void proccessOrderBy(Context context, Select select)
    {
        // Gather potential sort by items
        // and resolve the expressions
        Set<String> sortByIdentifiers = emptySet();
        Set<Integer> sortByOrdinals = emptySet();
        if (!select.getOrderBy().isEmpty())
        {
            sortByIdentifiers = new HashSet<>();
            sortByOrdinals = new HashSet<>();

            for (SortItem sortItem : select.getOrderBy())
            {
                Expression expression = sortItem.getExpression();
                if (expression instanceof LiteralIntegerExpression)
                {
                    sortByOrdinals.add(((LiteralIntegerExpression) expression).getValue());
                }
                else if (expression instanceof QualifiedReferenceExpression)
                {
                    QualifiedReferenceExpression qre = (QualifiedReferenceExpression) expression;
                    //CSOFF
                    if (qre.getQname().getParts().size() == 1)
                    //CSON
                    {
                        sortByIdentifiers.add(lowerCase(qre.getQname().getFirst()));
                    }
                }

                expression.accept(EXPRESSION_VISITOR, context);
            }
        }

        context.sortByIdentifiers = sortByIdentifiers;
        context.sortByOrdinals = sortByOrdinals;
    }

    /** Expression visitor. Finds out while table alias all {@link QualifiedReferenceExpression} are referencing */
    private static class ExpressionVisitor extends AExpressionVisitor<Set<TableAlias>, Context>
    {
        /** Visit expression and return resuling table aliases */
        private Set<TableAlias> visit(Expression expression, Context context)
        {
            return expression.accept(this, context);
        }

        @Override
        public Set<TableAlias> visit(SubscriptExpression expression, Context context)
        {
            // A subscripts resulting aliases the the result of the value's aliases
            return expression.getValue().accept(this, context);
        }

        @Override
        public Set<TableAlias> visit(DereferenceExpression expression, Context context)
        {
            // Set left sides aliases in context before visiting right side
            context.parentAliases = expression.getLeft().accept(this, context);
            return expression.getRight().accept(this, context);
        }

        //CSOFF
        @SuppressWarnings("deprecation")
        @Override
        public Set<TableAlias> visit(QualifiedReferenceExpression expression, Context context)
        //CSON
        {
            if (context.resolvedQualifiers != null)
            {
                context.resolvedQualifiers.add(expression);
            }

            Set<TableAlias> tableAliases = context.parentAliases;
            if (!expression.getResolvePaths().isEmpty())
            {
                // Already resolved
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

            int prevTargetOrdinal = -1;

            Set<TableAlias> output = new LinkedHashSet<>();
            for (TableAlias alias : tableAliases)
            {
                List<String> tempParts = new ArrayList<>(parts);
                TableAlias pathAlias = getFromQualifiedName(alias, tempParts);

                int sourceTupleOrdinal = needSourceTupleOrdinal ? alias.getTupleOrdinal() : -1;
                int targetTupleOrdinal = pathAlias.getTupleOrdinal();

                // Multi alias context but we never changed target then we can remove the previous path
                // since they are the same
                // this happens if we are traversing upwards and access a common tuple ordinal
                if (targetTupleOrdinal == prevTargetOrdinal)
                {
                    sourceTupleOrdinal = -1;
                    resolvePaths.remove(resolvePaths.size() - 1);
                }

                prevTargetOrdinal = targetTupleOrdinal;

                /*
                 * No parts left that means we have a tuple access ie. no columns/fields
                 */
                if (tempParts.isEmpty())
                {
                    resolvePaths.add(new ResolvePath(sourceTupleOrdinal, targetTupleOrdinal, tempParts));
                    output.add(pathAlias);
                    continue;
                }

                String column = tempParts.get(0);

                boolean done = false;
                while (pathAlias.getType() == Type.SUBQUERY)
                {
                    if (context.selectItemsByTupleOrdinal.containsKey(pathAlias.getTupleOrdinal()))
                    {
                        // Resolved path is a sub query then verify that column exists as a defined item
                        SelectItem item = getSelectItem(context, pathAlias, column);
                        if (item == null)
                        {
                            throw new ParseException("No column defined with name " + column + " for alias " + pathAlias.getAlias(), expression.getToken());
                        }

                        List<ResolvePath> itemResolvePaths = item.getResolvePaths();
                        if (!CollectionUtils.isEmpty(itemResolvePaths))
                        {
                            // Matching column in sub query then copy it's resolve path
                            resolvePaths.addAll(itemResolvePaths);
                            done = true;
                            break;
                        }
                        else if (item.isComputed())
                        {
                            // Computed item then targetOrdinal is the sub query itself
                            // There will be a ComputedValues operator at this ordinal in the plan
                            // that will be calculated

                            resolvePaths.add(new ResolvePath(sourceTupleOrdinal, targetTupleOrdinal, tempParts));
                            done = true;
                            break;
                        }
                    }

                    // If we came here item is an asterisk select and we should resolve into the sub queries
                    // child selects
                    /*
                     * select *
                     * from
                     * (                    <- We start here
                     *   select *
                     *   from tableA a      <- Set this to the found one continue
                     * ) x
                     *
                     *
                     */
                    pathAlias = pathAlias
                            .getChildAliases()
                            .get(0)                 // Sub queries ROOT
                            .getChildAliases()
                            .get(0);                // First child
                }

                if (done)
                {
                    continue;
                }

                resolvePaths.add(new ResolvePath(sourceTupleOrdinal, pathAlias.getTupleOrdinal(), tempParts));

                // Append column to pathAlias
                Set<String> columns = context.columnsByAlias.computeIfAbsent(pathAlias, key -> new TLinkedHashSet<>());
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

        private SelectItem getSelectItem(Context context, TableAlias alias, String column)
        {
            List<SelectItem> selectItems = context.selectItemsByTupleOrdinal.getOrDefault(alias.getTupleOrdinal(), emptyList());
            for (SelectItem item : selectItems)
            {
                if (item.isAsterisk()
                    ||
                    (!isBlank(item.getIdentifier()) && equalsIgnoreCase(column, item.getIdentifier())))
                {
                    return item;
                }
            }
            return null;
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
                if (equalsIgnoreCase(part, current.getAlias()))
                {
                    result = current;
                    parts.remove(0);
                    continue;
                }

                // 2. Child alias
                // TODO: not valid in join-predicates
                TableAlias alias = current.getChildAlias(part);
                if (alias == null)
                {
                    // 3. Sibling alias
                    alias = current.getSiblingAlias(part);
                }
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
}
