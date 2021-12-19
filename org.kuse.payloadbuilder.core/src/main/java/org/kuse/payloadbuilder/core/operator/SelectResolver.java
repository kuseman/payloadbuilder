package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction;
import org.kuse.payloadbuilder.core.catalog.LambdaFunction.LambdaBinding;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.Column;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.AJoin;
import org.kuse.payloadbuilder.core.parser.ASelectVisitor;
import org.kuse.payloadbuilder.core.parser.AsteriskSelectItem;
import org.kuse.payloadbuilder.core.parser.DereferenceExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionSelectItem;
import org.kuse.payloadbuilder.core.parser.LambdaExpression;
import org.kuse.payloadbuilder.core.parser.LiteralIntegerExpression;
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
import org.kuse.payloadbuilder.core.parser.Table;
import org.kuse.payloadbuilder.core.parser.TableFunction;
import org.kuse.payloadbuilder.core.parser.TableSourceJoined;
import org.kuse.payloadbuilder.core.parser.UnresolvedSubQueryExpression;

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
         * Expression: "aa.unionall(ra.flatMap(x -> x.aa))"
         * Will yield [s/aa, s/ra/aa] as result
         * </pre>
         **/
        private Set<TableAlias> parentAliases = new LinkedHashSet<>();
        /** Lambda bindings. Holds which lambda id points to which alias */
        private final Map<Integer, Set<TableAlias>> lambdaAliasById = new HashMap<>();
        /** Aggregated select items by tuple ordinal */
        private final Map<Integer, List<SelectItem>> selectItemsByTupleOrdinal = new HashMap<>();
        /** Gathered/aggregated select item during visit */
        private List<SelectItem> selectItems = new ArrayList<>();
        /** Flag to know when root context */
        private boolean rootSelect = true;
        /** Map with computed expressions by target tuple ordinal and identifier */
        private final Map<Integer, Map<SelectItem, Expression>> computedExpressionsByTupleOrdinal = new HashMap<>();
        /** Sort by identifiers that might need a computed value pushed down */
        private Set<String> sortByIdentifiers = emptySet();
        /** Sort by ordinals that might need a computed value pushed down */
        private Set<Integer> sortByOrdinals = emptySet();
        /** Current select item ordinal */
        private int selectItemOrdinal;

        /**
         * <pre>
         * Highest ordinal that is valid to reference in current scope.
         * This is to properly detect if a qualified reference is valid or not. For
         * example in a join condition it's invalid to reference an alias defined later on.
         * -1 indicates that all aliases is valid
         * </pre>
         */
        private int highestValidOrdinal = -1;

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

        /** Return resolved qualifiers. NOTE! Used for tests only */
        public List<QualifiedReferenceExpression> getResolvedQualifiers()
        {
            return resolvedQualifiers;
        }
    }

    /** Resolve select */
    public static Context resolve(QuerySession session, Select select)
    {
        Context context = new Context();
        context.session = session;
        select.accept(VISITOR, context);
        return context;
    }

    /** Resolve select. NOTE! Method used for testing only. */
    static Context resolveForTest(QuerySession session, Select select)
    {
        Context context = new Context();
        context.session = session;
        context.resolvedQualifiers = new ArrayList<>();
        select.accept(VISITOR, context);
        return context;
    }

    /** Method used by test to resolve expressions. NOTE! Method used for testing only. */
    public static Context resolveTorTest(Expression expression, TableAlias alias)
    {
        Context context = new Context();
        context.resolvedQualifiers = new ArrayList<>();
        context.parentAliases = asSet(alias);
        EXPRESSION_VISITOR.visit(expression, context);
        return context;
    }

    //CSOFF
    @Override
    //CSON
    public Void visit(Select select, Context context)
    {
        Set<TableAlias> selectAliases = context.parentAliases;
        TableSourceJoined tsj = select.getFrom();
        if (tsj != null)
        {
            int tupleOrdinal = tsj.getTableSource().getTableAlias().getTupleOrdinal();
            // Set select items
            context.selectItemsByTupleOrdinal.put(tupleOrdinal, select.getSelectItems());

            // Dig down and resolve table source
            tsj.getTableSource().accept(this, context);

            // Set the select alias property to the resulting aliases in context for the table source
            selectAliases = context.parentAliases.isEmpty()
                ? singleton(tsj.getTableSource().getTableAlias())
                : context.parentAliases;

            for (AJoin join : tsj.getJoins())
            {
                join.getTableSource().accept(this, context);

                // Set highest valid ordinal for the join condition
                // We cannot reach other than the joins table sources highest child ordinal
                context.highestValidOrdinal = join.getTableSource().getTableAlias().getHighestChildOrdinal();

                Expression condition = join.getCondition();
                if (condition != null)
                {
                    condition.accept(EXPRESSION_VISITOR, context);
                }
            }
        }

        // Set correct context before processing where/sort/group/select items
        context.parentAliases = selectAliases;
        context.highestValidOrdinal = -1;

        if (!context.parentAliases.isEmpty())
        {
            /*
             * The highest ordinal we can reach in a select regarding where/orderby/etc.
             * is the last siblings leafs ordinal
             *
             * select *
             * from tableA a                0
             * inner join tableB b          1
             *  on ...
             * inner join tableC c          2
             *  on ...
             * inner join                   3
             * (
             *    select *
             *    from tableD d             4
             *    inner join tableE e       5
             *      on ....
             * ) x
             * where ....                   <--- highest allowed i 3's highest => 5
             *
             *
             */
            TableAlias alias = context.parentAliases.iterator().next();
            context.highestValidOrdinal = alias.getHighestSiblingLeafOrdinal();
        }

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
            // Set the select aliases before every visit
            // in case of a sub query expression this will be changed
            context.parentAliases = selectAliases;

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
    public Void visit(Table table, Context context)
    {
        context.parentAliases = asSet(table.getTableAlias());
        return super.visit(table, context);
    }

    @Override
    public Void visit(SubQueryTableSource tableSource, Context context)
    {
        Set<TableAlias> parentAliases = context.parentAliases;
        boolean isRootSelect = context.rootSelect;

        context.rootSelect = false;
        context.parentAliases = asSet(tableSource.getTableAlias());

        // Set select items for sub query
        context.selectItemsByTupleOrdinal.put(tableSource.getTableAlias().getTupleOrdinal(), tableSource.getSelect().getSelectItems());
        super.visit(tableSource, context);

        // Overwrite the original select items with the generated ones
        context.selectItemsByTupleOrdinal.put(tableSource.getTableAlias().getTupleOrdinal(), context.selectItems);

        // Restore values before leaving
        context.parentAliases = parentAliases;
        context.rootSelect = isRootSelect;
        return null;
    }

    @Override
    public Void visit(TableFunction tableFunction, Context context)
    {
        // Resolve the table functions argument and calculate the resulting table aliases
        // Note the method sets parent aliases in context
        EXPRESSION_VISITOR.resolveFunction(tableFunction.getTableAlias(), tableFunction.getFunctionInfo(), tableFunction.getToken(), tableFunction.getArguments(), context);
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
        if (context.parentAliases.isEmpty() && selectItem.getExpression() instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression expression = (QualifiedReferenceExpression) selectItem.getExpression();
            throw new ParseException("Unkown reference '" + expression + "'", expression.getToken());
        }

        selectItem.getExpression().accept(EXPRESSION_VISITOR, context);

        /* We need to extract computed expression for a computed operator in these cases
         * - We are in a sub query with a computed expression as select item
         * - There is a sort item referencing a computed column
         * - There is a sort item referencing a computed column by it's ordinal
         */

        boolean sortItemExists = context.sortByIdentifiers.contains(lowerCase(selectItem.getIdentifier()));
        boolean sortOrdinalExists = context.sortByOrdinals.contains(context.selectItemOrdinal);

        if (selectItem.isComputed()
            && (!context.rootSelect
                || sortItemExists
                || sortOrdinalExists))
        {
            // Computed value, will pulled up later on so replace this
            // with a reference that points a simple reference
            /*
             * select *
             * from                             ordinal = 0
             * (
             *      select col1 + col2 calc
             *      from table                  ordinal = 1
             * ) x
             *
             * col1 + col2 should be replaced with a single calc item
             * resolved to target ordinal 1 since that is where a computed operator will be placed later on
             *
             */

            // A select with no FROM just add the expression to context as computed
            if (context.parentAliases.isEmpty())
            {
                // Store the computed select item in context on tuple ordinal 0
                Map<SelectItem, Expression> map = context.computedExpressionsByTupleOrdinal
                        .computeIfAbsent(0, k -> new LinkedHashMap<>());
                map.put(selectItem, selectItem.getExpression());
            }
            else
            {
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
        }
        else
        {
            context.selectItems.add(selectItem);
        }
        return null;
    }

    /**
     * Visits order by items
     *
     * <pre>
     * Collects potential order by items that might be present in an
     * expression select item as column or ordinal
     * </pre>
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

    /** Expression visitor. Finds out which table alias all {@link QualifiedReferenceExpression} are referencing */
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
            // A subscripts resulting aliases is the the result of the value's aliases
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

            QualifiedName qname = expression.getQname();
            List<String> parts = new ArrayList<>(qname.getParts());
            Set<TableAlias> tableAliases = context.parentAliases;
            if (tableAliases.isEmpty())
            {
                expression.setResolvePaths(singletonList(new ResolvePath(-1, -1, parts, -1)));
                return context.parentAliases;
            }

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
                    expression.setResolvePaths(singletonList(new ResolvePath(-1, -1, parts, -1)));
                    return null;
                }

                // Nothing left to process
                if (parts.isEmpty())
                {
                    // This is a lambda access ie. we have an identity lambda of form 'x -> x'
                    // Runtime this means simply return the value we encounter in the lambda
                    expression.setResolvePaths(singletonList(new ResolvePath(-1, -1, emptyList(), -1)));
                    return tableAliases;
                }
            }

            // If we have multiple aliases at this stage
            // this means we have some form of function the concatenates multiple
            // aliases ie. 'unionall(aa, ap).map(x -> x.column .....)' here x.column will point
            // to both aa and ap
            // And then we need to check the sourceTupleOrdinal before we
            // can know which targetTupleOrdinal we should use

            List<ResolvePath> resolvePaths = new ArrayList<>(tableAliases.size());
            boolean needSourceTupleOrdinal = tableAliases.size() > 1;

            int prevTargetOrdinal = -1;

            Set<TableAlias> output = new LinkedHashSet<>();
            for (TableAlias alias : tableAliases)
            {
                List<String> tempParts = new ArrayList<>(parts);
                TableAlias pathAlias = getFromQualifiedName(context, alias, tempParts, expression.getToken(), true);

                int sourceTupleOrdinal = needSourceTupleOrdinal ? alias.getTupleOrdinal() : -1;
                // If we landed on the same alias after traversing then we don't need a target, the target
                // will be the context tuple.
                int targetTupleOrdinal = alias != pathAlias ? pathAlias.getTupleOrdinal() : -1;

                // Multi alias context but we never changed target then we can remove the previous path
                // since they are the same
                // this happens if we are traversing upwards and access a common tuple ordinal
                if (sourceTupleOrdinal >= 0 && targetTupleOrdinal != -1 && targetTupleOrdinal == prevTargetOrdinal)
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
                    resolvePaths.add(new ResolvePath(sourceTupleOrdinal, targetTupleOrdinal, emptyList(), -1));
                    output.add(pathAlias);
                    continue;
                }

                String column = tempParts.get(0);
                MutableObject<TableAlias> pathAliasHolder = new MutableObject<>(pathAlias);
                if (processPathAlias(context, targetTupleOrdinal, column, tempParts, pathAliasHolder, resolvePaths, expression.getToken()))
                {
                    continue;
                }

                pathAlias = pathAliasHolder.getValue();
                int columnOrdinal = -1;
                DataType dataType = DataType.ANY;
                TableMeta tableMeta = pathAlias.getTableMeta();

                if (pathAlias.getType() != TableAlias.Type.TEMPORARY_TABLE)
                {
                    if (tableMeta != null)
                    {
                        TableMeta.Column tableColumn = tableMeta.getColumn(column);
                        //CSOFF
                        if (tableColumn != null)
                        //CSON
                        {
                            dataType = tableColumn.getType();
                            columnOrdinal = tableColumn.getOrdinal();
                            // Strip the first unresolved part from path
                            tempParts = tempParts.subList(1, tempParts.size());
                        }
                        else
                        {
                            throw new ParseException(
                                    "Unknown column: '"
                                        + column
                                        + "' in table source: '"
                                        + (pathAlias.getType() == TableAlias.Type.TEMPORARY_TABLE ? "#" : "")
                                        + pathAlias.getTable()
                                        + (!isBlank(pathAlias.getAlias()) ? (" (" + pathAlias.getAlias() + ")") : "")
                                        + "', expected one of: ["
                                        + tableMeta.getColumns().stream().map(Column::getName).collect(joining(", "))
                                        + "]",
                                    expression.getToken());
                        }
                    }
                }
                resolvePaths.add(new ResolvePath(sourceTupleOrdinal, pathAlias.getTupleOrdinal(), tempParts, columnOrdinal, dataType));
            }

            expression.setResolvePaths(resolvePaths);
            return output;
        }

        /**
         * Processes provided path alias.
         *
         * <pre>
         * If the alias is a sub query we resolve into the sub queries select items to see if there is a match
         * and if so copy it's resolve path.
         * ie.
         *
         * select id                <--- this get the same resolve path ...
         * ,      x.map.key         <--- this will get the same path as a.map below BUT with an extra un-resolved path (key)
         * from
         * (
         *    select a.id           <--- as this
         *    ,      a.map
         *    from table a
         * ) x
         *
         * If the alias is a temp table, the temp tables alias is traversed to find the destination alias
         * ie.
         *
         * select *
         * into #temp
         * from tableA a            ordinal = 0
         * inner join tableB b      ordinal = 1
         *   on b.col = a.col
         *
         * select t.b.col4          <-- will point to col4 in tuple ordinal 1
         * from #temp               ordinal = 2
         * </pre>
         */
        private boolean processPathAlias(
                Context context,
                int inputTupleOrdinal,
                String column,
                List<String> tempParts,
                MutableObject<TableAlias> pathAliasHolder,
                List<ResolvePath> resolvePaths,
                Token token)
        {
            // If we are resolving into a temp table, this is the ordinal in the original select hierarchy
            boolean tempTablePath = false;
            int columnIndex = -1;
            int targetTupleOrdinal = inputTupleOrdinal;

            TableAlias pathAlias = pathAliasHolder.getValue();
            while (true)
            {
                if (pathAlias.getType() == TableAlias.Type.SUBQUERY)
                {
                    if (context.selectItemsByTupleOrdinal.containsKey(pathAlias.getTupleOrdinal()))
                    {
                        // Resolved path is a sub query then verify that column exists as a defined item
                        SelectItem item = getSelectItem(context, pathAlias, column);
                        //CSOFF
                        if (item == null)
                        //CSON
                        {
                            throw new ParseException("No column defined with name " + column + " for alias " + pathAlias.getAlias(), token);
                        }

                        ResolvePath[] itemResolvePaths = item.getResolvePaths();
                        //CSOFF
                        if (!ArrayUtils.isEmpty(itemResolvePaths))
                        //CSON
                        {
                            resolvePaths.addAll(
                                    Arrays.stream(itemResolvePaths)
                                            .map(r ->
                                            {
                                                //CSOFF
                                                // The column is included in tempParts so count without it
                                                if (r.getUnresolvedPath().length < (tempParts.size() - 1))
                                                //CSON
                                                {
                                                    // We have more unresolved parts the the target resolve path
                                                    // alter the resolve path
                                                    return new ResolvePath(r, tempParts.subList(1, tempParts.size()));
                                                }
                                                return r;
                                            })
                                            .collect(toList()));
                            return true;
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
                // Temp table, resolve column ordinal or traverse into the temp table alias
                else if (pathAlias.getType() == TableAlias.Type.TEMPORARY_TABLE)
                {
                    tempTablePath = true;
                    TemporaryTable table = context.session.getTemporaryTable(pathAlias.getTable());
                    int size = tempParts.size();
                    int highestValidOrdinal = context.highestValidOrdinal;
                    context.highestValidOrdinal = -1;
                    // Traverse the temp table
                    TableAlias alias = getFromQualifiedName(context, table.getTableAlias(), tempParts, token, false);
                    context.highestValidOrdinal = highestValidOrdinal;

                    pathAlias = alias;

                    if (alias != table.getTableAlias()
                        || size != tempParts.size())
                    {
                        //CSOFF
                        // If we traversed deeper than the current alias then
                        // set the target tuple ordinal accordingly
                        targetTupleOrdinal = alias.getTupleOrdinal();
                        // No parts left then this means we are accessing a tuple
                        // and not a column, break before checking
                        if (tempParts.size() == 0)
                        //CSON
                        {
                            break;
                        }
                        // Temp table and still parts left, continue digging
                        continue;
                    }

                    String col = tempParts.remove(0);
                    columnIndex = ArrayUtils.indexOf(table.getColumns(), col);
                    if (columnIndex == -1)
                    {
                        throw new ParseException("Unkown column '" + col + "' in temporary table #" + table.getName(), token);
                    }
                }
                break;
            }

            // Temp table hit, add resolve path and return true since we're done
            // resolving
            if (tempTablePath)
            {
                resolvePaths.add(new ResolvePath(
                        -1,
                        targetTupleOrdinal,
                        tempParts,
                        columnIndex));
                return true;
            }

            pathAliasHolder.setValue(pathAlias);
            return false;
        }

        @Override
        public Set<TableAlias> visit(QualifiedFunctionCallExpression expression, Context context)
        {
            return resolveFunction(null, expression.getFunctionInfo(), expression.getToken(), expression.getArguments(), context);
        }

        @Override
        public Set<TableAlias> visit(UnresolvedSubQueryExpression expression, Context context)
        {
            Set<TableAlias> tableAliases = context.parentAliases;
            Select select = expression.getSelectStatement().getSelect();

            List<SelectItem> selectItems = context.selectItems;

            // Skip all select item in sub query expression
            // these should not count as accessible select item from outside
            // the whole expression is a scalar value
            context.selectItems = new ArrayList<>();
            select.accept(VISITOR, context);

            context.selectItems = selectItems;

            return tableAliases;
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
         * Visit function info and resolve arguments Takes special care with lambda functions and binds their identifiers etc.
         *
         * @param alias Table alias for the function. Only applicable when FunctionInfo is a {@link TableFunctionInfo}
         * @param functionInfo The function to resolve
         * @param token Token of the parent node (QualifiedFunctionCall or TableFunction)
         * @param arguments Arguments for the function
         */
        private Set<TableAlias> resolveFunction(TableAlias alias, FunctionInfo functionInfo, Token token, List<Expression> arguments, Context context)
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

            // Store parent aliases before resolving this function call
            Set<TableAlias> parentAliases = context.parentAliases;
            List<Expression> newArgs = new ArrayList<>(arguments);
            // Fill resulting aliases per argument list with
            List<Set<TableAlias>> argumentAliases = new ArrayList<>(Collections.nCopies(arguments.size(), null));
            if (functionInfo instanceof LambdaFunction)
            {
                bindLambdaArguments(context, newArgs, argumentAliases, functionInfo);
            }

            // Visit non visited arguments
            int size = newArgs.size();
            for (int i = 0; i < size; i++)
            {
                Expression arg = newArgs.get(i);
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
            Set<TableAlias> result;

            if (functionInfo instanceof ScalarFunctionInfo)
            {
                result = ((ScalarFunctionInfo) functionInfo).resolveAlias(parentAliases, argumentAliases);
            }
            else
            {
                result = ((TableFunctionInfo) functionInfo).resolveAlias(alias, parentAliases, argumentAliases);
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
                List<Expression> arguments,
                List<Set<TableAlias>> argumentAliases,
                FunctionInfo functionInfo)
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
        private TableAlias getFromQualifiedName(Context context, TableAlias parent, List<String> parts, Token token, boolean throwInvalidTableSource)
        {
            TableAlias result = parent;
            TableAlias current = parent;
            int initSize = parts.size();
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

            if (context.highestValidOrdinal != -1
                && result.getTupleOrdinal() > context.highestValidOrdinal)
            {
                throw new ParseException("Invalid table source reference '" + result.getAlias() + "'", token);
            }
            // We have a multi part identifier that did got any alias match => throw
            if (throwInvalidTableSource
                && !isBlank(result.getAlias())
                && initSize == parts.size()
                && initSize > 1)
            {
                throw new ParseException("Invalid table source reference '" + parts.get(0) + "'", token);
            }

            return result;
        }
    }
}
