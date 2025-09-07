package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.Strings.CI;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.SchemaResolveException;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Rule to optimize columns and eliminate sub queries.
 * 
 * <pre>
 * This rule binds column expressions to the input, taking care of
 * asterisk table sources etc. Correrlated sub queries and so on.
 * 
 * </pre>
 */
class ColumnResolver extends ALogicalPlanOptimizer<ColumnResolver.Ctx>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnResolver.class);

    ColumnResolver()
    {
        // We handle all expression rewriting when handling the logical plans
        super(null);
    }

    /** Visitor context */
    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        final ColumnResolver columnResvoler;

        /** Schema used when resolving expressions in {@link ColumnResolverVisitor} */
        ResolveSchema schema;

        /** Concated outer schema used by joins/sub query expressions to detect outer column references in {@link ColumnResolverVisitor} */
        ResolveSchema outerSchema;

        /** Outer references found during traversal. Used to detect correlated sub queries. Added in {@link ColumnResolverVisitor} */
        Set<Column> outerReferences;

        /** Stack with the last visited plans resulting schema */
        Deque<ResolveSchema> planSchema = new ArrayDeque<>();

        /** Map with type for each lambda id */
        Int2ObjectMap<ResolvedType> lambdaTypes = new Int2ObjectOpenHashMap<>();

        /** Flag to indicate when we are resolving inside a sub query */
        boolean insideSubQuery = false;

        /** Stack subQueryTableSource. */
        Deque<TableSourceReference> subQueryTableSource = new ArrayDeque<>();

        /**
         * Set with expanded aggregated asterisks. This is used to to skip resolving those since an
         * expanded asterisk should NOTE be a single value to match a schema less query which would make
         * those columns arrays.
         *
         * <pre>
         * Ie.
         *
         * select *
         * from data d
         * group by d.id
         *
         * => expands to
         *
         * select d.id, d.col1, d.col2      <-- d.id will be marked as single value but that would be inconsistent
         *                                           with a schema less query where all columns will be an array value
         * from data d
         * group by d.id
         *
         *
         * </pre>
         */
        Set<IExpression> expandedAsterisks = new HashSet<>();

        /**
         * Set with column references used when resolving aggregate projection to detect if an expression should be single value or grouped
         */
        Map<TableSourceReference, Set<ColumnReferenceExtractorResult>> aggregateColumnReferences;

        /**
         * Holder of the schema per table source. This is used to lookup each column reference to link back to the concrete table. Used later on when gathering projected columns for each table.
         */
        Int2ObjectMap<Schema> tableSourceSchemaById = new Int2ObjectOpenHashMap<>();

        Ctx(IExecutionContext context, ColumnResolver columnResolver)
        {
            super(context);
            this.columnResvoler = columnResolver;
        }

        /**
         * Return the schema for provided plan, either the top of the plan schema stack or the input plan if column resolver doesn't process the logical plan type
         */
        ResolveSchema getSchema(ILogicalPlan plan)
        {
            if (planSchema.size() > 0)
            {
                return planSchema.pop();
            }

            return new ResolveSchema(plan.getSchema());
        }

        /** Find {@link TableSourceReference} of type {@link TableSourceReference.Type#TABLE} from provided table source and column. */
        TableSourceReference getTableTypeTableSource(TableSourceReference tableSource, String column)
        {
            // Provided table source is of Type = TABLE already => return
            if (tableSource.getType() == TableSourceReference.Type.TABLE)
            {
                return tableSource;
            }

            Schema schema = tableSourceSchemaById.get(tableSource.getId());
            if (schema == null)
            {
                return null;
            }

            // Asterisk column on schema => bind
            if (schema.getSize() == 1
                    && SchemaUtils.isAsterisk(schema.getColumns()
                            .get(0)))
            {
                return SchemaUtils.getTableSource(schema.getColumns()
                        .get(0));
            }

            // Else search for column match
            for (Column col : schema.getColumns())
            {
                if (Strings.CI.equals(column, col.getName()))
                {
                    return SchemaUtils.getTableSource(col);
                }
            }

            return null;
        }
    }

    @Override
    protected Ctx createContext(IExecutionContext context)
    {
        return new Ctx(context, this);
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        return plan.accept(this, (Ctx) context);
    }

    @Override
    public ILogicalPlan visit(Projection plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Rewrite projections according to input plan schema
        ResolveSchema schema = context.getSchema(input);
        context.schema = schema;

        // Expand all static asterisks, they should not be left for execution
        List<IExpression> expressions = expandAsterisks(context, plan.getExpressions(), context.outerReferences, context.outerSchema, schema, false);
        expressions = ColumnResolverVisitor.rewrite(context, expressions);

        // Mark projection expressions that lack a table source reference with the closest sub query
        // reference, this is needed when projection is wrapped inside a subquery then we need to maintain
        // the sub query table source since that is what resolving has resolved against but at runtime
        // the subquey operator is removed and hence we need to retain it's table source when expanding asteriskts etc.
        TableSourceReference parentTableSourceReference = context.subQueryTableSource.peek();
        ILogicalPlan result = new Projection(input, expressions, parentTableSourceReference);
        context.planSchema.push(new ResolveSchema(result.getSchema()));
        return result;
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Rewrite predicate according to input plan schema
        ResolveSchema schema = context.getSchema(input);
        context.schema = schema;
        IExpression predicate = ColumnResolverVisitor.rewrite(context, plan.getPredicate());

        ILogicalPlan result = new Filter(input, plan.getTableSource(), predicate);

        context.planSchema.push(schema);
        return result;
    }

    @Override
    public ILogicalPlan visit(Sort plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);
        ResolveSchema schema = context.getSchema(input);

        if (input instanceof Sort)
        {
            // TODO: in populated join then this is valid
            throw new ParseException("Sort is invalid in sub queries", Location.EMPTY);
        }

        // Rewrite sort items expressions
        context.schema = schema;
        List<SortItem> sortItems = plan.getSortItems()
                .stream()
                .map(si -> new SortItem(ColumnResolverVisitor.rewrite(context, si.getExpression()), si.getOrder(), si.getNullOrder(), si.getLocation()))
                .collect(toList());

        ILogicalPlan result = new Sort(input, sortItems);
        context.planSchema.push(schema);
        return result;
    }

    @Override
    public ILogicalPlan visit(Join plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan outer = plan.getOuter()
                .accept(this, context);

        // Collect outer table sources, these are used to know if this
        // join is directly referencing any outer columns => we need to loop
        // row by row, otherwise we can do more effective join algorithms
        Set<TableSourceReference> outerTableSources = extractTableSources(outer);

        // CSOFF
        // Concat outer and left schema to detect outer references
        ResolveSchema prevOuterSchema = context.outerSchema;
        // Reset the outer reference list before visiting left to distinctly
        // know what references this join has
        Set<Column> prevOuterReferences = context.outerReferences;
        // CSON

        ResolveSchema outerPlanSchema = context.getSchema(outer);

        context.outerReferences = new HashSet<>();
        // Outer reference are only supported in outer/cross apply's so append the outer schema
        if (plan.canHaveOuterReferences())
        {
            context.outerSchema = ResolveSchema.concat(context.outerSchema, outerPlanSchema);
        }

        // CSOFF
        Schema outerSchema = context.outerSchema != null ? context.outerSchema.getSchema()
                : Schema.EMPTY;
        // CSON

        ILogicalPlan inner = plan.getInner()
                .accept(this, context);

        // Outer references to this join is all collected outer references that references any of
        // the outer plans table sources or columns that doesn't have a table source (computed columns)
        // that is present in the outer plan
        context.outerReferences.removeIf(or ->
        {
            Set<TableSourceReference> tableSources = SchemaUtils.getTableSources(or);
            if (tableSources.isEmpty())
            {
                // TODO: Find out when this happens
                return false;
            }
            boolean result = !CollectionUtils.containsAny(outerTableSources, tableSources);

            // Add the removed outer references to the prev list to bubble up
            if (result
                    && prevOuterReferences != null)
            {
                prevOuterReferences.add(or);
            }

            return result;
        });

        // If this join can have outer references but didn't have any, recreate the inner plan without
        // this joins outer context (but instead use the previous outer context which is perfectly fine to reference).
        // This means that the query was for example an outer apply
        // but in fact did not use any outer columns, recreate the input without outer schema to
        // get correct column ordinals etc.
        if (context.outerReferences.isEmpty()
                && plan.canHaveOuterReferences())
        {
            LOGGER.debug("Re-visting join since there was no outer references collected");

            context.outerSchema = prevOuterSchema;
            inner = plan.getInner()
                    .accept(this, context);
            context.outerReferences.clear();
        }

        ResolveSchema innerPlanSchema = context.getSchema(inner);

        // Construct a resulting join schema
        ResolveSchema resultSchema = new ResolveSchema();
        resultSchema.schemas.addAll(outerPlanSchema.schemas);
        if (plan.getPopulateAlias() != null)
        {
            Schema populatedSchema = Schema.of(SchemaUtils.getPopulatedColumn(plan.getPopulateAlias(), innerPlanSchema.getSchema()));
            resultSchema.add(new AliasSchema(populatedSchema, plan.getPopulateAlias()), null);
        }
        else
        {
            resultSchema.addAll(innerPlanSchema.schemas, null);
        }

        IExpression condition = plan.getCondition();

        // Rewrite condition if present. Cross/outer joins doesn't have conditions
        if (condition != null)
        {
            context.schema = ResolveSchema.concat(outerPlanSchema, innerPlanSchema);
            condition = ColumnResolverVisitor.rewrite(context, condition);
        }

        // CSOFF
        Join result = new Join(outer, inner, plan.getType(), plan.getPopulateAlias(), condition, context.outerReferences, plan.isSwitchedInputs(), outerSchema);
        // CSON

        // Restore context values
        context.outerSchema = prevOuterSchema;
        context.outerReferences = prevOuterReferences;

        context.planSchema.push(resultSchema);

        return result;
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        ResolveSchema schema = context.getSchema(input);
        // Resolve expressions against input schema
        context.schema = schema;

        List<IExpression> aggregateExpressions = ColumnResolverVisitor.rewrite(context, plan.getAggregateExpressions());
        context.aggregateColumnReferences = collectColumns(aggregateExpressions);

        List<IAggregateExpression> expandAsterisks = expandAsterisks(context, plan.getProjectionExpressions(), context.outerReferences, context.outerSchema, schema, true);
        List<IAggregateExpression> projectionExpressions = new ArrayList<>();
        for (IExpression exp : expandAsterisks)
        {
            IExpression resolvedExpression = ColumnResolverVisitor.rewrite(context, exp);
            projectionExpressions.add((IAggregateExpression) resolvedExpression);
        }

        context.expandedAsterisks.clear();

        TableSourceReference parentTableSourceReference = context.subQueryTableSource.peek();
        ILogicalPlan result = new Aggregate(input, aggregateExpressions, projectionExpressions, parentTableSourceReference);
        context.planSchema.push(new ResolveSchema(result.getSchema()));
        return result;
    }

    @Override
    public ILogicalPlan visit(SubQuery plan, Ctx context)
    {
        // CSOFF
        boolean prevInsideSubQuery = context.insideSubQuery;
        // CSON
        context.insideSubQuery = true;

        context.subQueryTableSource.push(plan.getTableSource());

        // Eliminate sub query, no sub queries should be left
        ILogicalPlan result = plan.getInput()
                .accept(this, context);

        context.subQueryTableSource.pop();

        ResolveSchema schema = context.getSchema(result);

        context.tableSourceSchemaById.put(plan.getTableSource()
                .getId(), schema.getSchema());

        // Validate sub query schema, all columns must have a specified alias
        Set<String> columnNames = new HashSet<>();
        int size = schema.getSize();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumn(i)
                    .getValue();
            boolean isAsterisk = SchemaUtils.isAsterisk(column);
            if ("".equals(column.getName())
                    && !isAsterisk)
            {
                throw new ParseException("Missing column name for ordinal " + i + " of " + plan.getAlias(), plan.getLocation());
            }

            // Multiple columns with the same name
            if (!isAsterisk
                    && !columnNames.add(column.getName()))
            {
                throw new ParseException("The column '" + column.getName() + "' was specified multiple times for '" + plan.getAlias() + "'", plan.getLocation());
            }
        }
        context.planSchema.push(new ResolveSchema(schema, plan.getTableSource()));
        context.insideSubQuery = prevInsideSubQuery;

        return new SubQuery(result, plan.getTableSource(), plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(Concatenation plan, Ctx context)
    {
        ILogicalPlan result = super.visit(plan, context);

        context.planSchema.push(new ResolveSchema(result.getSchema()));

        return result;
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, Ctx context)
    {
        // CSOFF
        ResolveSchema prevSchema = context.schema;
        context.schema = null;
        // Table function only have outer schema to search values from
        List<IExpression> expressions = ColumnResolverVisitor.rewrite(context, plan.getArguments());
        // CSON

        ResolveSchema prevOuterSchema = context.outerSchema;
        context.outerSchema = null;

        context.schema = prevSchema;
        context.outerSchema = prevOuterSchema;

        TableFunctionInfo function = context.getSession()
                .resolveTableFunctionInfo(plan.getTableSource()
                        .getCatalogAlias(),
                        plan.getTableSource()
                                .getName()
                                .getFirst())
                .getValue();

        // Resolve the table function schema from it's resolved arguments
        Schema schema = Schema.EMPTY;
        TableSourceReference tableSource = plan.getTableSource();
        try
        {
            schema = function.getSchema(expressions, plan.getOptions());
        }
        catch (SchemaResolveException e)
        {
            throw new ParseException(e.getMessage(), plan.getLocation());
        }

        Schema originalSchema = schema;

        // Empty schema => create an asterisk column with the table source
        if (Schema.EMPTY.equals(schema))
        {
            originalSchema = schema = Schema.of(new CoreColumn(plan.getAlias(), ResolvedType.of(Type.Any), "", false, tableSource, CoreColumn.Type.ASTERISK));
        }
        else
        {
            // .. force all columns to have a proper table source reference
            schema = SchemaResolver.recreate(tableSource, schema);
        }

        // Store the original schema before we change the table source
        context.tableSourceSchemaById.put(plan.getTableSource()
                .getId(), originalSchema);

        context.planSchema.push(new ResolveSchema(schema, plan.getAlias()));

        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .toList();

        return new TableFunctionScan(tableSource, schema, expressions, options, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(TableScan plan, Ctx context)
    {
        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .toList();

        context.planSchema.push(new ResolveSchema(plan.getSchema(), plan.getAlias()));
        context.tableSourceSchemaById.put(plan.getTableSource()
                .getId(), plan.getSchema());

        return new TableScan(plan.getTableSchema(), plan.getTableSource(), plan.getProjection(), plan.isTempTable(), options, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(ConstantScan plan, Ctx context)
    {
        // Empty schema or zero row => nothing to resolve
        if (Schema.EMPTY.equals(plan.getSchema())
                || plan.getRowsExpressions()
                        .isEmpty())
        {
            context.planSchema.push(new ResolveSchema(plan.getSchema()));
            return plan;
        }

        // Constant scans doesn't have any input so clear the schema while resolving
        ResolveSchema prevSchema = context.schema;
        context.schema = null;

        List<List<IExpression>> rowsExpressions = plan.getRowsExpressions()
                .stream()
                .map(list -> ColumnResolverVisitor.rewrite(context, list))
                .toList();

        context.schema = prevSchema;

        ILogicalPlan result = plan.reCreate(rowsExpressions);

        context.planSchema.push(new ResolveSchema(result.getSchema()));

        return result;
    }

    @Override
    public ILogicalPlan visit(ExpressionScan plan, Ctx context)
    {
        // CSOFF
        ResolveSchema prevSchema = context.schema;
        context.schema = null;
        // Expression scans only have outer schema to search values from
        IExpression expression = ColumnResolverVisitor.rewrite(context, plan.getExpression());
        // CSON

        ResolvedType type = expression.getType();
        // Parse error, expression has invalid type
        if (!(type.getType() == Type.Any
                || type.getType() == Type.Table))
        {
            throw new ParseException("Expression scans must reference either " + Type.Any + " or " + Type.Table + " types.", plan.getLocation());
        }

        TableSourceReference tableSource = plan.getTableSource();
        Schema schema = type.getSchema();
        if (schema == null)
        {
            // If this is a table variable (@table) see if there is an empty vector with the schema
            // in context and use that schema if present
            // TODO: add compiler info message if missing
            if (expression instanceof VariableExpression ve)
            {
                ValueVector value = ((ExecutionContext) context.context).getVariableValue(ve.getName());
                Object obj = value != null ? value.valueAsObject(0)
                        : null;

                if (obj != null
                        && !(obj instanceof TupleVector))
                {
                    throw new ParseException("Table variable expression must be of type: " + Type.Table + " but got: " + value.type(), plan.getLocation());
                }
                else if (obj instanceof TupleVector tv)
                {
                    schema = tv.getSchema();
                }
            }

            if (schema == null)
            {
                schema = Schema.EMPTY;
            }
        }

        Schema originalSchema = schema;

        // Try to find the source table source reference and link that to this expression scans table source
        TableSourceReference sourceTableSourceReference = SchemaUtils.getTableSource(originalSchema);
        if (sourceTableSourceReference != null)
        {
            tableSource = tableSource.withParent(sourceTableSourceReference);
        }

        // Create an asterisk column with the table source ref if no schema was provided.
        if (Schema.EMPTY.equals(schema))
        {
            originalSchema = schema = Schema.of(new CoreColumn(plan.getAlias(), ResolvedType.of(Type.Any), "", false, tableSource, CoreColumn.Type.ASTERISK));
        }
        else
        {
            // .. else force all columns to have a proper column reference
            schema = SchemaResolver.recreate(tableSource, schema);
        }

        // Store the original schema before we change table source
        context.tableSourceSchemaById.put(plan.getTableSource()
                .getId(), originalSchema);

        context.schema = prevSchema;

        context.planSchema.push(new ResolveSchema(schema, tableSource));

        return new ExpressionScan(tableSource, schema, expression, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(OperatorFunctionScan plan, Ctx context)
    {
        Pair<String, OperatorFunctionInfo> pair = context.getSession()
                .resolveOperatorFunctionInfo(plan.getCatalogAlias(), plan.getFunction());
        if (pair == null)
        {
            throw new ParseException("No operator function found named: " + plan.getFunction(), plan.getLocation());
        }
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        Column column = plan.getSchema()
                .getColumns()
                .get(0);

        boolean isInternal = SchemaUtils.isInternal(column);

        // Recreate the operator schema
        Schema schema = Schema.of(new CoreColumn(column.getName(), pair.getValue()
                .getType(input.getSchema()), "", isInternal, null, CoreColumn.Type.REGULAR));

        context.planSchema.push(new ResolveSchema(schema));
        return new OperatorFunctionScan(schema, input, plan.getCatalogAlias(), plan.getFunction(), plan.getLocation());
    }

    @SuppressWarnings("unchecked")
    private <T extends IExpression> List<T> expandAsterisks(Ctx context, List<T> expressions, Set<Column> outerReferences, ResolveSchema outerSchema, ResolveSchema inputSchema, boolean aggregate)
    {
        List<T> result = new ArrayList<>();
        for (T expression : expressions)
        {
            T orig = expression;
            if (expression instanceof AggregateWrapperExpression awe)
            {
                expression = (T) awe.getExpression();
            }
            if (!(expression instanceof AsteriskExpression))
            {
                result.add(orig);
                continue;
            }
            AsteriskExpression ae = (AsteriskExpression) expression;
            QualifiedName qname = ae.getQname();
            ResolveSchema schema = null;
            boolean qualified = qname.size() > 0;

            String alias = "";
            // Full input
            if (!qualified)
            {
                schema = inputSchema;
            }
            else if (qname.size() == 1)
            {
                alias = qname.getFirst()
                        .toLowerCase();

                Pair<Integer, ResolveSchema> pair = inputSchema.getResolveSchema(alias);
                if (pair != null)
                {
                    schema = pair.getRight();
                }
                // Try outer
                if (pair == null
                        && outerSchema != null)
                {
                    pair = outerSchema.getResolveSchema(alias);
                    if (pair != null)
                    {
                        schema = pair.getRight();
                    }
                }

                if (schema == null)
                {
                    throw new ParseException("Alias " + alias + " could not be bound", ae.getLocation());
                }
            }
            // Expand nested value like map, tuple vector etc.
            // NOTE! This isn't supported in PLB grammar so unreachable but better safe than sorry
            else if (qname.size() > 1)
            {
                throw new ParseException("Asterisk for nested values is unsupported.", ae.getLocation());
            }

            // Asterisk schemas are expanded runtime
            if (SchemaUtils.isAsterisk(schema.getSchema()))
            {
                result.add(orig);
                continue;
            }

            int size = schema.getSize();
            for (int i = 0; i < size; i++)
            {
                Pair<String, Column> pair = schema.getColumn(i);
                // Internal columns should not be expanded
                if (SchemaUtils.isInternal(pair.getValue()))
                {
                    continue;
                }
                boolean populated = SchemaUtils.isPopulated(pair.getValue());

                if (!qualified)
                {
                    alias = pair.getKey();
                }
                // Project each populated column. Note! This is only done if we target the column explicitly (qualified asterisk)
                if (qualified
                        && populated)
                {
                    for (Column col : pair.getValue()
                            .getType()
                            .getSchema()
                            .getColumns())
                    {
                        // Internal columns should not be expanded
                        if (SchemaUtils.isInternal(col))
                        {
                            continue;
                        }
                        IExpression e = new UnresolvedColumnExpression(QualifiedName.of(alias, col.getName()), -1, ae.getLocation());
                        if (aggregate)
                        {
                            e = new AggregateWrapperExpression(e, false, false);
                            context.expandedAsterisks.add(e);
                        }
                        result.add((T) e);
                    }
                }
                else
                {
                    QualifiedName newColumn;
                    if (populated)
                    {
                        newColumn = QualifiedName.of(alias);
                    }
                    else if (StringUtils.isBlank(alias))
                    {
                        newColumn = QualifiedName.of(pair.getValue()
                                .getName());
                    }
                    else
                    {
                        newColumn = QualifiedName.of(alias, pair.getValue()
                                .getName());
                    }
                    IExpression e = new UnresolvedColumnExpression(newColumn, -1, ae.getLocation());
                    if (aggregate)
                    {
                        e = new AggregateWrapperExpression(e, false, false);
                        context.expandedAsterisks.add(e);
                    }
                    result.add((T) e);
                }
            }
        }
        return result;
    }

    static class ColumnResolverVisitor extends ARewriteExpressionVisitor<ColumnResolver.Ctx>
    {
        private static final ColumnResolverVisitor INSTANCE = new ColumnResolverVisitor();

        /** Rewrite provided expression according to provided schema */
        static IExpression rewrite(ColumnResolver.Ctx context, IExpression expression)
        {
            return expression.accept(INSTANCE, context);
        }

        /** Rewrite provided expressions according to provided schema */
        static List<IExpression> rewrite(ColumnResolver.Ctx context, List<IExpression> expressions)
        {
            ResolveSchema schema = context.schema;
            ResolveSchema outerSchema = context.outerSchema;

            List<IExpression> result = new ArrayList<>(expressions.size());
            for (IExpression expression : expressions)
            {
                result.add(expression.accept(INSTANCE, context));

                // Restore context between resolves since there could be sub query expressions that sets these
                context.schema = schema;
                context.outerSchema = outerSchema;
            }
            return result;
        }

        @Override
        public IExpression visit(AggregateWrapperExpression expression, Ctx context)
        {
            boolean isExpandedAsterisk = context.expandedAsterisks.contains(expression);

            IExpression result = expression.getExpression()
                    .accept(this, context);

            /*
             * If this is a wrapper around an asterisk expression then simply return the input. This will be expanded into column expressions later on and the singleValue property on the wrapper
             * should be not be altered with because it's used when creating the expanded expressions
             */
            if (isExpandedAsterisk
                    || result instanceof AsteriskExpression)
            {
                return new AggregateWrapperExpression(result, expression.isSingleValue(), expression.isInternal());
            }

            Map<TableSourceReference, Set<ColumnReferenceExtractorResult>> columns = ColumnResolver.collectColumns(singletonList(result));
            boolean singleValue;

            if (!columns.isEmpty())
            {
                singleValue = true;
                for (Entry<TableSourceReference, Set<ColumnReferenceExtractorResult>> e : columns.entrySet())
                {
                    Set<ColumnReferenceExtractorResult> aggregateColumns = context.aggregateColumnReferences.get(e.getKey());

                    if (aggregateColumns == null
                            || !CollectionUtils.containsAll(aggregateColumns, e.getValue()))
                    {
                        singleValue = false;
                        break;
                    }
                }
            }
            else
            {
                singleValue = true;
            }

            return new AggregateWrapperExpression(result, singleValue, expression.isInternal());
        }

        @Override
        public IExpression visit(IDereferenceExpression expression, Ctx context)
        {
            return DereferenceExpression.create(expression.getExpression()
                    .accept(this, context), QualifiedName.of(expression.getRight()), null);
        }

        @Override
        public IExpression visit(IFunctionCallExpression expression, Ctx context)
        {
            if (expression.getFunctionInfo() instanceof LambdaFunction)
            {
                int size = expression.getArguments()
                        .size();
                List<IExpression> arguments = new ArrayList<>(Collections.nCopies(size, null));

                LambdaFunction lf = (LambdaFunction) expression.getFunctionInfo();

                // For each lambda binding, resolve the schema for each lambda index, will
                // be used later on when resolving column references
                for (LambdaFunction.LambdaBinding lb : lf.getLambdaBindings())
                {
                    int lambdaArg = lb.getLambdaArg();
                    int toArg = lb.getToArg();

                    IExpression toArgExpression = expression.getArguments()
                            .get(toArg)
                            .accept(this, context);

                    // Set the resolved toArg expression to result
                    arguments.set(toArg, toArgExpression);

                    // See if the resolved type is known, else we will have an empty schema
                    // for the lambda alias and all references will be Dereferecnes that evals
                    // runtime.
                    ResolvedType toArgType = toArgExpression.getType();

                    LambdaExpression le = (LambdaExpression) expression.getArguments()
                            .get(lambdaArg);

                    // Set lambda types for all lambda ids
                    for (int lambdaId : le.getLambdaIds())
                    {
                        context.lambdaTypes.put(lambdaId, toArgType);
                    }

                    // Resolve the lambda's expression
                    arguments.set(lambdaArg, new LambdaExpression(le.getIdentifiers(), le.getExpression()
                            .accept(this, context), le.getLambdaIds()));

                    // Clean up
                    for (int lambdaId : le.getLambdaIds())
                    {
                        context.lambdaTypes.remove(lambdaId);
                    }
                }

                // Resolve all the rest of the functions arguments that is left after we resolved the lambda expressions
                for (int i = 0; i < size; i++)
                {
                    if (arguments.get(i) == null)
                    {
                        IExpression arg = expression.getArguments()
                                .get(i);
                        arguments.set(i, arg.accept(this, context));
                    }
                }

                return new FunctionCallExpression(expression.getCatalogAlias(), expression.getFunctionInfo(), expression.getAggregateMode(), arguments);
            }

            return super.visit(expression, context);
        }

        @Override
        public IExpression visit(LambdaExpression expression, Ctx context)
        {
            throw new IllegalArgumentException("Lambda expressions should not be visited");
        }

        @Override
        public IExpression visit(AsteriskExpression expression, Ctx context)
        {
            String alias = expression.getQname()
                    .size() > 0 ? expression.getQname()
                            .getFirst()
                            : null;

            // Asterisks are resolved from either current schema or outer, not both
            ResolveSchema resolveSchema = context.schema;
            boolean outer = false;
            if (resolveSchema == null
                    || resolveSchema.isEmpty())
            {
                resolveSchema = context.outerSchema;
                outer = true;
            }

            // Linked set because we need to add the table refs in the order they come
            // so expansion later on gets columns in correct order
            Set<TableSourceReference> tableSourceReferences = new LinkedHashSet<>();
            if (resolveSchema != null)
            {
                if (alias != null)
                {
                    Pair<Integer, ResolveSchema> pair = resolveSchema.getResolveSchema(alias);
                    assert (pair != null) : "Pair should not be null here sice the same check is done earlier when trying to expand asterisks";
                    resolveSchema = pair.getValue();
                }

                // Add outer reference to context
                if (outer)
                {
                    Schema schema = resolveSchema.getSchema();
                    if (resolveSchema.isPopulated())
                    {
                        schema = resolveSchema.getColumn(0)
                                .getValue()
                                .getType()
                                .getSchema();
                    }
                    context.outerReferences
                            .add(new CoreColumn(alias, ResolvedType.table(schema), "", false, SchemaUtils.getTableSource(schema), SchemaUtils.isAsterisk(schema) ? CoreColumn.Type.ASTERISK
                                    : CoreColumn.Type.REGULAR));
                }
                int size = resolveSchema.getSize();
                for (int i = 0; i < size; i++)
                {
                    Column column = resolveSchema.getColumn(i)
                            .getRight();
                    boolean isInternal = SchemaUtils.isInternal(column);
                    // Internal column's table sources should not be included
                    if (isInternal)
                    {
                        continue;
                    }
                    TableSourceReference tableSource = SchemaUtils.getTableSource(column);
                    if (tableSource != null)
                    {
                        tableSourceReferences.add(tableSource);
                    }
                }
            }

            return new AsteriskExpression(expression.getQname(), expression.getLocation(), tableSourceReferences);
        }

        @Override
        public IExpression visit(UnresolvedFunctionCallExpression expression, Ctx context)
        {
            throw new IllegalArgumentException("UnresolvedFunctionCallExpression should not be present at this stage");
        }

        @Override
        public IExpression visit(UnresolvedSubQueryExpression expression, ColumnResolver.Ctx context)
        {
            throw new IllegalArgumentException("UnresolvedSubQueryExpression should not be present at this stage");
        }

        /**
         * Resolve unbound column expression.
         * 
         * <pre>
         * Binds the column to input. Binding has this priority
         *  - Find a suitable schema to search in
         *    - Alias mapping for each table source
         *    - Outer schema if any (this also detects outer references)
         *    - If qualified name has no alias then we search in both current schema and outer schema
         * </pre>
         */
        @Override
        public IExpression visit(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            if (expression.getLambdaId() >= 0)
            {
                return resolveLambda(expression, context);
            }
            IExpression result = resolveColumn(expression, context);
            return result;
        }

        private IExpression resolveLambda(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            QualifiedName path = expression.getColumn();
            String alias = path.getFirst();
            path = path.extract(1);

            int lambdaId = expression.getLambdaId();

            ResolvedType lambdaType = context.lambdaTypes.get(lambdaId);

            Column.Type type = lambdaType.getType();

            // Applying lambda to a table yields an object with the same schema
            if (type == Column.Type.Table)
            {
                return DereferenceExpression.create(ColumnExpression.Builder.of(alias, ResolvedType.object(lambdaType.getSchema()))
                        .withLambdaId(lambdaId)
                        .build(), path, expression.getLocation());
            }
            // Applying a lambda to an array then we will get the sub type
            else if (type == Column.Type.Array)
            {
                return DereferenceExpression.create(ColumnExpression.Builder.of(alias, lambdaType.getSubType())
                        .withLambdaId(lambdaId)
                        .build(), path, expression.getLocation());
            }

            // All other types is resolved to that type
            return DereferenceExpression.create(ColumnExpression.Builder.of(alias, lambdaType)
                    .withLambdaId(lambdaId)
                    .build(), path, expression.getLocation());
        }

        /**
         * <pre>
         * Column resolving.
         * -----------------
         * High level precedence:
         * - 1. Inner scope. The closest schema scope to expression. Ie. ExpressionScan/From/Join etc.
         * - 2. In case of ambiguity regular/populated column wins over asterisk column
         * - 3. Outer scope. The outer schema if any. Sub query expressions
         * - 4. In case of ambiguity regular/populated column wins over asterisk column
         * </pre>
         */
        private IExpression resolveColumn(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            // CSOFF
            ResolveSchema schema = context.schema;
            ResolveSchema outerSchema = context.outerSchema;
            // CSON

            QualifiedName path = expression.getColumn();
            String alias = Objects.toString(path.getAlias(), "")
                    .toLowerCase();
            path = "".equals(alias) ? path
                    : path.extract(1);

            String column = path.getFirst();
            path = path.extract(1);

            ResolveResult resolveResult = null;
            if (schema != null)
            {
                resolveResult = resolve(context, expression.getColumn(), schema, alias, column, null, expression.getLocation());
            }

            boolean aliasMatch = resolveResult != null
                    && resolveResult.aliasMatch;

            // Search in outer scope if we didn't get any result from current scope
            // or we did get a non alias match
            // NOTE! Schema is the same as the outer schema when resolving a projection with subexpressions
            if (outerSchema != null
                    && schema != outerSchema
                    && !aliasMatch)
            {
                ResolveResult scopeResult = resolveResult;
                Set<Column> outerReferences = new HashSet<>();
                ResolveResult outerResult = resolve(context, expression.getColumn(), outerSchema, alias, column, outerReferences, expression.getLocation());
                resolveResult = getHighestPrecedence(scopeResult, outerResult);

                // If we picked the outer scoped result, add the the resulting column to the outer references collection
                if (outerResult != null
                        && resolveResult == outerResult)
                {
                    if (scopeResult != null)
                    {
                        LOGGER.debug("Picked outer reference for: {}, expression: {}", expression.getQualifiedColumn(), resolveResult.expression());
                    }
                    if (context.outerReferences != null)
                    {
                        context.outerReferences.addAll(outerReferences);
                    }
                }
            }

            if (resolveResult == null)
            {
                /*
                 * @formatter:off
                 * If we have a qualifier with multiple parts we treat the first part as alias
                 * and if that don't match we will get no result, BUT if we have a single schema in context
                 * then we can assume that the first part of the qualifier is NOT an alias but rather a column.
                 *
                 * ie.
                 *
                 * select ( select multi.part.qualifier )   <--- 'multi' will be treated as an alias but will get no match
                 * from tableA a                            <--- only schema we have so assume 'multi' is a column
                 *
                 * @formatter:on
                 */
                if (!context.insideSubQuery)
                {
                    path = expression.getColumn();
                    column = path.getFirst();
                    path = path.extract(1);

                    // If we only have a single schema to choose from and no hit's
                    // bind the expression to that schema
                    if (schema != null
                            && (outerSchema == null
                                    || outerSchema.isEmpty())
                            && schema.schemas.size() == 1)
                    {
                        resolveResult = resolve(context, expression.getColumn(), schema, schema.schemas.get(0).alias, column, null, expression.getLocation());
                    }
                }

                if (resolveResult == null)
                {
                    // No expression => cannot be bound
                    throw new ParseException(expression.getColumn() + " cannot be bound", expression.getLocation());
                }

                return DereferenceExpression.create(resolveResult.expression, path, expression.getLocation());
            }

            IExpression result = DereferenceExpression.create(resolveResult.expression, path, expression.getLocation());
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Resolved {} to: {}:{}", expression.getColumn(), result.getClass()
                        .getSimpleName(), result.toVerboseString());
            }
            return result;
        }

        /** Returns the expression that has the highest precedence between a current match and outer schema match */
        private ResolveResult getHighestPrecedence(ResolveResult current, ResolveResult outer)
        {
            if (outer == null)
            {
                return current;
            }
            else if (current == null)
            {
                return outer;
            }

            // Score the candidates according to alias/populated/asterisk
            //@formatter:off
            int currentScore =
                      (current.aliasMatch ? 1: 0)
                    + (current.columnMatch ? 1: 0)
                    + (current.populatedMatch ? 1 : 0)
                    + (current.asteriskMatch ? -1 : 0);
            int outerScore =
                      (outer.aliasMatch ? 1 : 0)
                    + (outer.columnMatch ? 1 : 0)
                    + (outer.populatedMatch ? 1 : 0)
                    + (outer.asteriskMatch ? -1 : 0);
            //@formatter:on

            if (outerScore > currentScore)
            {
                return outer;
            }

            // Fallback to current if no distinct "winner" can be found
            return current;
        }

        /** Result of a resolve of a QFN against a schema. */
        private record ResolveResult(IExpression expression, boolean aliasMatch, boolean columnMatch, boolean asteriskMatch, boolean populatedMatch)
        {
        }

        /**
         * @formatter:off
         * Resolve a split qualified name me against a schema.
         * Alias is the first part of the QFN if there are more than one parts otherwise is empty
         *
         * Matching aliases
         * ----------------
         *
         * alias => the alias of the qualified name (will be empty if QFN is a single item)
         * - a.col => alias 'a'
         * - t => no alias ie. ''
         * --
         * column => the second part of the QFN if items > 1 otherwise the QFN part
         * - a.col => 'col'
         * - t => 't'
         * --
         * schemaAlias => The alias that the column belongs to
         * --
         * -- select a.col
         * -- from tableA a
         * --
         * -- => all columns originating from tableA will have schemaAlias 'a'
         * --
         * -- select a.col, b.col
         * -- from tableA a
         * -- inner join tableB b
         * --   on ..
         * --
         * -- => all columns originating from tableA will have schemaAlias 'a'
         * -- => all columns originating from tableB will have schemaAlias 'b'
         * --
         * -- select x.col
         * -- from (
         * --   select *
         * --   from tableA a
         * --   inner join tableB b
         * --     on..
         * -- ) x
         * -- => All columns originating from subquery x have schemaAlias 'x'
         *
         * Precedence
         * ----------
         *
         * - 1. Alias match with column match
         * ----- Full match.
         * - 2. Empty alias with column match
         * ----- Semi match. If multiple => ambiguity
         * - 3. Empty alias with schema alias matching on populated column
         * ----- Full match (populated)
         * - 4. Alias match on asterisk column
         * ----- Full Asterisk match.
         * - 5. Empty alias matching asterisk schema column
         * ----- Semi match. If multiple => ambiguity
         *
         * Ordinals can only be used if the schema doesn't contain any asterisk columns
         * @formatter:on
         */
        private ResolveResult resolve(ColumnResolver.Ctx context, QualifiedName originalQname, ResolveSchema schema, String alias, String column, Set<Column> outerReferences, Location location)
        {
            int asteriskIndexMatch = -1;
            int nonAsteriskIndexMatch = -1;
            boolean multipleAsterisk = false;
            boolean canUseOrdinal = true;

            boolean emptyAlias = "".equals(alias);

            int size = schema.getSize();
            for (int i = 0; i < size; i++)
            {
                Pair<String, Column> p = schema.getColumn(i);

                Column schemaColumn = p.getValue();
                // We only look for direct asterisk columns here
                boolean isAsterisk = SchemaUtils.getColumnType(schemaColumn) == CoreColumn.Type.ASTERISK;
                String schemaAlias = p.getKey();
                // If the schema doesn't have an alias check to see if the column has a TableSourceReference
                // attached and if so use the as schemaAlias
                if ("".equals(schemaAlias))
                {
                    TableSourceReference tsr = SchemaUtils.getTableSource(schemaColumn);
                    schemaAlias = tsr != null ? tsr.getAlias()
                            : "";
                }
                boolean emptySchemaAlias = "".equals(schemaAlias);

                boolean aliasMatch = !emptyAlias
                        && !emptySchemaAlias
                        && alias.equalsIgnoreCase(schemaAlias);
                // Ordinals can be used if there are no asterisk in the schema
                canUseOrdinal = canUseOrdinal
                        && !isAsterisk;

                // Drop out on mismatching alias combo
                if (!emptyAlias
                        && !emptySchemaAlias
                        && !alias.equalsIgnoreCase(schemaAlias))
                {
                    continue;
                }

                /*
                 * @formatter:off
                 * A populated match can be on column or alias
                 *
                 * select x       <--- column match
                 * from tableA A
                 * inner populate join tableB x
                 *   on ....
                 *
                 * select x.col       <--- alias match (this also needs a Dereference since we want col on x)
                 * from tableA A
                 * inner populate join tableB x
                 *   on ....
                 *
                 * @formatter:on
                 */
                boolean populatedMatch = SchemaUtils.isPopulated(schemaColumn)
                        && ((emptyAlias
                                && column.equalsIgnoreCase(schemaColumn.getName()))
                                || (alias.equalsIgnoreCase(schemaColumn.getName())));

                // Bullet 3
                // A full populated match.
                // We can break out here since it's now allowed to have multiple aliases with the same name
                // so there can only be one match
                if (populatedMatch)
                {
                    nonAsteriskIndexMatch = i;
                    break;
                }
                // Bullet 1,2
                else if (!isAsterisk)
                {
                    boolean columnMatch = column.equalsIgnoreCase(schemaColumn.getName());
                    // A full non asterisk match => no need to look any more
                    if ((aliasMatch
                            && columnMatch))
                    {
                        nonAsteriskIndexMatch = i;
                        // We can break here when a full match occurs
                        break;
                    }
                    // A semi match
                    else if (emptyAlias
                            && columnMatch)
                    {
                        // Multiple matches of named columns is not allowed
                        if (nonAsteriskIndexMatch >= 0)
                        {
                            throw new ParseException("Ambiguous column: " + originalQname, location);
                        }
                        nonAsteriskIndexMatch = i;
                    }
                }
                // Bullet 4,5 in precedence chart
                else
                {
                    // Full or semi alias match
                    if (aliasMatch
                            || emptyAlias)
                    {
                        // We cannot throw until after loop since a non asterisk column has higher prio
                        // and then it's ok to have multiple asterisk matches
                        multipleAsterisk = asteriskIndexMatch >= 0;
                        asteriskIndexMatch = i;
                    }
                }
            }

            if (nonAsteriskIndexMatch == -1
                    && multipleAsterisk)
            {
                throw new ParseException("Ambiguous column: " + originalQname, location);
            }
            else if (nonAsteriskIndexMatch == -1
                    && asteriskIndexMatch == -1)
            {
                return null;
            }

            final int index = nonAsteriskIndexMatch >= 0 ? nonAsteriskIndexMatch
                    : asteriskIndexMatch;

            Pair<String, Column> pair = schema.getColumn(index);
            Column match = pair.getRight();
            TableSourceReference tableRef = SchemaUtils.getTableSource(match);
            boolean populatedMatch = SchemaUtils.isPopulated(match);
            boolean populatedAliasMatch = populatedMatch
                    && alias.equalsIgnoreCase(match.getName());
            // CSOFF
            boolean asteriskMatch = SchemaUtils.isAsterisk(match);
            boolean aliasMatch = !emptyAlias
                    && (alias.equalsIgnoreCase(pair.getKey())
                            || (tableRef != null
                                    && alias.equalsIgnoreCase(tableRef.getAlias())));
            boolean columnMatch = column.equalsIgnoreCase(match.getName());

            // CSON
            int ordinal = canUseOrdinal ? index
                    : -1;

            // Set the alias of the ColumnExpression to the alias and not the column
            // in case we matched the alias and not the column
            ColumnExpression.Builder builder = ColumnExpression.Builder.of(populatedAliasMatch ? alias
                    : column, match.getType())
                    .withOrdinal(ordinal);

            if (ordinal < 0)
            {
                builder.withColumn(populatedAliasMatch ? alias
                        : column);
            }

            // If column did not have a table source use the one from schema (sub query)
            if (tableRef == null)
            {
                tableRef = schema.getTableSource(index);
            }

            if (tableRef != null)
            {
                CoreColumn.Type columnType = SchemaUtils.getColumnType(match);
                // Switch asterisk to regular upon match
                if (columnType == CoreColumn.Type.ASTERISK)
                {
                    columnType = CoreColumn.Type.REGULAR;
                }
                match = new CoreColumn(column, match.getType(), "", false, tableRef, columnType);

                TableSourceReference tableTypeTableSource = null;
                // Only lookup real columns
                if (columnType != CoreColumn.Type.POPULATED)
                {
                    tableTypeTableSource = context.getTableTypeTableSource(tableRef, column);
                    // Clear the found table source if it's not of desired type or the same as the original
                    if (tableTypeTableSource != null
                            && (tableTypeTableSource.getType() != TableSourceReference.Type.TABLE
                                    || tableTypeTableSource.getId() == tableRef.getId()))
                    {
                        tableTypeTableSource = null;
                    }
                }
                builder.withColumnReference(new ColumnReference(tableRef, columnType, tableTypeTableSource));
            }

            // Add column to outer references and set outer reference to column builder
            if (outerReferences != null)
            {
                builder.withOuterReference(true);
                outerReferences.add(match);
            }
            // If we had a match on the alias we need to wrap the column expression with a dereference of the column
            IExpression expression = builder.build();
            if (populatedAliasMatch)
            {
                expression = DereferenceExpression.create(builder.build(), QualifiedName.of(column), location);
            }

            return new ResolveResult(expression, aliasMatch, columnMatch, asteriskMatch, populatedMatch);
        }
    }

    /** Wrapper around a list of schemas used during resolving */
    static class ResolveSchema
    {
        final List<AliasSchema> schemas = new ArrayList<>();

        ResolveSchema()
        {
        }

        /** Construct a resolve schema from a single regular schema */
        ResolveSchema(Schema schema)
        {
            this(schema, "");
        }

        /** Construct a resolve schema from a single regular schema */
        ResolveSchema(Schema schema, String alias)
        {
            schemas.add(new AliasSchema(schema, alias));
        }

        /** Construct a resolve schema from a single regular schema */
        ResolveSchema(Schema schema, TableSourceReference tableSource)
        {
            schemas.add(new AliasSchema(schema, tableSource));
        }

        /** Copy a resolve schema but change it's alias */
        ResolveSchema(ResolveSchema schema, TableSourceReference tableSource)
        {
            // Concat all the actual schemas into one alias schema
            Schema result = null;

            for (AliasSchema s : schema.schemas)
            {
                if (result == null)
                {
                    result = s.schema;
                }
                else
                {
                    result = SchemaUtils.joinSchema(result, s.schema);
                }
            }

            schemas.add(new AliasSchema(result, tableSource));
        }

        boolean isEmpty()
        {
            return schemas.isEmpty()
                    || (schemas.size() == 1
                            && schemas.get(0).schema.getSize() == 0);
        }

        /** Create a regular schema from this instance */
        Schema getSchema()
        {
            List<Column> columns = new ArrayList<>(getSize());

            for (AliasSchema s : schemas)
            {
                for (Column column : s.schema.getColumns())
                {
                    columns.add(column);
                }
            }

            return new Schema(columns);
        }

        /** Get sub schema with provided alias */
        Pair<Integer, ResolveSchema> getResolveSchema(String alias)
        {
            int index = 0;
            for (AliasSchema s : schemas)
            {
                if (alias.equalsIgnoreCase(s.alias))
                {
                    return Pair.of(index, new ResolveSchema(s.schema));
                }
                index += s.schema.getSize();
            }
            return null;
        }

        int getSize()
        {
            int size = 0;
            for (AliasSchema s : schemas)
            {
                size += s.schema.getSize();
            }
            return size;
        }

        Pair<String, Column> getColumn(int index)
        {
            int count = 0;
            for (AliasSchema s : schemas)
            {
                int size = s.schema.getSize();
                for (int i = 0; i < size; i++)
                {
                    if (count == index)
                    {
                        Column column = s.schema.getColumns()
                                .get(i);
                        return Pair.of(s.alias, column);
                    }
                    count++;
                }
            }
            return null;
        }

        TableSourceReference getTableSource(int index)
        {
            int count = 0;
            for (AliasSchema s : schemas)
            {
                int size = s.schema.getSize();
                for (int i = 0; i < size; i++)
                {
                    if (count == index)
                    {
                        return s.tableSource;
                    }
                    count++;
                }
            }
            return null;
        }

        boolean isPopulated()
        {
            return getSize() == 1
                    && SchemaUtils.isPopulated(getColumn(0).getValue());
        }

        /** Append alias schema */
        void add(AliasSchema aliasSchema, Location location)
        {
            if (!"".equalsIgnoreCase(aliasSchema.alias)
                    && schemas.stream()
                            .anyMatch(s -> CI.equals(s.alias, aliasSchema.alias)))
            {
                throw new ParseException("Alias '" + aliasSchema.alias + "' is specified multiple times.", location);
            }
            schemas.add(aliasSchema);
        }

        void addAll(List<AliasSchema> schemas, Location location)
        {
            for (AliasSchema schema : schemas)
            {
                add(schema, location);
            }
        }

        static ResolveSchema concat(ResolveSchema schema1, ResolveSchema schema2)
        {
            if (schema1 == null)
            {
                return schema2;
            }

            ResolveSchema result = new ResolveSchema();
            result.schemas.addAll(schema1.schemas);
            result.schemas.addAll(schema2.schemas);
            return result;
        }
    }

    /** Wrapper around a schema with an attached alias to property resolve qualifiers */
    static class AliasSchema
    {
        final Schema schema;
        final String alias;
        TableSourceReference tableSource;

        AliasSchema(Schema schema, TableSourceReference tableSource)
        {
            this(schema, tableSource.getAlias());
            this.tableSource = tableSource;
        }

        AliasSchema(Schema schema, String alias)
        {
            this.schema = schema;
            this.alias = alias;
        }
    }
}
