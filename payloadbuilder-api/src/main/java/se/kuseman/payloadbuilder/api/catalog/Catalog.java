package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;

/**
 * Catalog. Defines the hooking points for retrieving data, functions etc.
 */
// CSOFF
public abstract class Catalog
// CSON
{
    public static final String SYSTEM_CATALOG_ALIAS = "sys";

    protected static final String SYS_TABLES = "tables";
    protected static final String SYS_TABLES_NAME = "name";

    protected static final String SYS_COLUMNS = "columns";
    protected static final String SYS_COLUMNS_NAME = "name";
    protected static final String SYS_COLUMNS_TABLE = "table";

    protected static final String SYS_FUNCTIONS = "functions";
    protected static final String SYS_FUNCTIONS_NAME = "name";
    protected static final String SYS_FUNCTIONS_DESCRIPTION = "description";
    protected static final String SYS_FUNCTIONS_TYPE = "type";

    protected static final String SYS_INDICES = "indices";
    protected static final String SYS_INDICES_TABLE = "table";
    protected static final String SYS_INDICES_COLUMNS = "columns";

    protected static final Schema SYS_FUNCTIONS_SCHEMA = Schema.of(Column.of(SYS_FUNCTIONS_NAME, Type.String), Column.of(SYS_FUNCTIONS_TYPE, Type.String),
            Column.of(SYS_FUNCTIONS_DESCRIPTION, Type.String));

    /** Name of the catalog */
    private final String name;
    /** Scalar functions. This also includes aggregate functions */
    private final Map<String, ScalarFunctionInfo> scalarFunctionByName = new HashMap<>();
    /** Table functions */
    private final Map<String, TableFunctionInfo> tableFunctionByName = new HashMap<>();
    /** Operator functions */
    private final Map<String, OperatorFunctionInfo> operatorFunctionByName = new HashMap<>();

    public Catalog(String name)
    {
        this.name = requireNonNull(name, "name");
    }

    public String getName()
    {
        return name;
    }

    /**
     * Return the table schema information for provided table.
     *
     * @deprecated Use {@link #getTableSchema(IExecutionContext, String, QualifiedName, List)} instead
     */
    @Deprecated
    public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table)
    {
        return TableSchema.EMPTY;
    }

    /**
     * Return the table schema information for provided table.
     */
    public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        return getTableSchema(context, catalogAlias, table);
    }

    /** Create a scan {@link IDatasource} for provided table */
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support scan operators.");
    }

    /** Create a seek {@link IDatasource} for provided predicate */
    public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support seek operators.");
    }

    /** Return an insert into sink for provided table. */
    public IDatasink getInsertIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, InsertIntoData data)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support insert into operator.");
    }

    /**
     * Return a select into sink for provided table. This operator assumes that the table does not already exists and should throw error if exists.
     */
    public IDatasink getSelectIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, SelectIntoData data)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support select into operator.");
    }

    /**
     * Drop provided table.
     *
     * @param qname Name of table
     * @param lenient False if this operation should fail if table does not exist otherwise false.
     */
    public void dropTable(IQuerySession session, String catalogAlias, QualifiedName qname, boolean lenient)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support drop table operator.");
    }

    /**
     * <pre>
     * Get system datasource for provided table.
     * This method should return a system operator for various system tables like:
     *  - tables
     *     - Return tables in catalog
     *     - Preferable to return at least one column 'name'
     *  - columns
     *     - Return columns in catalog
     *     - Preferable to return at least two columns 'table', 'name'
     *  - indices
     *     - Return indices in catalog
     *     - Preferable to return at least two columns 'table', 'columns'
     *  - functions
     *     - Return functions in catalog
     *     - Preferable to return at least two columns 'name', 'description'
     *
     * NOTE! It's optional to implement this method, but it's a good way to expose
     *       things that the catalog supports
     * NOTE! It's perfectly fine to support other system tables than listed above
     * NOTE! The provided query session is not the session used when executing the query it's provided from {@link IDatasource}
     * </pre>
     */
    public IDatasource getSystemTableDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support system operator for " + table);
    }

    /** Return table schema for a system table */
    public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        throw new IllegalArgumentException("Catalog " + name + " doesn't support system operator for: " + table);
    }

    /**
     * Return summarized execution statistics for provided context. This is executed after an ANALZYE query and lets catalogs aggregate data for all it's datasources
     */
    public Map<String, Object> getExecutionStatistics(IExecutionContext context)
    {
        return emptyMap();
    }

    /** Return registered functions for this catalog */
    public Collection<FunctionInfo> getFunctions()
    {
        List<FunctionInfo> functions = new ArrayList<>();
        functions.addAll(scalarFunctionByName.values());
        functions.addAll(tableFunctionByName.values());
        functions.addAll(operatorFunctionByName.values());
        return functions;
    }

    /** Register function */
    protected void registerFunction(FunctionInfo functionInfo)
    {
        requireNonNull(functionInfo);

        String name = functionInfo.getName()
                .toLowerCase();

        switch (functionInfo.getFunctionType())
        {
            case SCALAR:
            case AGGREGATE:
            case SCALAR_AGGREGATE:
                if (scalarFunctionByName.put(name, (ScalarFunctionInfo) functionInfo) != null)
                {
                    throw new IllegalArgumentException("A function named " + functionInfo.getName() + " was already registered.");
                }
                break;
            case OPERATOR:
                if (operatorFunctionByName.put(name, (OperatorFunctionInfo) functionInfo) != null)
                {
                    throw new IllegalArgumentException("A function named " + functionInfo.getName() + " was already registered.");
                }
                break;
            case TABLE:
                if (tableFunctionByName.put(name, (TableFunctionInfo) functionInfo) != null)
                {
                    throw new IllegalArgumentException("A function named " + functionInfo.getName() + " was already registered.");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported function type: " + functionInfo.getFunctionType());
        }
    }

    /** Get scalar function info by name */
    public ScalarFunctionInfo getScalarFunction(String name)
    {
        return scalarFunctionByName.get(requireNonNull(name).toLowerCase());
    }

    /** Get table function info by name */
    public TableFunctionInfo getTableFunction(String name)
    {
        return tableFunctionByName.get(requireNonNull(name).toLowerCase());
    }

    /** Get operator function info by name */
    public OperatorFunctionInfo getOperatorFunction(String name)
    {
        return operatorFunctionByName.get(requireNonNull(name).toLowerCase());
    }

    /**
     * Return a functions tuple vector that can be used for {@link Catalog#getSystemTableDataSource(IQuerySession, String, QualifiedName, DatasourceData)} when functions is requested.
     */
    protected TupleVector getFunctionsTupleVector(Schema schema)
    {
        if (schema.getSize() != SYS_FUNCTIONS_SCHEMA.getSize())
        {
            throw new IllegalArgumentException("Should should have the same column count as Catalog#SYS_FUNCTIONS_SCHEMA");
        }
        List<FunctionInfo> functions = new ArrayList<>(getFunctions());
        return new ObjectTupleVector(schema, functions.size(), (row, col) ->
        {
            FunctionInfo function = functions.get(row);
            // CSOFF
            switch (col)
            // CSON
            {
                case 0:
                    return function.getName();
                case 1:
                    return function.getFunctionType();
                case 2:
                    return function.getDescription();
            }

            throw new IllegalArgumentException("Illegal column index: " + col);
        });
    }

    /** Close catalog. Is called by clients to shut down to be able to close resources etc. */
    public void close()
    {
    }
}
