package se.kuseman.payloadbuilder.core.execution;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.cache.CacheProvider;
import se.kuseman.payloadbuilder.core.cache.CacheType;
import se.kuseman.payloadbuilder.core.cache.GenericCache;
import se.kuseman.payloadbuilder.core.cache.InMemoryGenericCache;
import se.kuseman.payloadbuilder.core.cache.InMemoryTempTableCache;
import se.kuseman.payloadbuilder.core.cache.TempTableCache;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.VectorFactory;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.utils.WeakListenerList;

/**
 * A query session. Holds properties for catalog implementations etc.
 *
 * <pre>
 * Life cycle for a session is pretty much infinite, the same session
 * can be used for multiple executions.
 * </pre>
 **/
public class QuerySession implements IQuerySession
{
    /* System properties */
    /** Enable to print logical and physical plans to session print writer */
    public static final QualifiedName PRINT_PLAN = QualifiedName.of("printplan");
    /** Enable to print all logical plans for each optimisation rule. */
    public static final QualifiedName DEBUG_PLAN = QualifiedName.of("debugplan");

    /** Force a nested loop where default would have been a hash match */
    public static final QualifiedName FORCE_NESTED_LOOP = QualifiedName.of("force_nested_loop");

    /** Force no inner cache for non correlated nested loops */
    public static final QualifiedName FORCE_NO_INNER_CACHE = QualifiedName.of("force_no_inner_cache");
    /* End system properties */

    /* Compile fields */
    private final CatalogRegistry catalogRegistry;
    /** Sessions default catalog alias */
    private String defaultCatalogAlias;

    /* Execution fields */
    /** Variable values for {@link VariableExpression}'s */
    private final Map<QualifiedName, ValueVector> variables;
    /** Catalog properties by catalog alias */
    private Map<String, Map<String, ValueVector>> catalogProperties;
    /** System properties */
    private Map<QualifiedName, ValueVector> systemProperties;
    private Writer printWriter;
    private ExceptionHandler exceptionHandler;
    private BooleanSupplier abortSupplier;
    private WeakListenerList abortQueryListeners;
    private Map<QualifiedName, TemporaryTable> temporaryTables;
    private TempTableCache tempTableCache = new InMemoryTempTableCache();
    private GenericCache genericCache = new InMemoryGenericCache("QuerySession");
    private long lastQueryExecutionTime;
    private long lastQueryRowCount;
    private VectorFactory vectorFactory = new VectorFactory(new BufferAllocator());

    public QuerySession(CatalogRegistry catalogRegistry)
    {
        this(catalogRegistry, emptyMap());
    }

    public QuerySession(CatalogRegistry catalogRegistry, Map<String, Object> variables)
    {
        this.catalogRegistry = requireNonNull(catalogRegistry, "catalogRegistry");
        this.variables = requireNonNull(variables, "variables").entrySet()
                .stream()
                .collect(toMap(k -> QualifiedName.of(k.getKey()
                        .toLowerCase()), v -> ValueVector.literalAny(v.getValue())));
    }

    @Override
    public Catalog getSystemCatalog()
    {
        return SystemCatalog.get();
    }

    /** Get default catalog for this session */
    public Catalog getDefaultCatalog()
    {
        return isBlank(defaultCatalogAlias) ? null
                : catalogRegistry.getCatalog(defaultCatalogAlias);
    }

    /** Return default catalog alias */
    @Override
    public String getDefaultCatalogAlias()
    {
        return defaultCatalogAlias;
    }

    public VectorFactory getVectorFactory()
    {
        return vectorFactory;
    }

    public void setVectorFactory(VectorFactory vectorFactory)
    {
        this.vectorFactory = requireNonNull(vectorFactory);
    }

    /**
     * Get default catalog for this session
     *
     * @param alias Alias of the catalog to set as default
     **/
    public void setDefaultCatalogAlias(String alias)
    {
        defaultCatalogAlias = lowerCase(alias);
    }

    /** Get catalog with provided alias. Uses default alias if empty */
    public Catalog getCatalog(String catalogAlias)
    {
        Catalog catalog;
        if (StringUtils.isBlank(catalogAlias))
        {
            catalog = getDefaultCatalog();
        }
        else
        {
            catalog = catalogRegistry.getCatalog(catalogAlias);
        }

        return requireNonNull(catalog, isBlank(catalogAlias) ? "No default catalog specified in registry"
                : "No catalog found with alias: " + catalogAlias);
    }

    /** Resolve scalar function */
    public Pair<String, ScalarFunctionInfo> resolveScalarFunctionInfo(String catalogAlias, String function)
    {
        return resolveFunctionInfo(catalogAlias, function, (c, f) -> c.getScalarFunction(f));
    }

    /** Resolve table function */
    public Pair<String, TableFunctionInfo> resolveTableFunctionInfo(String catalogAlias, String function)
    {
        return resolveFunctionInfo(catalogAlias, function, (c, f) -> c.getTableFunction(f));
    }

    /** Resolve operator function */
    public Pair<String, OperatorFunctionInfo> resolveOperatorFunctionInfo(String catalogAlias, String function)
    {
        return resolveFunctionInfo(catalogAlias, function, (c, f) -> c.getOperatorFunction(f));
    }

    private <T extends FunctionInfo> Pair<String, T> resolveFunctionInfo(String catalogAlias, String function, BiFunction<Catalog, String, T> functionSupplier)
    {
        Catalog catalog;
        if (isBlank(catalogAlias))
        {
            // First try default catalog
            catalog = getDefaultCatalog();
            if (catalog != null)
            {
                T f = functionSupplier.apply(catalog, lowerCase(function));
                // CSOFF
                if (f != null)
                // CSON
                {
                    return Pair.of(defaultCatalogAlias, f);
                }
            }

            catalog = catalogRegistry.getSystemCatalog();
            catalogAlias = Catalog.SYSTEM_CATALOG_ALIAS;
        }
        else
        {
            catalog = catalogRegistry.getCatalog(lowerCase(catalogAlias));
        }

        if (catalog == null)
        {
            return null;
        }

        T f = functionSupplier.apply(catalog, lowerCase(function));
        if (f == null)
        {
            return null;
        }
        return Pair.of(catalogAlias, f);
    }

    /** Get current print writer */
    @Override
    public Writer getPrintWriter()
    {
        return printWriter;
    }

    @Override
    public void handleKnownException(Exception e)
    {
        if (exceptionHandler != null)
        {
            exceptionHandler.handle(e);
            return;
        }
        IQuerySession.super.handleKnownException(e);
    }

    /** Set print writer */
    public void setPrintWriter(Writer printWriter)
    {
        this.printWriter = printWriter;
    }

    /** Set exception handler */
    public void setExceptionHandler(ExceptionHandler exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
    }

    /** Set abort supplier. */
    public void setAbortSupplier(BooleanSupplier abortSupplier)
    {
        this.abortSupplier = abortSupplier;
    }

    /** Abort current query */
    @Override
    public boolean abortQuery()
    {
        return abortSupplier != null
                && abortSupplier.getAsBoolean();
    }

    /** Register abort listener to session */
    @Override
    public void registerAbortListener(Runnable listener)
    {
        if (abortQueryListeners == null)
        {
            abortQueryListeners = new WeakListenerList();
        }
        abortQueryListeners.registerListener(listener);
    }

    @Override
    public void unregisterAbortListener(Runnable listener)
    {
        if (abortQueryListeners == null)
        {
            return;
        }

        abortQueryListeners.unregisterListener(listener);
    }

    /** Fire all registered abort query listeners */
    public void fireAbortQueryListeners()
    {
        if (abortQueryListeners != null)
        {
            abortQueryListeners.fire();
        }
    }

    /** Return catalog registry */
    public CatalogRegistry getCatalogRegistry()
    {
        return catalogRegistry;
    }

    /** Return variables map */
    Map<QualifiedName, ValueVector> getVariables()
    {
        return variables;
    }

    /** Print value to print stream if set */
    public void printLine(Object value)
    {
        if (printWriter != null)
        {
            try
            {
                printWriter.append(String.valueOf(value));
                printWriter.append(System.lineSeparator());
                printWriter.flush();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error writing to print writer", e);
            }
        }
    }

    /** Get temporary table with provided qualifier */
    public TemporaryTable getTemporaryTable(QualifiedName table)
    {
        TemporaryTable result;
        // CSOFF
        if (temporaryTables == null
                || (result = temporaryTables.get(table)) == null)
        // CSON
        {
            throw new QueryException("No temporary table found with name #" + table);
        }

        return result;
    }

    /**
     * Set a temporary table into context
     *
     * @param table The temporary table
     */
    public void setTemporaryTable(QualifiedName name, TemporaryTable table)
    {
        requireNonNull(table);
        if (temporaryTables == null)
        {
            temporaryTables = new HashMap<>();
        }
        if (temporaryTables.containsKey(name))
        {
            throw new QueryException("Temporary table #" + name + " already exists in session");
        }
        temporaryTables.put(name, table);
    }

    /** Drop temporary table */
    public void dropTemporaryTable(QualifiedName table, boolean lenient)
    {
        if (!lenient
                && (temporaryTables == null
                        || !temporaryTables.containsKey(table)))
        {
            throw new QueryException("No temporary table found with name #" + table);
        }
        else if (temporaryTables != null)
        {
            temporaryTables.remove(table);
        }
    }

    /** Return temporary table names */
    public Collection<Entry<QualifiedName, TemporaryTable>> getTemporaryTables()
    {
        if (temporaryTables == null)
        {
            return emptyList();
        }
        return temporaryTables.entrySet();
    }

    /** Set catalog property */
    @Override
    public void setCatalogProperty(String alias, String key, ValueVector value)
    {
        if (catalogProperties == null)
        {
            catalogProperties = new HashMap<>();
        }

        catalogProperties.computeIfAbsent(alias, k -> new HashMap<>())
                .put(key, value);
    }

    /** Get catalog property */
    @Override
    public ValueVector getCatalogProperty(String alias, String key)
    {
        if (catalogProperties == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Column.Type.Any), 1);
        }

        ValueVector property = catalogProperties.getOrDefault(alias, emptyMap())
                .get(key);

        if (property == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Column.Type.Any), 1);
        }
        return property;
    }

    /** Set system property */
    public void setSystemProperty(QualifiedName name, ValueVector value)
    {
        requireNonNull(value);
        if (systemProperties == null)
        {
            systemProperties = new HashMap<>();
        }
        systemProperties.put(name, value);
    }

    /** Get system property */
    public ValueVector getSystemProperty(QualifiedName name)
    {
        if (systemProperties == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Column.Type.Any), 1);
        }

        ValueVector property = systemProperties.get(name);
        if (property == null)
        {
            return ValueVector.literalNull(ResolvedType.of(Column.Type.Any), 1);
        }
        return property;
    }

    /** Return temp table cache */
    public TempTableCache getTempTableCache()
    {
        return tempTableCache;
    }

    /** Set temp table cache */
    public void setTempTableCache(TempTableCache tempTableCache)
    {
        this.tempTableCache = requireNonNull(tempTableCache, "Cache cannot be null");
    }

    /** Return generic cache */
    @Override
    public GenericCache getGenericCache()
    {
        return genericCache;
    }

    /** Set generic cache */
    public void setGenericCache(GenericCache genericCache)
    {
        this.genericCache = requireNonNull(genericCache, "Cache cannot be null");
    }

    /** Return cache with provided type */
    public CacheProvider getCache(CacheType type)
    {
        return switch (type)
        {
            case GENERIC -> genericCache;
            case TEMPTABLE -> tempTableCache;
            default -> throw new IllegalArgumentException("Unknown cache type " + type);
        };
    }

    @Override
    public long getLastQueryExecutionTime()
    {
        return lastQueryExecutionTime;
    }

    public void setLastQueryExecutionTime(long lastQueryExecutionTime)
    {
        this.lastQueryExecutionTime = lastQueryExecutionTime;
    }

    @Override
    public long getLastQueryRowCount()
    {
        return lastQueryRowCount;
    }

    public void setLastQueryRowCount(long lastQueryRowCount)
    {
        this.lastQueryRowCount = lastQueryRowCount;
    }

    /** Exception handle for known exceptions thrown during execution */
    public interface ExceptionHandler
    {
        void handle(Exception e);
    }
}
