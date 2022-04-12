package se.kuseman.payloadbuilder.core;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.session.CacheProvider;
import se.kuseman.payloadbuilder.api.session.CustomCacheProvider;
import se.kuseman.payloadbuilder.api.session.IQuerySession;
import se.kuseman.payloadbuilder.core.cache.BatchCacheProvider;
import se.kuseman.payloadbuilder.core.cache.SessionBatchCacheProvider;
import se.kuseman.payloadbuilder.core.cache.SessionCustomCacheProvider;
import se.kuseman.payloadbuilder.core.cache.SessionTempTableCacheProvider;
import se.kuseman.payloadbuilder.core.cache.TempTableCacheProvider;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.operator.TemporaryTable;
import se.kuseman.payloadbuilder.core.parser.VariableExpression;

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
    /**
     * Enable code gen for query. Will try to precompile every expression that supports code gen. NOTE! Will be default in the future and this property will be removed
     */
    public static final String CODEGEN_ENABLED = "codegen.enabled";
    /** Disable batch cache */
    public static final String BATCH_CACHE_DISABLED = "batch_cache.disbled";
    /** Only perform reads when using batch cache */
    public static final String BATCH_CACHE_READ_ONLY = "batch_cache.read_only";
    /* End system properties */

    private final CatalogRegistry catalogRegistry;

    /** Variable values for {@link VariableExpression}'s */
    private final Map<String, Object> variables;
    /** Sessions default catalog alias */
    private String defaultCatalogAlias;
    /** Catalog properties by catalog alias */
    private Map<String, Map<String, Object>> catalogProperties;
    /** System properties */
    private Map<String, Object> systemProperties;

    private Writer printWriter;
    private BooleanSupplier abortSupplier;
    private Map<QualifiedName, TemporaryTable> temporaryTables;

    private BatchCacheProvider batchCacheProvider = new SessionBatchCacheProvider();
    private TempTableCacheProvider tempTableCacheProvider = new SessionTempTableCacheProvider();
    private CustomCacheProvider customCacheProvider = new SessionCustomCacheProvider();

    public QuerySession(CatalogRegistry catalogRegistry)
    {
        this(catalogRegistry, emptyMap());
    }

    public QuerySession(CatalogRegistry catalogRegistry, Map<String, Object> variables)
    {
        this.catalogRegistry = requireNonNull(catalogRegistry, "catalogRegistry");
        this.variables = requireNonNull(variables, "variables");
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

    /**
     * Get default catalog for this session
     *
     * @param alias Alias of the catalog to set as default
     **/
    public void setDefaultCatalogAlias(String alias)
    {
        defaultCatalogAlias = alias;
    }

    /**
     * Resolves function info from provided catalog alias and function name
     */
    public Pair<String, FunctionInfo> resolveFunctionInfo(String catalogAlias, String function)
    {
        Catalog catalog;
        if (isBlank(catalogAlias))
        {
            // First try default catalog
            catalog = getDefaultCatalog();
            if (catalog != null)
            {
                FunctionInfo f = catalog.getFunction(lowerCase(function));
                // CSOFF
                if (f != null)
                // CSON
                {
                    return Pair.of(defaultCatalogAlias, f);
                }
            }

            catalog = catalogRegistry.getSystemCatalog();
        }
        else
        {
            catalog = catalogRegistry.getCatalog(lowerCase(catalogAlias));
        }

        if (catalog == null)
        {
            return null;
        }

        FunctionInfo f = catalog.getFunction(lowerCase(function));
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

    /** Set print writer */
    public void setPrintWriter(Writer printWriter)
    {
        this.printWriter = printWriter;
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

    /** Return catalog registry */
    public CatalogRegistry getCatalogRegistry()
    {
        return catalogRegistry;
    }

    /** Return variables map */
    public Map<String, Object> getVariables()
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
            throw new QueryException("No temporary table found with name #" + table.toDotDelimited());
        }

        return result;
    }

    /**
     * Set a temporary table into context
     *
     * @param table The temporary table
     */
    public void setTemporaryTable(TemporaryTable table)
    {
        requireNonNull(table);
        if (temporaryTables == null)
        {
            temporaryTables = new HashMap<>();
        }
        if (temporaryTables.containsKey(table.getName()))
        {
            throw new QueryException("Temporary table #" + table.getName()
                    .toDotDelimited() + " already exists in session");
        }
        temporaryTables.put(table.getName(), table);
    }

    /** Drop temporary table */
    public void dropTemporaryTable(QualifiedName table, boolean lenient)
    {
        if (!lenient
                && (temporaryTables == null
                        || !temporaryTables.containsKey(table)))
        {
            throw new QueryException("No temporary table found with name #" + table.toDotDelimited());
        }
        else if (temporaryTables != null)
        {
            temporaryTables.remove(table);
        }
    }

    /** Return temporary table names */
    public Collection<TemporaryTable> getTemporaryTables()
    {
        if (temporaryTables == null)
        {
            return emptyList();
        }
        return temporaryTables.values();
    }

    /** Set catalog property */
    @Override
    public void setCatalogProperty(String alias, String key, Object value)
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
    @SuppressWarnings("unchecked")
    public <T> T getCatalogProperty(String alias, String key)
    {
        if (catalogProperties == null)
        {
            return null;
        }

        return (T) catalogProperties.getOrDefault(alias, emptyMap())
                .get(key);
    }

    /** Set system property */
    public void setSystemProperty(String name, Object value)
    {
        if (systemProperties == null)
        {
            systemProperties = new HashMap<>();
        }
        systemProperties.put(name, value);
    }

    /** Get system property */
    public Object getSystemProperty(String name)
    {
        return systemProperties != null ? systemProperties.get(lowerCase(name))
                : null;
    }

    /** Return batch cache provider */
    public BatchCacheProvider getBatchCacheProvider()
    {
        return batchCacheProvider;
    }

    /** Set batch cache provider */
    public void setBatchCacheProvider(BatchCacheProvider batchCacheProvider)
    {
        this.batchCacheProvider = requireNonNull(batchCacheProvider, "Cache provider cannot be null");
        if (batchCacheProvider.getType() != CacheProvider.Type.BATCH)
        {
            throw new IllegalArgumentException("Wrong type of cache provider");
        }
    }

    /** Return temp table cache provider */
    public TempTableCacheProvider getTempTableCacheProvider()
    {
        return tempTableCacheProvider;
    }

    /** Set temp table cache provider */
    public void setTempTableCacheProvider(TempTableCacheProvider tempTableCacheProvider)
    {
        this.tempTableCacheProvider = requireNonNull(tempTableCacheProvider, "Cache provider cannot be null");
        if (tempTableCacheProvider.getType() != CacheProvider.Type.TEMPTABLE)
        {
            throw new IllegalArgumentException("Wrong type of cache provider");
        }
    }

    /** Return custom cache provider */
    @Override
    public CustomCacheProvider getCustomCacheProvider()
    {
        return customCacheProvider;
    }

    /** Set custom cache provider */
    public void setCustomCacheProvider(CustomCacheProvider customCacheProvider)
    {
        this.customCacheProvider = requireNonNull(customCacheProvider, "Cache provider cannot be null");
        if (customCacheProvider.getType() != CacheProvider.Type.CUSTOM)
        {
            throw new IllegalArgumentException("Wrong type of cache provider");
        }
    }

    /** Return cache provider with provided type */
    public CacheProvider getCacheProvider(CacheProvider.Type type)
    {
        switch (type)
        {
            case BATCH:
                return batchCacheProvider;
            case CUSTOM:
                return customCacheProvider;
            case TEMPTABLE:
                return tempTableCacheProvider;
            default:
                throw new IllegalArgumentException("Unknown cache type " + type);
        }
    }
}
