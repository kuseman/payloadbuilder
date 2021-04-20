package org.kuse.payloadbuilder.core;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.kuse.payloadbuilder.core.cache.BatchCacheProvider;
import org.kuse.payloadbuilder.core.cache.CacheProvider;
import org.kuse.payloadbuilder.core.cache.CacheProvider.Type;
import org.kuse.payloadbuilder.core.cache.CustomCacheProvider;
import org.kuse.payloadbuilder.core.cache.SessionBatchCacheProvider;
import org.kuse.payloadbuilder.core.cache.SessionCustomCacheProvider;
import org.kuse.payloadbuilder.core.cache.SessionTempTableCacheProvider;
import org.kuse.payloadbuilder.core.cache.TempTableCacheProvider;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.TemporaryTable;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.VariableExpression;

/**
 * A query session. Holds properties for catalog implementations etc.
 *
 * <pre>
 * Life cycle for a session is pretty much infinite, the same session
 * can be used for multiple executions.
 * </pre>
 **/
public class QuerySession
{
    /* System properties */
    /**
     * Enable code gen for query. Will try to precompile every expression that supports code gen. NOTE! Will be default in the future and this
     * property will be removed
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

    /** Get current print writer */
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
    public boolean abortQuery()
    {
        return abortSupplier != null && abortSupplier.getAsBoolean();
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
        //CSOFF
        if (temporaryTables == null
            || (result = temporaryTables.get(table)) == null)
        //CSON
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
    public void setTemporaryTable(TemporaryTable table)
    {
        requireNonNull(table);
        if (temporaryTables == null)
        {
            temporaryTables = new HashMap<>();
        }
        if (temporaryTables.containsKey(table.getName()))
        {
            throw new QueryException("Temporary table #" + table.getName() + " already exists in session");
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
            throw new QueryException("No temporary table found with name #" + table);
        }
        else if (temporaryTables != null)
        {
            temporaryTables.remove(table);
        }
    }

    /** Return temporary table names */
    public Collection<QualifiedName> getTemporaryTableNames()
    {
        if (temporaryTables == null)
        {
            return emptyList();
        }
        return temporaryTables.keySet();
    }

    /** Set catalog property */
    public void setCatalogProperty(String alias, String key, Object value)
    {
        if (catalogProperties == null)
        {
            catalogProperties = new HashMap<>();
        }

        catalogProperties
                .computeIfAbsent(alias, k -> new HashMap<>())
                .put(key, value);
    }

    /** Get catalog property */
    @SuppressWarnings("unchecked")
    public <T> T getCatalogProperty(String alias, String key)
    {
        if (catalogProperties == null)
        {
            return null;
        }

        return (T) catalogProperties.getOrDefault(alias, emptyMap()).get(key);
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
        return systemProperties != null ? systemProperties.get(lowerCase(name)) : null;
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
        if (batchCacheProvider.getType() != Type.BATCH)
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
        if (tempTableCacheProvider.getType() != Type.TEMPTABLE)
        {
            throw new IllegalArgumentException("Wrong type of cache provider");
        }
    }

    /** Return custom cache provider */
    public CustomCacheProvider getCustomCacheProvider()
    {
        return customCacheProvider;
    }

    /** Set custom cache provider */
    public void setCustomCacheProvider(CustomCacheProvider customCacheProvider)
    {
        this.customCacheProvider = requireNonNull(customCacheProvider, "Cache provider cannot be null");
        if (customCacheProvider.getType() != Type.CUSTOM)
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
