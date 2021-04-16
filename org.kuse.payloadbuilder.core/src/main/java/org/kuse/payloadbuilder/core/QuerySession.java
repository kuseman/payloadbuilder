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

import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.TemporaryTable;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.VariableExpression;

/**
 * A query session. Holds properties for catalog implementations etc. Can live through multiple query executions
 **/
public class QuerySession
{
    /* System properties */
    /**
     * Enable code gen for query. Will try to precompile every expression that supports code gen. NOTE! Will be default in the future and this
     * property will be removed
     */
    public static final String CODEGEN_ENABLED = "codegen.enabled";
    /* End system properties */

    private final CatalogRegistry catalogRegistry;
    /** Variable values for {@link VariableExpression}'s */
    private final Map<String, Object> variables;
    /** Catalog properties by catalog alias */
    private Map<String, Map<String, Object>> catalogProperties;
    private Writer printWriter;
    private BooleanSupplier abortSupplier;
    private Map<QualifiedName, TemporaryTable> temporaryTables;
    private Map<String, Object> systemProperties;

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
    public Object getCatalogProperty(String alias, String key)
    {
        if (catalogProperties == null)
        {
            return null;
        }

        return catalogProperties.getOrDefault(alias, emptyMap()).get(key);
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
}
