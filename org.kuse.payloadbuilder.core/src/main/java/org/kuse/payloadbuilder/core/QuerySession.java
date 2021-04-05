package org.kuse.payloadbuilder.core;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.VariableExpression;

/**
 * A query session. Holds properties for catalog implementations etc. Can live through multiple query executions
 **/
public class QuerySession
{
    private final CatalogRegistry catalogRegistry;
    /** Variable values for {@link VariableExpression}'s */
    private final Map<String, Object> variables;
    /** Catalog properties by catalog alias */
    private Map<String, Map<String, Object>> catalogProperties;
    private Writer printWriter;
    private BooleanSupplier abortSupplier;
    private Map<QualifiedName, List<Tuple>> temporaryTables;
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
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error writing to print writer", e);
            }
        }
    }

    /** Get temporary table with provided qualifier */
    public List<Tuple> getTemporaryTable(QualifiedName table)
    {
        List<Tuple> rows;
        //CSOFF
        if (temporaryTables == null
            || (rows = temporaryTables.get(table)) == null)
        //CSON
        {
            throw new QueryException("No temporary table found with name #" + table);
        }

        return rows;
    }

    /** Set a temporary table into context */
    public void setTemporaryTable(QualifiedName table, List<Tuple> rows)
    {
        requireNonNull(rows);
        if (temporaryTables == null)
        {
            temporaryTables = new HashMap<>();
        }
        if (temporaryTables.containsKey(table))
        {
            throw new QueryException("Temporary table #" + table + " already exists in session");
        }
        temporaryTables.put(table, rows);
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
