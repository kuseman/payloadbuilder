package org.kuse.payloadbuilder.core;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.parser.NamedParameterExpression;

/**
 * A query session. Holds properties for catalog implementations etc. Can live through multiple query executions
 **/
public class QuerySession
{
    //    private static final String CATALOG = "catalog";
    //    private static final String DEFAULTCATALOG = "defaultCatalog";
    private final CatalogRegistry catalogRegistry;
    /** Parameter values for {@link NamedParameterExpression}'s */
    private final Map<String, Object> parameters;
    /** Session scoped variables for {@link VariableExpression}'s */

    /** Catalog properties by catalog alias */
    private Map<String, Map<String, Object>> catalogProperties;

    //    private Map<String, Object> variables;
    //    private PropertyChangeSupport pcs;
    private String defaultCatalogAlias;
    private PrintStream printStream;
    private BooleanSupplier abortSupplier;

    public QuerySession(CatalogRegistry catalogRegistry)
    {
        this(catalogRegistry, emptyMap());
    }

    public QuerySession(CatalogRegistry catalogRegistry, Map<String, Object> parameters)
    {
        this.catalogRegistry = requireNonNull(catalogRegistry, "catalogRegistry");
        this.parameters = requireNonNull(parameters, "parameters");
    }

    /** Set print stream */
    public void setPrintStream(PrintStream printStream)
    {
        this.printStream = printStream;
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

    /** Return value for named parameter */
    public Object getParameterValue(String name)
    {
        return parameters.get(name);
    }

    /** Get default catalog for this session */
    public Catalog getDefaultCatalog()
    {
        return isBlank(defaultCatalogAlias) ? null : catalogRegistry.getCatalog(defaultCatalogAlias);
    }
    
    /** Return default catalog alias */
    public String getDefaultCatalogAlias()
    {
        return defaultCatalogAlias;
    }

    /**
     * Get default catalog for this session
     * 
     * @param alias Alias of the catalog to set as default
     **/
    public void setDefaultCatalog(String alias)
    {
//        Catalog oldValue = this.defaultCatalog;
        defaultCatalogAlias = alias;
//        if (!Objects.equals(oldValue, newValue))
//        {
//            this.defaultCatalog = newValue;
//            //            if (pcs != null)
//            //            {
//            //                pcs.firePropertyChange(DEFAULTCATALOG, oldValue, newValue);
//            //            }
//        }
    }

    //    /** Set a variable to session */
    //    public void setVariable(String name, Object value)
    //    {
    ////        if (variables == null)
    ////        {
    ////            variables = new HashMap<>();
    ////        }
    //
    //        // Default catalog key
    ////        if (CATALOG.equals(lowerCase(name)))
    ////        {
    ////            String alias = String.valueOf(value);
    ////            Catalog c = catalogRegistry.getCatalog(alias);
    ////            if (c == null)
    ////            {
    ////                throw new IllegalArgumentException("Cannot find a catalog with alias " + alias);
    ////            }
    ////            setDefaultCatalog(c);
    ////            return;
    ////        }
    //        
    ////        Object oldValue = variables.put(name, value);
    ////        if (!Objects.equals(oldValue, value))
    ////        {
    ////            if (pcs != null)
    ////            {
    ////                pcs.firePropertyChange(name, oldValue, value);
    ////            }
    ////        }
    //    }
    //
    //    /** Return variables map */
    //    public Map<String, Object> getVariables()
    //    {
    //        return ObjectUtils.defaultIfNull(variables, emptyMap());
    //    }

    /** Return parameters map */
    public Map<String, Object> getParameters()
    {
        return parameters;
    }

    //    /** Get property by key */
    //    public Object getVariableValue(String name)
    //    {
    //        if (variables == null)
    //        {
    //            return null;
    //        }
    //        
    //        return variables.get(name);
    //    }

    //    /** Add property change listener */
    //    public void addPropertyChangeListener(PropertyChangeListener listener)
    //    {
    //        if (pcs == null)
    //        {
    //            pcs = new PropertyChangeSupport(this);
    //        }
    //        pcs.addPropertyChangeListener(listener);
    //    }
    //
    //    /** Remove property change listener */
    //    public void remvoePropertyChangeListener(PropertyChangeListener listener)
    //    {
    //        if (pcs == null)
    //        {
    //            return;
    //        }
    //        pcs.removePropertyChangeListener(listener);
    //    }

    /** Print value to print stream if set */
    public void printLine(Object value)
    {
        if (printStream != null)
        {
            printStream.println(value);
        }
    }

    /** Set catalog property */
    public void setCatalogProperty(String alias, String key, Object value)
    {
        if (catalogProperties == null)
        {
            catalogProperties = new HashMap<>();
        }

//        if (catalogRegistry.getCatalog(alias) == null)
//        {
//            throw new IllegalArgumentException("No catalog found with alias " + alias);
//        }

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

    //    /** Set internal property */
    //    public void setProperty(String name, Object value)
    //    {
    //        if (catalogProperties == null)
    //        {
    //            catalogProperties = new HashMap<>();
    //        }
    //        
    //        catalogProperties.put(name, value);
    //    }
    //    
    //    /** Get internal property by name */
    //    @SuppressWarnings("unchecked")
    //    public <T> T getProperty(String name)
    //    {
    //        if (catalogProperties == null)
    //        {
    //            return null;
    //        }
    //        return (T) catalogProperties.get(name);
    //    }

    /**
     * Resolves function info from provided
     * 
     * @param functionId Unique function id to cache a lookup
     */
    public FunctionInfo resolveFunctionInfo(String catalogAlias, String function, int functionId)
    {
        Catalog catalog;
        if (catalogAlias == null)
        {
            catalog = catalogRegistry.getBuiltin();
        }
        else
        {
            catalog = catalogRegistry.getCatalog(catalogAlias);
        }

        if (catalog == null)
        {
            return null;
        }

        return catalog.getFunction(function);
    }
}
