package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.FunctionInfo;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/** A query session. Holds properties for catalog implementations etc.
 * Can live through mulitple query executions 
 **/
public class QuerySession
{
    private static final String CATALOG = "catalog";
    private static final String DEFAULTCATALOG = "defaultCatalog";
    private final CatalogRegistry catalogRegistry;
    private Map<String, Object> variables;
    private PropertyChangeSupport pcs;
    private Catalog defaultCatalog;
    private PrintStream printStream;
    private BooleanSupplier abortSupplier;

    private final Map<String, Object> parameters;

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
    
    /** Set abort supplier */
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
        return defaultCatalog;
    }

    /** Get default catalog for this session */
    public void setDefaultCatalog(Catalog defaultCatalog)
    {
        Catalog oldValue = this.defaultCatalog;
        Catalog newValue = defaultCatalog;
        if (!Objects.equals(oldValue, newValue))
        {
            this.defaultCatalog = defaultCatalog;
            if (pcs != null)
            {
                pcs.firePropertyChange(DEFAULTCATALOG, oldValue, newValue);
            }
        }
    }

    /** Set a variable to session */
    public void setVariable(String name, Object value)
    {
        if (variables == null)
        {
            variables = new HashMap<>();
        }

        // Default catalog key
        if (CATALOG.equals(lowerCase(name)))
        {
            String catalogName = String.valueOf(value);
            Catalog c = catalogRegistry.getCatalog(String.valueOf(catalogName));
            if (c == null)
            {
                throw new IllegalArgumentException("Cannot find a catalog named " + catalogName);
            }
            setDefaultCatalog(c);
            return;
        }
        
        Object oldValue = variables.put(name, value);
        if (!Objects.equals(oldValue, value))
        {
            if (pcs != null)
            {
                pcs.firePropertyChange(name, oldValue, value);
            }
        }
    }

    /** Get property by key */
    public Object getVariable(String name)
    {
        if (variables == null)
        {
            return null;
        }
        
        return variables.get(name);
    }

    /** Add property change listener */
    public void addPropertyChangeListener(PropertyChangeListener listener)
    {
        if (pcs == null)
        {
            pcs = new PropertyChangeSupport(this);
        }
        pcs.addPropertyChangeListener(listener);
    }

    /** Remove property change listener */
    public void remvoePropertyChangeListener(PropertyChangeListener listener)
    {
        if (pcs == null)
        {
            return;
        }
        pcs.removePropertyChangeListener(listener);
    }
    
    /** Print value to print stream if set */
    public void printLine(Object value)
    {
        if (printStream != null)
        {
            printStream.println(value);
        }
    }
    
    /** Resolves function info from provided 
     * @param functionId Unique function id to cache a lookup
     * */
    public FunctionInfo resolveFunctionInfo(String catalogName, String function, int functionId)
    {
        Catalog catalog;
        if (catalogName == null)
        {
            catalog = catalogRegistry.getBuiltin();
        }
        else
        {
            catalog = catalogRegistry.getCatalog(catalogName);
        }
        
        if (catalog == null)
        {
            return null;
        }
        
        return catalog.getFunction(function) ;
    }
}
