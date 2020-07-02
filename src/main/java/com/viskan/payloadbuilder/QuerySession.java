package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.FunctionInfo;
import com.viskan.payloadbuilder.parser.QualifiedName;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

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
    private static final String DEFAULTCATALOG = "defaultCatalog";

    private final CatalogRegistry catalogRegistry;
    private Map<String, Object> properties;
    private PropertyChangeSupport pcs;
    private Catalog defaultCatalog;
    private PrintStream printStream = System.out;
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
    
    /** Get print stream */
    public PrintStream getPrintStream()
    {
        return printStream;
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

    /** Set a property to session */
    public void setProperty(String key, Object value)
    {
        if (properties == null)
        {
            properties = new HashMap<>();
        }

        Object oldValue = properties.put(key, value);
        if (!Objects.equals(oldValue, value))
        {
            if (pcs != null)
            {
                pcs.firePropertyChange(key, oldValue, value);
            }
        }
    }

    /** Get property by key */
    public Object getProperty(String key)
    {
        if (properties == null)
        {
            return null;
        }
        return properties.get(key);
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
    
    /** Resolves function info from provided */
    public FunctionInfo resolveFunctionInfo(QualifiedName qname, int functionId)
    {
        // Todo: store lookup
        
        Catalog catalog;
        if (qname.getCatalog() == null)
        {
            catalog = catalogRegistry.getBuiltin();
        }
        else
        {
            catalog = catalogRegistry.getCatalog(qname.getCatalog());
        }
        
        if (catalog == null)
        {
            return null;
        }
        
        
        return catalog.getFunction(qname.getFirst()) ;
    }
}
