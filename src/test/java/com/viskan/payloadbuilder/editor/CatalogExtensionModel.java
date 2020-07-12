package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.editor.catalog.ICatalogExtension;

import static java.util.Objects.requireNonNull;

/** Model for a catalog extension */
class CatalogExtensionModel
{
    private final ICatalogExtension extension;
    private boolean enabled;
    private String alias;
    
    CatalogExtensionModel(ICatalogExtension extension)
    {
        this.extension = requireNonNull(extension);
        this.alias = extension.getDefaultAlias();
    }
    
    public ICatalogExtension getExtension()
    {
        return extension;
    }
    
    public boolean isEnabled()
    {
        return enabled;
    }
    
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }
    
    public String getAlias()
    {
        return alias;
    }
    
    public void setAlias(String alias)
    {
        this.alias = alias;
    }
}
