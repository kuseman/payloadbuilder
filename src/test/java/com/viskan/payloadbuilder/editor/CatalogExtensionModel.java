package com.viskan.payloadbuilder.editor;

/** Model for a catalog extension */
class CatalogExtensionModel
{
//    private final ICatalogExtension extension;
    private boolean enabled = true;
    private String alias;
    
    CatalogExtensionModel(String alias)
//            ICatalogExtension extension
//            )
    {
//        this.extension = requireNonNull(extension);
        this.alias = alias;
    }
    
//    public ICatalogExtension getExtension()
//    {
//        return extension;
//    }
    
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
