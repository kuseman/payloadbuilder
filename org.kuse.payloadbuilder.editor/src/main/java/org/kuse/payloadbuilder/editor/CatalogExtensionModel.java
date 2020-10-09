package org.kuse.payloadbuilder.editor;

/** Model for a catalog extension */
class CatalogExtensionModel
{
    private boolean enabled = true;
    private String alias;

    CatalogExtensionModel(String alias)
    {
        this.alias = alias;
    }

    boolean isEnabled()
    {
        return enabled;
    }

    void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    String getAlias()
    {
        return alias;
    }

    void setAlias(String alias)
    {
        this.alias = alias;
    }
}
