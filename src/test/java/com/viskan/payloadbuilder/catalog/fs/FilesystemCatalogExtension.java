package com.viskan.payloadbuilder.catalog.fs;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.editor.ICatalogExtension;

/** Catalog editor extension for {@link FilesystemCatalog} */
public class FilesystemCatalogExtension implements ICatalogExtension
{
    private static final Catalog CATALOG = new FilesystemCatalog();

    @Override
    public String getTitle()
    {
        return CATALOG.getName();
    }

    @Override
    public String getDefaultAlias()
    {
        return "fs";
    }

    @Override
    public Catalog getCatalog()
    {
        return CATALOG;
    }
}
