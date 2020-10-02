package org.kuse.payloadbuilder.catalog.fs;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.editor.ICatalogExtension;

/** Catalog editor extension for {@link FilesystemCatalog} */
class FilesystemCatalogExtension implements ICatalogExtension
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
