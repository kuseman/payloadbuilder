package org.kuse.payloadbuilder.catalog.jdbc;

import org.kuse.payloadbuilder.core.catalog.CatalogException;

/** Exception thrown when credentials are missing in Jdbc catalog properies */
class CredentialsException extends CatalogException
{
    CredentialsException(String catalogAlias, String message)
    {
        super(catalogAlias, message);
    }
}
