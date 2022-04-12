package se.kuseman.payloadbuilder.catalog.jdbc;

import se.kuseman.payloadbuilder.api.catalog.CatalogException;

/** Exception thrown when credentials are missing in Jdbc catalog properies */
class CredentialsException extends CatalogException
{
    CredentialsException(String catalogAlias, String message)
    {
        super(catalogAlias, message);
    }
}
