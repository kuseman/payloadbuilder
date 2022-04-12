package se.kuseman.payloadbuilder.catalog.es;

import se.kuseman.payloadbuilder.api.catalog.CatalogException;

/** Exception thrown when credentials are missing in ES catalog properies */
class CredentialsException extends CatalogException
{
    CredentialsException(String catalogAlias, String message)
    {
        super(catalogAlias, message);
    }
}
