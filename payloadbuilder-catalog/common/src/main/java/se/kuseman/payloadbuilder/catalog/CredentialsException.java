package se.kuseman.payloadbuilder.catalog;

import se.kuseman.payloadbuilder.api.catalog.CatalogException;

/** Exception thrown when credentials are missing in catalog properies */
public class CredentialsException extends CatalogException
{
    public CredentialsException(String catalogAlias, String message)
    {
        super(catalogAlias, message);
    }
}
