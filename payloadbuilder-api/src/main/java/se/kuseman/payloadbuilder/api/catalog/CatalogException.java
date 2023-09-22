package se.kuseman.payloadbuilder.api.catalog;

/** Exception throws from catalogs */
public class CatalogException extends RuntimeException
{
    private final String catalogAlias;

    public CatalogException(String catalogAlias, String message)
    {
        super(message);
        this.catalogAlias = catalogAlias;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }
}
