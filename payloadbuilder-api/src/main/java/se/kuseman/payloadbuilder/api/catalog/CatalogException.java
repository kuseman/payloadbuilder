package se.kuseman.payloadbuilder.api.catalog;

/** Exception throws from catalogs */
public class CatalogException extends RuntimeException
{
    private final String catalogAlias;
    private final Object data;

    public CatalogException(String catalogAlias, String message)
    {
        this(catalogAlias, message, null);
    }

    public CatalogException(String catalogAlias, String message, Object data)
    {
        super(message);
        this.catalogAlias = catalogAlias;
        this.data = data;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    /** Get custom data provided from catalog */
    public Object getData()
    {
        return data;
    }
}
