package se.kuseman.payloadbuilder.catalog.jdbc;

import java.sql.SQLException;

import se.kuseman.payloadbuilder.api.catalog.CatalogException;

/** Connection exception thrown for when an connection couldn't be created */
class ConnectionException extends CatalogException
{
    private final SQLException exception;

    ConnectionException(String catalogAlias, SQLException exception)
    {
        super(catalogAlias, exception.getMessage());
        this.exception = exception;
    }

    SQLException getException()
    {
        return exception;
    }
}
