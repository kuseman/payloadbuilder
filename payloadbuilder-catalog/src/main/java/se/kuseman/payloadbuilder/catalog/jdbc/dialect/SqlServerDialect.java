package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;

import microsoft.sql.DateTimeOffset;

/** Dialect for SQL server */
class SqlServerDialect implements SqlDialect
{
    @Override
    public void setResultSetValue(Type type, ResultSet rs, int ordinal, int row, MutableValueVector vector) throws SQLException, IOException
    {
        // Special handling of SQLServer's own DateTimeOffset type
        if (type == Type.DateTimeOffset)
        {
            DateTimeOffset dto = (DateTimeOffset) rs.getObject(ordinal);
            if (dto != null)
            {
                vector.setDateTimeOffset(row, EpochDateTimeOffset.from(dto.getOffsetDateTime()));
            }
            if (rs.wasNull())
            {
                vector.setNull(row);
            }
        }
        else
        {
            SqlDialect.super.setResultSetValue(type, rs, ordinal, row, vector);
        }
    }
}
