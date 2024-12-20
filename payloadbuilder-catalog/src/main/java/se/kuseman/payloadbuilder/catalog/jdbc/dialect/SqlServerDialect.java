package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;

/** Dialect for SQL server */
class SqlServerDialect implements SqlDialect
{

    /**
     * @formatter:off
     * FROM SQLServer JDBC driver
     * --------------------------
     * The constant in the Java programming language, sometimes referred to as a type code, that identifies the
     * Microsoft SQL type DATETIMEOFFSET.
     * @formatter:on
     */
    static final int DATETIMEOFFSET = -155;

    @Override
    public Type getColumnType(ResultSetMetaData rsmd, int jdbcType, int ordinal) throws SQLException
    {
        if (DATETIMEOFFSET == jdbcType)
        {
            return Column.Type.DateTimeOffset;
        }
        return SqlDialect.super.getColumnType(rsmd, jdbcType, ordinal);
    }

    @Override
    public void setResultSetValue(Column.Type type, ResultSet rs, int ordinal, int row, int jdbcType, MutableValueVector vector) throws SQLException, IOException
    {
        // Special handling of SQLServer's own DateTimeOffset type
        if (type == Column.Type.DateTimeOffset)
        {
            OffsetDateTime odt = rs.getObject(ordinal, OffsetDateTime.class);
            if (odt != null)
            {
                vector.setDateTimeOffset(row, EpochDateTimeOffset.from(odt));
            }
            if (rs.wasNull())
            {
                vector.setNull(row);
            }
        }
        else
        {
            SqlDialect.super.setResultSetValue(type, rs, ordinal, row, jdbcType, vector);
        }
    }
}
