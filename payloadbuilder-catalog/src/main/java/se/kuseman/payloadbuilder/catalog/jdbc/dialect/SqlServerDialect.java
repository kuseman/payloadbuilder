package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.sql.ResultSet;
import java.time.OffsetDateTime;

class SqlServerDialect implements SqlDialect
{
    @Override
    public Object getJdbcValue(ResultSet rs, int ordinal, int jdbcType) throws Exception
    {
        // SPecial handling for DATETIMEOFFSET
        if (jdbcType == -155)
        {
            return rs.getObject(ordinal, OffsetDateTime.class);
        }
        return SqlDialect.super.getJdbcValue(rs, ordinal, jdbcType);
    }
}
