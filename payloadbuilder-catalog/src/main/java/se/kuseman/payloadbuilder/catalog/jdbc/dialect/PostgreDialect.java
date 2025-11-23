package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/** Dialect for Postgresql. */
class PostgreDialect implements SqlDialect
{
    private static final String TIMESTAMPTZ = "TIMESTAMPTZ";

    @Override
    public String getIdentifierQuoteString(Connection connection) throws SQLException
    {
        // No quotes for Postgres since then all identifiers needs to be quoted
        return "";
    }

    @Override
    public ColumnMeta getColumnMeta(ResultSetMetaData rsmd, int jdbcType, int ordinal) throws SQLException
    {
        ColumnMeta meta = SqlDialect.super.getColumnMeta(rsmd, jdbcType, ordinal);

        if (meta.type() == Type.Any)
        {
            // See if the type name can be of use to determine type
            String typeName = rsmd.getColumnTypeName(ordinal);
            // A json/xml type from eg. postgres => String
            if ("json".equalsIgnoreCase(typeName)
                    || "xml".equalsIgnoreCase(typeName)
                    || "uuid".equalsIgnoreCase(typeName))
            {
                return new ColumnMeta(Type.String, -1, meta.scale());
            }
        }
        // Check to see if its a timestampz
        else if (jdbcType == java.sql.Types.TIMESTAMP)
        {
            String typeName = rsmd.getColumnTypeName(ordinal);
            if (TIMESTAMPTZ.equalsIgnoreCase(typeName))
            {
                return new ColumnMeta(Type.DateTimeOffset, meta.precision(), meta.scale());
            }
        }
        else if (meta.type() == Type.String
                && meta.precision() == Integer.MAX_VALUE)
        {
            return new ColumnMeta(meta.type(), -1, meta.scale());
        }
        return meta;
    }

    @Override
    public String getColumnDeclaration(Type type, int scale, int precision)
    {
        if (type == Type.Boolean)
        {
            return "BOOLEAN";
        }
        else if (type == Type.String
                && precision < 0)
        {
            return "TEXT";
        }
        else if (type == Type.Any)
        {
            return "TEXT";
        }
        else if (type == Type.DateTimeOffset)
        {
            return TIMESTAMPTZ;
        }
        return SqlDialect.super.getColumnDeclaration(type, scale, precision);
    }
}
