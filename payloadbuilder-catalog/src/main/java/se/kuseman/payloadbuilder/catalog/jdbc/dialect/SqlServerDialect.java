package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/** Dialect for SQL server */
class SqlServerDialect implements SqlDialect
{
    @Override
    public Connection createInsertConnection(String url, String username, String password) throws SQLException
    {
        // Enable bulk copy for insert connections
        url += ";useBulkCopyForBatchInsert=true";
        return SqlDialect.super.createInsertConnection(url, username, password);
    }

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
    public ColumnMeta getColumnMeta(ResultSetMetaData rsmd, int jdbcType, int ordinal) throws SQLException
    {
        ColumnMeta meta = SqlDialect.super.getColumnMeta(rsmd, jdbcType, ordinal);
        if (DATETIMEOFFSET == jdbcType)
        {
            return new ColumnMeta(Column.Type.DateTimeOffset, meta.precision(), meta.scale());
        }
        else if (meta.type() == Type.String
                && meta.precision() > 1_000_000_000)
        {
            return new ColumnMeta(meta.type(), -1, meta.scale());
        }
        return meta;
    }

    @Override
    public String getColumnDeclaration(Type type, int scale, int precision)
    {
        if (type == Type.DateTime)
        {
            return "DATETIME2";
        }
        else if (type == Type.DateTimeOffset)
        {
            return "DATETIMEOFFSET";
        }
        else if (type == Type.String)
        {
            return "NVARCHAR(" + (precision < 0 ? "MAX"
                    : precision)
                   + ")";
        }
        else if (type == Type.Any)
        {
            return "NVARCHAR(MAX)";
        }
        return SqlDialect.super.getColumnDeclaration(type, scale, precision);
    }

    @Override
    public String getDropTableStatement(QualifiedName qname, boolean lenient)
    {
        if (!lenient)
        {
            return SqlDialect.super.getDropTableStatement(qname, lenient);
        }
        // Use of format with OBJECT_ID for lenient mode to cover for old sqlserver versions
        return """
                IF OBJECT_ID(N'%1$s') IS NOT NULL
                BEGIN
                    DROP TABLE %1$s
                END
                """.formatted(qname.toDotDelimited());
    }
}
