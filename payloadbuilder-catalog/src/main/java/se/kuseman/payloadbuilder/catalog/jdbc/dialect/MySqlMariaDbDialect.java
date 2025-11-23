package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.ZoneId;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Dialect for MariaDb/MySql. */
class MySqlMariaDbDialect implements SqlDialect
{
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Override
    public ColumnMeta getColumnMeta(ResultSetMetaData rsmd, int jdbcType, int ordinal) throws SQLException
    {
        ColumnMeta meta = SqlDialect.super.getColumnMeta(rsmd, jdbcType, ordinal);
        // LONGVARCHAR => precision -1
        if (meta.type() == Type.String
                && jdbcType == -1)
        {
            return new ColumnMeta(meta.type(), -1, meta.scale());
        }
        return meta;
    }

    @Override
    public String getColumnDeclaration(Type type, int scale, int precision)
    {
        if (type == Type.Float)
        {
            return "FLOAT";
        }
        else if (type == Type.Double)
        {
            return "DOUBLE";
        }
        else if (type == Type.DateTime)
        {
            return "DATETIME";
        }
        else if (type == Type.String)
        {
            return precision < 0 ? "LONGTEXT"
                    : "VARCHAR(%s)".formatted(precision);
        }
        else if (type == Type.Any)
        {
            return "LONGTEXT";
        }
        // These dialects doesn't have any support for offset timestamps
        // so use plain datetime with UTC
        else if (type == Type.DateTimeOffset)
        {
            return "DATETIME";
        }
        return SqlDialect.super.getColumnDeclaration(type, scale, precision);
    }

    @Override
    public void setStatementValue(Type type, PreparedStatement stm, int ordinal, int row, ValueVector vector) throws SQLException
    {
        if (vector.hasNulls()
                && vector.isNull(row))
        {
            stm.setObject(ordinal, null);
            return;
        }
        else if (type == Type.DateTimeOffset)
        {
            // Convert to UTC and the to LocaleDateTime since these dialects doesn't have offsets
            stm.setObject(ordinal, vector.getDateTimeOffset(row)
                    .getZonedDateTime()
                    .withZoneSameInstant(UTC)
                    .toLocalDateTime());
            return;
        }

        SqlDialect.super.setStatementValue(type, stm, ordinal, row, vector);
    }
}
