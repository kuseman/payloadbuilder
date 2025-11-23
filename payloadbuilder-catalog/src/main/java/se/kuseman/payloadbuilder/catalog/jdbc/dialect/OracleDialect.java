package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Dialect for oracle RDBM's */
class OracleDialect implements SqlDialect
{
    private static final int TIMESTAMPTZ = -101;

    @Override
    public boolean usesSchemaAsDatabase()
    {
        return true;
    }

    @Override
    public String getIdentifierQuoteString(Connection connection) throws SQLException
    {
        // No quotes for Oracle since then all identifiers needs to be quoted
        return "";
    }

    @Override
    public ColumnMeta getColumnMeta(ResultSetMetaData rsmd, int jdbcType, int ordinal) throws SQLException
    {
        ColumnMeta meta = SqlDialect.super.getColumnMeta(rsmd, jdbcType, ordinal);

        if (TIMESTAMPTZ == jdbcType)
        {
            return new ColumnMeta(Type.DateTimeOffset, meta.precision(), meta.scale());
        }

        Column.Type type = meta.type();
        int scale = meta.scale();
        int precision = meta.precision();
        // Re-map the NUMBER type since that is used in multiple types in PLB
        if (meta.type() == Type.Decimal)
        {
            if (scale == 0)
            {
                if (precision <= 10)
                {
                    type = Type.Int;
                }
                else if (precision <= 19)
                {
                    type = Type.Long;
                }
            }
            else if (precision == 63)
            {
                type = Type.Float;
            }
            else if (precision == 126)
            {
                type = Type.Double;
            }
        }

        return new ColumnMeta(type, precision, scale);
    }

    @Override
    public String getColumnDeclaration(Type type, int scale, int precision)
    {
        if (type == Type.Int)
        {
            return "NUMBER(10)";
        }
        else if (type == Type.Long)
        {
            return "NUMBER(19)";
        }
        else if (type == Type.Boolean)
        {
            return "NUMBER(1)";
        }
        else if (type == Type.String)
        {
            return precision < 0 ? "CLOB"
                    : "VARCHAR2(" + precision + ")";
        }
        else if (type == Type.Any)
        {
            return "CLOB";
        }
        return SqlDialect.super.getColumnDeclaration(type, scale, precision);
    }

    @Override
    public void appendIndexJoinStatement(StringBuilder sb, ISeekPredicate indexPredicate, List<ISeekKey> seekKeys)
    {
        sb.append(" INNER JOIN (");

        int keySize = seekKeys.size();
        int rowCount = seekKeys.get(0)
                .getValue()
                .size();
        for (int i = 0; i < rowCount; i++)
        {
            if (i > 0)
            {
                sb.append(" UNION ALL ");
            }

            sb.append("SELECT ");

            for (int j = 0; j < keySize; j++)
            {
                ValueVector values = seekKeys.get(j)
                        .getValue();
                if (j > 0)
                {
                    sb.append(", ");
                }
                Object value = convert(values.valueAsObject(i));
                sb.append(value);
                if (i == 0)
                {
                    // Name columns on first row
                    sb.append(" ")
                            .append(indexPredicate.getIndexColumns()
                                    .get(j));
                }
            }

            sb.append(" FROM DUAL");
        }

        sb.append(") xx ON ");
        for (int i = 0; i < keySize; i++)
        {
            if (i > 0)
            {
                sb.append(" AND ");
            }

            String indexCol = indexPredicate.getIndexColumns()
                    .get(i);
            sb.append("xx.")
                    .append(indexCol)
                    .append(" = y.")
                    .append(indexCol);
        }
    }

    @Override
    public String getDropTableStatement(QualifiedName qname, boolean lenient)
    {
        if (lenient)
        {
            throw new IllegalArgumentException("Oracle doesn't support lenient drop of tables");
        }
        return SqlDialect.super.getDropTableStatement(qname, lenient);
    }
}
