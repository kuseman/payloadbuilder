package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Dialect for oracle RDBM's */
class OracleDialect implements SqlDialect
{
    @Override
    public boolean usesSchemaAsDatabase()
    {
        return true;
    }

    @Override
    public Type getColumnType(ResultSetMetaData rsmd, int ordinal) throws SQLException
    {
        Column.Type type = SqlDialect.super.getColumnType(rsmd, ordinal);
        // Re-map to Integer for Number types with integer scale/precision
        if (type == Type.Decimal
                && rsmd.getScale(ordinal) == 0
                && rsmd.getPrecision(ordinal) <= 38)
        {
            type = Type.Int;
        }

        return type;
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
                sb.append(" UNION ");
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
}
