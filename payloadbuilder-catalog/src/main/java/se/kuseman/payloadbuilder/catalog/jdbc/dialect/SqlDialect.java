package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import java.io.Reader;
import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.io.IOUtils;

import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Definition of a sql dialect. This is used to even out oddities for RDBM's where plain JDBC/ANSI SQL doesn't work */
public interface SqlDialect
{
    /** Returns true if this dialect uses JDBC schemas as database */
    default boolean usesSchemaAsDatabase()
    {
        return false;
    }

    /**
     * Appends join statement for provided seek keys.
     * 
     * <pre>
     * This is the SQL that we need
     *
     *  INNER JOIN
     * (
     *         SELECT 1 col1, 2 col2
     *   UNION SELECT 2,      4
     *   UNION SELECT 3,      6
     * 
     * ) xx
     *   ON xx.col1 = y.col1
     *   AND xx.col2 = y.col2
     * </pre>
     */
    default void appendIndexJoinStatement(StringBuilder sb, ISeekPredicate indexPredicate, List<ISeekPredicate.ISeekKey> seekKeys)
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

    /** Convert provided value regarding booleans, quoting of strings etc. */
    default Object convert(Object value)
    {
        if (value instanceof Boolean)
        {
            value = (Boolean) value ? 1
                    : 0;
        }

        if (!(value instanceof Number))
        {
            return "'" + String.valueOf(value) + "'";
        }

        return value;
    }

    /**
     * Returns a value for a result set ordinal.
     */
    default Object getJdbcValue(ResultSet rs, int ordinal, int jdbcType) throws Exception
    {
        if (jdbcType == java.sql.Types.CLOB)
        {
            Reader reader = rs.getCharacterStream(ordinal);
            if (rs.wasNull())
            {
                return null;
            }
            return IOUtils.toString(reader);
        }

        return rs.getObject(ordinal);
    }
}
