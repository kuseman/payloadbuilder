package se.kuseman.payloadbuilder.catalog.jdbc.dialect;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.utils.MapUtils;

/** Definition of a sql dialect. This is used to even out oddities for RDBM's where plain JDBC/ANSI SQL doesn't work */
public interface SqlDialect
{

    // CSOFF
    /*
     * @formatter:off
     *  Java Type            JDBC               Oracle                  PostgreSQL                 MySQL / MariaDB         Microsoft SQL Server         IBM DB2
     *  byte (8-bit signed)  TINYINT            NUMBER(3)               SMALLINT (or SMALLINT)     TINYINT                 TINYINT (0-255)              SMALLINT
     *  short (16-bit)       SMALLINT           NUMBER(5)               SMALLINT                   SMALLINT                SMALLINT                     SMALLINT
     *  int (32-bit)         INTEGER            NUMBER(10)              INTEGER                    INT / INTEGER           INT                          INTEGER
     *  long (64-bit)        BIGINT             NUMBER(19)              BIGINT                     BIGINT                  BIGINT                       BIGINT
     *  float (32-bit IEEE)  REAL               BINARY_FLOAT or NUMBER  REAL / FLOAT4              FLOAT                   FLOAT / REAL                 REAL
     *  double (64-bit IEEE) DOUBLE             BINARY_DOUBLE or NUMBER DOUBLE PRECISION / FLOAT8  DOUBLE / FLOAT(53)      FLOAT (or DOUBLE PRECISION)  DOUBLE / FLOAT
     *  BigDecimal           DECIMAL or NUMERIC NUMBER(p,s)             NUMERIC(p,s) / DECIMAL     DECIMAL(p,s)            DECIMAL(p,s) or NUMERIC(p,s) DECIMAL(p,s) / NUMERIC(p,s)
     *  BigInteger           DECIMAL / NUMERIC  NUMBER(p,0)             NUMERIC(p) (scale 0)       DECIMAL(p,0)            DECIMAL(p,0)                 DECIMAL(p,0)
     *  boolean              BOOLEAN / BIT      NUMBER(1) or CHAR(1)    BOOLEAN                    BOOLEAN or TINYINT(1)   BIT or BOOLEAN               (often SMALLINT or CHAR(1))
     *
     *  Java Type               JDBC /java.sql.Types             Oracle                                                     PostgreSQL                          MySQL / MariaDB                                             SQL Server                  IBM DB2
     *  LocalDate               DATE                             DATE                                                       DATE                                DATE                                                        DATE                        DATE
     *  LocalTime               TIME                             DATE* (no pure TIME pre-21c → usually DATE or TIMESTAMP)   TIME                                TIME                                                        TIME                        TIME
     *  LocalDateTime           TIMESTAMP                        TIMESTAMP                                                  TIMESTAMP WITHOUT TIME ZONE         DATETIME or TIMESTAMP                                       DATETIME2                   TIMESTAMP
     *  Instant                 TIMESTAMP                        TIMESTAMP WITH LOCAL TIME ZONE                             TIMESTAMP WITH TIME ZONE            TIMESTAMP (no TZ; store UTC manually)                       DATETIME2 (no TZ metadata)  TIMESTAMP
     *  OffsetDateTime          TIMESTAMP_WITH_TIMEZONE          TIMESTAMP WITH TIME ZONE                                   TIMESTAMP WITH TIME ZONE            No true offset type → store as TIMESTAMP + offset string    DATETIMEOFFSET              No true type → use TIMESTAMP + offset
     *  ZonedDateTime           usually TIMESTAMP_WITH_TIMEZONE  Store as TIMESTAMP WITH TIME ZONE                          Store as TIMESTAMP WITH TIME ZONE   No true mapping → TIMESTAMP + zone ID                       DATETIMEOFFSET              TIMESTAMP + zone manually
     *  java.sql.Date           DATE                             DATE                                                       DATE                                DATE                                                        DATE                        DATE
     *  java.sql.Time           TIME                             DATE*                                                      TIME                                TIME                                                        TIME                        TIME
     *  java.sql.Timestamp      TIMESTAMP                        TIMESTAMP                                                  TIMESTAMP                           TIMESTAMP or DATETIME                                       DATETIME2                   TIMESTAMP
     * @formatter:on
     */
    // CSON

    //@formatter:off
    static final Map<Integer, Column.Type> JDBC_TO_PLB_MAP = MapUtils.ofEntries(
            // Boolean
            MapUtils.entry(java.sql.Types.BIT, Column.Type.Boolean),
            MapUtils.entry(java.sql.Types.BOOLEAN, Column.Type.Boolean),
            // Int
            MapUtils.entry(java.sql.Types.TINYINT, Column.Type.Int),
            MapUtils.entry(java.sql.Types.SMALLINT, Column.Type.Int),
            MapUtils.entry(java.sql.Types.INTEGER, Column.Type.Int),
            // Long
            MapUtils.entry(java.sql.Types.BIGINT, Column.Type.Long),
            // Float
            MapUtils.entry(java.sql.Types.REAL, Column.Type.Float),
            // Double
            MapUtils.entry(java.sql.Types.DOUBLE, Column.Type.Double),
            MapUtils.entry(java.sql.Types.FLOAT, Column.Type.Double),
            // Decimal
            MapUtils.entry(java.sql.Types.DECIMAL, Column.Type.Decimal),
            MapUtils.entry(java.sql.Types.NUMERIC, Column.Type.Decimal),
            // String
            MapUtils.entry(java.sql.Types.CHAR, Column.Type.String),
            MapUtils.entry(java.sql.Types.VARCHAR, Column.Type.String),
            MapUtils.entry(java.sql.Types.LONGVARCHAR, Column.Type.String),
            MapUtils.entry(java.sql.Types.NCHAR, Column.Type.String),
            MapUtils.entry(java.sql.Types.NVARCHAR, Column.Type.String),
            MapUtils.entry(java.sql.Types.LONGNVARCHAR, Column.Type.String),
            MapUtils.entry(java.sql.Types.CLOB, Column.Type.String),
            MapUtils.entry(java.sql.Types.NCLOB, Column.Type.String),
            // DateTime
            MapUtils.entry(java.sql.Types.TIMESTAMP, Column.Type.DateTime),
            MapUtils.entry(java.sql.Types.DATE, Column.Type.DateTime),
            // DateTimeOffset
            MapUtils.entry(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, Column.Type.DateTimeOffset)
            );
    //@formatter:on

    /**
     * Create connection with provided url and credentials. This connected is intended to be used when inserting data via {@link InsertSink} and if dialect supports special connections for fast
     * inserts it can be applied.
     */
    default Connection createInsertConnection(String url, String username, String password) throws SQLException
    {
        return DriverManager.getConnection(url, username, password);
    }

    /** Returns true if this dialect uses JDBC schemas as database */
    default boolean usesSchemaAsDatabase()
    {
        return false;
    }

    /** Returns the identifier quote string for this dialect. */
    default String getIdentifierQuoteString(Connection connection) throws SQLException
    {
        return connection.getMetaData()
                .getIdentifierQuoteString();
    }

    /** Jdbc meta for a column. */
    record ColumnMeta(Column.Type type, int precision, int scale)
    {
        public ColumnMeta
        {
            requireNonNull(type);
        }
    }

    /**
     * Retuns PLB column type from provided JDBC meta data.
     */
    default ColumnMeta getColumnMeta(ResultSetMetaData rsmd, int jdbcType, int ordinal) throws SQLException
    {
        int scale = rsmd.getScale(ordinal);
        int precision = rsmd.getPrecision(ordinal);
        Column.Type type = JDBC_TO_PLB_MAP.get(jdbcType);
        if (type == null)
        {
            type = Column.Type.Any;
        }
        return new ColumnMeta(type, precision, scale);
    }

    /** Set result set value to provided value vector. */
    default void setResultSetValue(Column.Type type, ResultSet rs, int ordinal, int row, int jdbcType, MutableValueVector vector) throws SQLException, IOException
    {
        if (type == Type.Any)
        {
            vector.setAny(row, rs.getObject(ordinal));
            return;
        }

        if (type == Type.Boolean)
        {
            vector.setBoolean(row, rs.getBoolean(ordinal));
        }
        else if (type == Type.Int)
        {
            vector.setInt(row, rs.getInt(ordinal));
        }
        else if (type == Type.Long)
        {
            vector.setLong(row, rs.getLong(ordinal));
        }
        else if (type == Type.Float)
        {
            vector.setFloat(row, rs.getFloat(ordinal));
        }
        else if (type == Type.Double)
        {
            vector.setDouble(row, rs.getDouble(ordinal));
        }
        else if (type == Type.String)
        {
            if (jdbcType == java.sql.Types.CLOB
                    || jdbcType == java.sql.Types.NCLOB)
            {
                Reader reader = rs.getCharacterStream(ordinal);
                if (reader != null)
                {
                    vector.setString(row, UTF8String.from(IOUtils.toString(reader)));
                }
            }
            else
            {
                vector.setString(row, UTF8String.from(rs.getString(ordinal)));
            }
        }
        else if (type == Type.DateTime)
        {
            LocalDateTime obj = rs.getObject(ordinal, LocalDateTime.class);
            if (obj != null)
            {
                vector.setDateTime(row, EpochDateTime.from(obj));
            }
        }
        else if (type == Type.DateTimeOffset)
        {
            OffsetDateTime obj = rs.getObject(ordinal, OffsetDateTime.class);
            if (obj != null)
            {
                vector.setDateTimeOffset(row, EpochDateTimeOffset.from(obj));
            }
        }
        else if (type == Type.Decimal)
        {
            Object obj = rs.getObject(ordinal);
            if (obj != null)
            {
                vector.setDecimal(row, Decimal.from(obj));
            }
        }
        // NO ELSE-IF
        if (rs.wasNull())
        {
            vector.setNull(row);
        }
    }

    /**
     * Set value into {@link PreparedStatement} object.
     */
    default void setStatementValue(Column.Type type, PreparedStatement stm, int ordinal, int row, ValueVector vector) throws SQLException
    {
        if (vector.hasNulls()
                && vector.isNull(row))
        {
            stm.setObject(ordinal, null);
            return;
        }

        switch (type)
        {
            case String:
            case Any:
                stm.setString(ordinal, vector.valueAsString(row));
                break;
            case Boolean:
                stm.setBoolean(ordinal, vector.getBoolean(row));
                break;
            case DateTime:
                stm.setObject(ordinal, vector.getDateTime(row)
                        .getLocalDateTime());
                break;
            case DateTimeOffset:
                stm.setObject(ordinal, vector.getDateTimeOffset(row)
                        .getZonedDateTime()
                        .toOffsetDateTime());
                break;
            case Decimal:
                stm.setBigDecimal(ordinal, vector.getDecimal(row)
                        .asBigDecimal());
                break;
            case Double:
                stm.setDouble(ordinal, vector.getDouble(row));
                break;
            case Float:
                stm.setFloat(ordinal, vector.getFloat(row));
                break;
            case Int:
                stm.setInt(ordinal, vector.getInt(row));
                break;
            case Long:
                stm.setLong(ordinal, vector.getLong(row));
                break;
            default:
                throw new IllegalArgumentException("Type: " + type + " is not supported.");
        }
    }

    /** Return column definition for provided column. */
    default String getColumnDeclaration(Type type, int scale, int precision)
    {
        return switch (type)
        {
            case Boolean -> "BIT";
            case Int -> "INT";
            case Long -> "BIGINT";
            case Float -> "REAL";
            case Double -> "FLOAT";
            case Decimal -> "NUMERIC(%s, %s)".formatted(precision < 0 ? 19
                    : precision,
                    scale < 0 ? 4
                            : scale);
            case DateTime -> "TIMESTAMP";
            case DateTimeOffset -> "TIMESTAMP WITH TIME ZONE";
            case String -> "VARCHAR(%s)".formatted(precision < 0 ? "MAX"
                    : precision);
            default -> throw new IllegalArgumentException("Type: " + type + " is not supported.");
        };
    }

    /** Return drop table statement for provided table. */
    default String getDropTableStatement(QualifiedName qname, boolean lenient)
    {
        return (lenient ? "DROP TABLE IF EXISTS %"
                : "DROP TABLE %s").formatted(qname.toDotDelimited());
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
}
