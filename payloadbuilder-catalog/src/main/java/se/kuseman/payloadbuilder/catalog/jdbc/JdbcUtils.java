package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.Strings;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.catalog.jdbc.JdbcCatalog.ColumnOption;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.SqlDialect;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.SqlDialect.ColumnMeta;

/** Jdbc utils. Managing resources etc. */
final class JdbcUtils
{
    private JdbcUtils()
    {
    }

    static final String JAVA_SQL_TYPE = "java.sql.Type";

    record SchemaResult(Schema schema, int[] jdbcTypes)
    {
        SchemaResult
        {
            requireNonNull(schema);
            requireNonNull(jdbcTypes);
        }
    }

    /** Return a create table statement for provided schema. */
    static String getCreateTableStatement(SqlDialect dialect, QualifiedName table, Schema schema, String identifierQuoteString, Map<String, ColumnOption> columnOptions)
    {
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table.getParts()
                .stream()
                .map(p -> "%1$s%2$s%1$s".formatted(identifierQuoteString, p))
                .collect(joining(".")))
                .append("\n(\n");
        sb.append(schema.getColumns()
                .stream()
                .map(c ->
                {
                    Column.Type type = c.getType()
                            .getType();
                    String name = c.getName();

                    int precision = c.getMetaData()
                            .getPrecision();
                    int scale = c.getMetaData()
                            .getScale();
                    boolean nullable = c.getMetaData()
                            .isNullable();

                    String columnDeclaration;
                    ColumnOption option = columnOptions.get(name.toLowerCase());
                    IExpression valueExp;
                    if (option != null
                            && (valueExp = (option.values()
                                    .get(JdbcCatalog.DECLARATION))) != null)
                    {
                        columnDeclaration = valueExp.eval(null)
                                .valueAsString(0);
                    }
                    else
                    {
                        columnDeclaration = dialect.getColumnDeclaration(type, scale, precision);
                    }

                    if (!nullable)
                    {
                        columnDeclaration += " NOT NULL";
                    }

                    return "%1$s%2$s%1$s\t%3$s".formatted(identifierQuoteString, name, columnDeclaration);
                })
                .collect(joining(",\n")));
        sb.append("\n)\n");
        return sb.toString();
    }

    static SchemaResult getSchemaFromResultSet(SqlDialect dialect, ResultSetMetaData rsmd, Map<String, ColumnOption> columnOptions) throws SQLException
    {
        int count = rsmd.getColumnCount();
        int[] jdbcTypes = new int[count];
        List<Column> columns = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            int ordinal = i + 1;
            String columnName = rsmd.getColumnLabel(ordinal);
            jdbcTypes[i] = rsmd.getColumnType(ordinal);
            Column.Type type = null;
            int precision = -1;
            int scale = -1;
            IExpression valueExp;

            // Hint of PLB type
            ColumnOption columnOption = columnOptions.get(columnName.toLowerCase());
            if (columnOption != null
                    && (valueExp = columnOption.values()
                            .get(JdbcCatalog.PLBTYPE)) != null)
            {
                String value = valueExp.eval(null)
                        .valueAsString(0);
                if (value != null)
                {
                    type = Arrays.stream(Column.Type.values())
                            .filter(e -> Strings.CI.equals(e.name(), value))
                            .findAny()
                            .orElse(null);
                }
            }

            if (type == null)
            {
                ColumnMeta meta = dialect.getColumnMeta(rsmd, jdbcTypes[i], ordinal);
                type = meta.type();
                precision = meta.precision();
                scale = meta.scale();
            }

            //@formatter:off
            Column.MetaData metaData = new Column.MetaData(Map.of(
                    Column.MetaData.NULLABLE, rsmd.isNullable(ordinal) == ResultSetMetaData.columnNullable,
                    Column.MetaData.SCALE, scale,
                    Column.MetaData.PRECISION, precision
                    //JAVA_SQL_TYPE, rsmd.getColumnType(ordinal)
                    ));
            //@formatter:on
            columns.add(new Column(rsmd.getColumnLabel(ordinal), ResolvedType.of(type), metaData));
        }
        return new SchemaResult(new Schema(columns), jdbcTypes);
    }

    /** Sets result set values into vectors. */
    static void setVectorValues(SqlDialect dialect, int row, ResultSet rs, SchemaResult schemaResult, List<MutableValueVector> vectors) throws SQLException, IOException
    {
        int count = schemaResult.schema.getSize();
        for (int i = 0; i < count; i++)
        {
            Column.Type type = schemaResult.schema.getColumns()
                    .get(i)
                    .getType()
                    .getType();
            MutableValueVector vector = vectors.get(i);

            int ordinal = i + 1;
            int jdbcType = schemaResult.jdbcTypes[i];
            dialect.setResultSetValue(type, rs, ordinal, row, jdbcType, vector);
        }
    }

    static void cancelQuiet(Statement statement)
    {
        if (statement != null)
        {
            try
            {
                statement.cancel();
            }
            catch (SQLException e)
            {
            }
        }
    }

    static void rollbackQuiet(Connection connection)
    {
        if (connection != null)
        {
            try
            {
                connection.rollback();
            }
            catch (SQLException e)
            {
            }
        }
    }

    /** Close connection, statement and result set quietly */
    static void closeQuiet(Connection conn, Statement statement, ResultSet rs)
    {
        try
        {
            closeQuiet(rs);
        }
        finally
        {
            try
            {
                closeQuiet(statement);
            }
            finally
            {
                closeQuiet(conn);
            }
        }
    }

    static void closeQuiet(Connection connection)
    {
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
            }
        }
    }

    static void closeQuiet(Statement statement)
    {
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (SQLException e)
            {
            }
        }
    }

    static void closeQuiet(ResultSet resultset)
    {
        if (resultset != null)
        {
            try
            {
                resultset.close();
            }
            catch (SQLException e)
            {
            }
        }
    }

    static void printWarnings(Connection connection, Writer messageWriter)
    {
        if (connection == null)
        {
            return;
        }
        try
        {
            printWarnings(connection.getWarnings(), messageWriter);
            connection.clearWarnings();
        }
        catch (SQLException e)
        {
        }
    }

    static void printWarnings(Statement statement, Writer messageWriter)
    {
        if (statement == null)
        {
            return;
        }
        try
        {
            printWarnings(statement.getWarnings(), messageWriter);
            statement.clearWarnings();
        }
        catch (SQLException e)
        {
        }
    }

    static void printWarnings(ResultSet resultSet, Writer messageWriter)
    {
        if (resultSet == null)
        {
            return;
        }
        try
        {
            printWarnings(resultSet.getWarnings(), messageWriter);
            resultSet.clearWarnings();
        }
        catch (SQLException e)
        {
        }
    }

    private static void printWarnings(SQLWarning warning, Writer messageWriter)
    {
        while (warning != null)
        {
            try
            {
                messageWriter.append(warning.getMessage())
                        .append(System.lineSeparator());
            }
            catch (IOException e)
            {
            }
            warning = warning.getNextWarning();
        }
    }

    static ResultSet getNextResultSet(Consumer<SQLException> exceptionHandler, Writer messageWriter, Statement statement, String query, boolean first) throws Exception
    {
        // Skip a while loop here to protect against bugs/bad drivers etc.
        // Traverse until we have a result set or there are no more result sets
        for (int iteration = 0; iteration < 256; iteration++)
        {
            try
            {
                boolean isResultSet;
                if (first)
                {
                    if (statement instanceof PreparedStatement)
                    {
                        isResultSet = ((PreparedStatement) statement).execute();
                    }
                    else
                    {
                        isResultSet = statement.execute(query);
                    }
                }
                else
                {
                    isResultSet = statement.getMoreResults();
                }

                first = false;
                JdbcUtils.printWarnings(statement, messageWriter);
                if (isResultSet)
                {
                    ResultSet rs = statement.getResultSet();
                    JdbcUtils.printWarnings(statement, messageWriter);
                    return rs;
                }
                else
                {
                    int updateCount = statement.getUpdateCount();
                    // We're done
                    if (updateCount < 0)
                    {
                        return null;
                    }

                    messageWriter.append(String.valueOf(updateCount))
                            .append(" row(s) affected")
                            .append(System.lineSeparator());
                }
            }
            catch (SQLException e)
            {
                exceptionHandler.accept(e);
            }
            finally
            {
                first = false;
            }
        }

        throw new RuntimeException("Max iteration count reached when trying to fetch a result set.");
    }
}
