package se.kuseman.payloadbuilder.catalog.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.SqlDialect;

/** Jdbc utils. Managing resources etc. */
class JdbcUtils
{
    static Schema getSchemaFromResultSet(SqlDialect dialect, ResultSetMetaData rsmd) throws SQLException
    {
        int count = rsmd.getColumnCount();
        List<Column> columns = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            Column.Type type = dialect.getColumnType(rsmd, i + 1);
            columns.add(Column.of(rsmd.getColumnLabel(i + 1), type));
        }
        return new Schema(columns);
    }

    /** Sets result set values into vectors. */
    static void setVectorValues(SqlDialect dialect, int row, ResultSet rs, Schema schema, List<MutableValueVector> vectors) throws SQLException, IOException
    {
        int count = schema.getSize();
        for (int i = 0; i < count; i++)
        {
            Column.Type type = schema.getColumns()
                    .get(i)
                    .getType()
                    .getType();
            MutableValueVector vector = vectors.get(i);

            int ordinal = i + 1;
            dialect.setResultSetValue(type, rs, ordinal, row, vector);
        }
    }

    /**
     * Converts provided value according to jdbc type. Stringifies LOBs etc.
     */
    static Object getAndConvertValue(ResultSet rs, int ordinal, int jdbcType) throws SQLException, IOException
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
