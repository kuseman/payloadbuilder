package se.kuseman.payloadbuilder.catalog.jdbc;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.function.Consumer;

/** Jdbc utils. Managing resources etc. */
class JdbcUtils
{
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
