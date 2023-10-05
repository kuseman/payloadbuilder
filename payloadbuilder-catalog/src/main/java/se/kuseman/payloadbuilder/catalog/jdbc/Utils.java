package se.kuseman.payloadbuilder.catalog.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.commons.io.IOUtils;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;

/** Jdbc utils */
class Utils
{
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

    static void printWarnings(Connection connection, IQuerySession session)
    {
        if (connection == null)
        {
            return;
        }
        try
        {
            printWarnings(connection.getWarnings(), session);
            connection.clearWarnings();
        }
        catch (SQLException e)
        {
        }
    }

    static void printWarnings(Statement statement, IQuerySession session)
    {
        if (statement == null)
        {
            return;
        }
        try
        {
            printWarnings(statement.getWarnings(), session);
            statement.clearWarnings();
        }
        catch (SQLException e)
        {
        }
    }

    static void printWarnings(ResultSet resultSet, IQuerySession session)
    {
        if (resultSet == null)
        {
            return;
        }
        try
        {
            printWarnings(resultSet.getWarnings(), session);
            resultSet.clearWarnings();
        }
        catch (SQLException e)
        {
        }
    }

    private static void printWarnings(SQLWarning warning, IQuerySession session)
    {
        Writer writer = session.getPrintWriter();
        while (warning != null)
        {
            try
            {
                writer.append(warning.getMessage())
                        .append(System.lineSeparator());
            }
            catch (IOException e)
            {
            }
            warning = warning.getNextWarning();
        }
    }
}
