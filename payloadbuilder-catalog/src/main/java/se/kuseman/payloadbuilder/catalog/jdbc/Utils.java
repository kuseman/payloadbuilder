package se.kuseman.payloadbuilder.catalog.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Jdbc utils */
class Utils
{
    /** Close connection and result set quietly */
    static void closeQuiet(Connection connection, ResultSet resultSet)
    {
        if (resultSet != null)
        {
            try
            {
                resultSet.close();
            }
            catch (SQLException e)
            {
                throw new RuntimeException("Error closing SQL result set", e);
            }
        }
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                throw new RuntimeException("Error closing SQL connection", e);
            }
        }
    }
}
