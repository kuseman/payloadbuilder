package se.kuseman.payloadbuilder.catalog.jdbc;

import javax.sql.DataSource;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;

abstract class ASqlServerTest extends BaseJDBCTest
{
    ASqlServerTest(DataSource datasource, String jdbcUrl, String driverClassName, String username, String password)
    {
        super(datasource, jdbcUrl, driverClassName, username, password);
    }

    @Override
    protected String getColumnDeclaration(Column column)
    {
        Type type = column.getType()
                .getType();
        if (type == Type.DateTime)
        {
            return "DATETIME";
        }
        return super.getColumnDeclaration(column);
    }

    @Override
    protected Column getColumn(Type type, String name, int precision, int scale)
    {
        if (type == Type.DateTime)
        {
            precision = 23;
            scale = 3;
        }
        return super.getColumn(type, name, precision, scale);
    }
}
