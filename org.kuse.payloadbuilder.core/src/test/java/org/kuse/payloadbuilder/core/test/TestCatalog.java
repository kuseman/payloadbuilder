package org.kuse.payloadbuilder.core.test;

import java.util.List;

/** Harness catalog */
class TestCatalog
{
    private String alias;
    private List<TestTable> tables;

    String getAlias()
    {
        return alias;
    }

    void setAlias(String alias)
    {
        this.alias = alias;
    }

    List<TestTable> getTables()
    {
        return tables;
    }

    void setTables(List<TestTable> tables)
    {
        this.tables = tables;
    }
}
