package se.kuseman.payloadbuilder.core.test;

import java.util.List;

/**
 * Definition of a test harness.
 *
 * <pre>
 * Works by setting a test harness containng
 * a set of catalog with their
 * </pre>
 **/
class TestHarness
{
    private String name;
    private List<TestCatalog> catalogs;
    private List<TestCase> cases;

    String getName()
    {
        return name;
    }

    void setName(String name)
    {
        this.name = name;
    }

    List<TestCatalog> getCatalogs()
    {
        return catalogs;
    }

    void setCatalogs(List<TestCatalog> catalogs)
    {
        this.catalogs = catalogs;
    }

    List<TestCase> getCases()
    {
        return cases;
    }

    void setCases(List<TestCase> cases)
    {
        this.cases = cases;
    }
}
