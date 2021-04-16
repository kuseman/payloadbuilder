package org.kuse.payloadbuilder.core.test;

import static java.util.stream.Collectors.joining;

import java.util.Collection;
import java.util.List;

import org.kuse.payloadbuilder.core.operator.AObjectOutputWriter.ColumnValue;

/** Domain of a test case inside a {@link TestHarness} */
class TestCase
{
    private String name;
    private String query;
    private boolean ignore;
    private List<List<List<ColumnValue>>> expectedResultSets;
    private Class<? extends Exception> expectedException;
    private String expectedMessageContains;

    String getName()
    {
        return name;
    }

    void setName(String name)
    {
        this.name = name;
    }

    String getQuery()
    {
        return query;
    }

    @SuppressWarnings("unchecked")
    void setQuery(Object query)
    {
        if (query instanceof String)
        {
            this.query = (String) query;
        }
        else if (query instanceof Collection)
        {
            this.query = ((Collection<Object>) query).stream().map(Object::toString).collect(joining(System.lineSeparator()));
        }
    }

    boolean isIgnore()
    {
        return ignore;
    }

    void setIgnore(boolean ignore)
    {
        this.ignore = ignore;
    }

    List<List<List<ColumnValue>>> getExpectedResultSets()
    {
        return expectedResultSets;
    }

    void setExpectedResultSets(List<List<List<ColumnValue>>> expectedResultSets)
    {
        this.expectedResultSets = expectedResultSets;
    }

    void setExpectedException(Class<? extends Exception> expectedException)
    {
        this.expectedException = expectedException;
    }

    Class<? extends Exception> getExpectedException()
    {
        return expectedException;
    }

    String getExpectedMessageContains()
    {
        return expectedMessageContains;
    }

    void setExpectedMessageContains(String expectedMessageContains)
    {
        this.expectedMessageContains = expectedMessageContains;
    }
}
