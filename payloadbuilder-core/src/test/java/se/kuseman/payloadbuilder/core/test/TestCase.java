package se.kuseman.payloadbuilder.core.test;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.test.AObjectOutputWriter.ColumnValue;

/** Domain of a test case inside a {@link TestHarness} */
class TestCase
{
    private String name;
    private String query;
    private boolean ignore;
    /** Filter for only run on schema or shema less */
    private Boolean schemaLess;
    /** Filter for only run with typed vectors or not */
    private Boolean typedVectors;

    private boolean onlyAssertExpectedColumns;

    private List<List<List<ColumnValue>>> expectedResultSets;
    private Class<? extends Exception> expectedException;
    private String expectedMessageContains;

    @JsonDeserialize(
            using = SchemasDeserializer.class)
    private List<Schema> expectedRuntimeSchemasTypedVectors = emptyList();
    @JsonDeserialize(
            using = SchemasDeserializer.class)
    private List<Schema> expectedRuntimeSchemasAnyVectors = emptyList();

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
            this.query = ((Collection<Object>) query).stream()
                    .map(Object::toString)
                    .collect(joining(System.lineSeparator()));
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

    Boolean getSchemaLess()
    {
        return schemaLess;
    }

    void setSchemaLess(Boolean schemaLess)
    {
        this.schemaLess = schemaLess;
    }

    Boolean getTypedVectors()
    {
        return typedVectors;
    }

    void setTypedVectors(Boolean typedVectors)
    {
        this.typedVectors = typedVectors;
    }

    boolean isOnlyAssertExpectedColumns()
    {
        return onlyAssertExpectedColumns;
    }

    void setOnlyAssertExpectedColumns(boolean onlyAssertExpectedColumns)
    {
        this.onlyAssertExpectedColumns = onlyAssertExpectedColumns;
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

    List<Schema> getExpectedRuntimeSchemasAnyVectors()
    {
        return expectedRuntimeSchemasAnyVectors;
    }

    void setExpectedRuntimeSchemasAnyVectors(List<Schema> expectedRuntimeSchemasAnyVectors)
    {
        this.expectedRuntimeSchemasAnyVectors = expectedRuntimeSchemasAnyVectors;
    }

    List<Schema> getExpectedRuntimeSchemasTypedVectors()
    {
        return expectedRuntimeSchemasTypedVectors;
    }

    void setExpectedRuntimeSchemasTypedVectors(List<Schema> expectedRuntimeSchemasTypedVectors)
    {
        this.expectedRuntimeSchemasTypedVectors = expectedRuntimeSchemasTypedVectors;
    }

    static class SchemasDeserializer extends JsonDeserializer<List<Schema>>
    {
        @SuppressWarnings("unchecked")
        @Override
        public List<Schema> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException
        {
            /*
             * @formatter:off
             * "expectedResultSchemasSchemaLess": [
             *    { 
             *    "schema": [
             *    ] } 
             *  ]
             * @formatter:on
             */
            List<Map<String, Object>> schemas = p.readValueAs(new TypeReference<List<Map<String, Object>>>()
            {
            });
            return schemas.stream()
                    .map(m -> TestTable.ResolvedTypeDeserializer.schemaFrom((List<Map<String, Object>>) m.get("schema")))
                    .toList();
        }
    }
}
