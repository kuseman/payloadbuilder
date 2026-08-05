package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.test.IPredicateMock;

/**
 * Unit tests of {@link MongoDatasource#getDescribeProperties(IExecutionContext)}.
 *
 * <pre>
 * NOTE! Describing never touches Mongo/the network - it only evaluates predicate value expressions (mocked here) and builds the equivalent Bson,
 * so these run without Docker.
 * </pre>
 */
class MongoDatasourceTest
{
    private static final String CATALOG_ALIAS = "mongo";

    @Test
    void test_describe_before_execution_shows_catalog_predicate_sort_projection_and_query()
    {
        IPredicate predicate = IPredicateMock.eq("key", 123);
        when(predicate.getSqlRepresentation()).thenReturn("key = 123");
        List<IPredicate> predicates = new ArrayList<>(List.of(predicate));

        MongoDatasource ds = new MongoDatasource(0, new MongoClientHolder(), CATALOG_ALIAS, new MongoTable("db", "coll"), null, predicates, Sorts.ascending("key"), Projections.include("key"),
                List.of("key"), emptyList());

        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, emptyMap(), 0, null);

        Map<String, Object> props = ds.getDescribeProperties(context);

        assertEquals("MongoCatalog", props.get(IDatasource.CATALOG));
        assertEquals("key = 123", props.get(IDatasource.PREDICATE));
        assertTrue(((String) props.get("Sort")).contains("key"));
        assertTrue(((String) props.get("Projection")).contains("key"));
        assertTrue(((String) props.get("Query")).contains("123"));
        assertNull(props.get("Document count"));
    }

    @Test
    void test_describe_without_predicate_sort_or_projection()
    {
        MongoDatasource ds = new MongoDatasource(0, new MongoClientHolder(), CATALOG_ALIAS, new MongoTable("db", "coll"), null, new ArrayList<>(), null, null, null, emptyList());

        IExecutionContext context = mockExecutionContext(CATALOG_ALIAS, emptyMap(), 0, null);

        Map<String, Object> props = ds.getDescribeProperties(context);

        assertEquals("", props.get(IDatasource.PREDICATE));
        assertEquals("", props.get("Sort"));
        assertEquals("*", props.get("Projection"));
        assertEquals("{}", props.get("Query"));
    }
}
