package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;

import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.test.IPredicateMock;

/** Unit tests of {@link MongoFilterBuilder}. */
class MongoFilterBuilderTest
{
    private final IExecutionContext context = mockExecutionContext("mongo", emptyMap(), 0, null);

    @Test
    void test_equal()
    {
        assertFilter(Filters.eq("col", "val"), IPredicateMock.eq("col", "val"));
    }

    @Test
    void test_not_equal()
    {
        assertFilter(Filters.ne("col", "val"), IPredicateMock.neq("col", "val"));
    }

    @Test
    void test_greater_than()
    {
        assertFilter(Filters.gt("col", 10), IPredicateMock.gt("col", 10));
    }

    @Test
    void test_greater_than_equal()
    {
        assertFilter(Filters.gte("col", 10), IPredicateMock.gte("col", 10));
    }

    @Test
    void test_less_than()
    {
        assertFilter(Filters.lt("col", 10), IPredicateMock.lt("col", 10));
    }

    @Test
    void test_less_than_equal()
    {
        assertFilter(Filters.lte("col", 10), IPredicateMock.lte("col", 10));
    }

    @Test
    void test_comparison_against_null_never_matches()
    {
        Bson filter = build(IPredicateMock.eq("col", null));
        // A field can never both exist and not exist
        assertFilter(Filters.and(Filters.exists("_id", true), Filters.exists("_id", false)), filter);
    }

    @Test
    void test_id_equal_converts_valid_hex_string_to_ObjectId()
    {
        String hex = "507f1f77bcf86cd799439011";
        assertFilter(Filters.eq("_id", new ObjectId(hex)), IPredicateMock.eq("_id", hex));
    }

    @Test
    void test_id_equal_leaves_non_hex_string_as_is()
    {
        assertFilter(Filters.eq("_id", "not-a-valid-object-id"), IPredicateMock.eq("_id", "not-a-valid-object-id"));
    }

    @Test
    void test_in()
    {
        assertFilter(Filters.in("col", asList(1, 2, 3)), IPredicateMock.in("col", asList(1, 2, 3)));
    }

    @Test
    void test_not_in()
    {
        assertFilter(Filters.nor(Filters.in("col", asList(1, 2, 3))), IPredicateMock.notIn("col", asList(1, 2, 3)));
    }

    @Test
    void test_like()
    {
        assertFilter(Filters.regex("col", LikePatternUtils.toRegex("abc%", null)), IPredicateMock.like("col", "abc%"));
    }

    @Test
    void test_not_like()
    {
        assertFilter(Filters.nor(Filters.regex("col", LikePatternUtils.toRegex("abc%", null))), IPredicateMock.notLike("col", "abc%"));
    }

    @Test
    void test_like_with_escape_character_is_not_pushed_down()
    {
        IPredicate predicate = IPredicateMock.like("col", "abc%");
        when(predicate.getLikeExpression()
                .getEscapeCharacterExpression()).thenReturn(mock(IExpression.class));

        List<IPredicate> predicates = new ArrayList<>(List.of(predicate));
        List<IPredicate> collected = MongoFilterBuilder.collect(predicates);

        assertTrue(collected.isEmpty());
        assertEquals(1, predicates.size());
    }

    @Test
    void test_is_null()
    {
        Bson expected = Filters.eq("col", null);
        Bson actual = build(IPredicateMock._null("col", false));
        assertFilter(expected, actual);
    }

    @Test
    void test_is_not_null()
    {
        Bson expected = Filters.nor(Filters.eq("col", null));
        Bson actual = build(IPredicateMock._null("col", true));
        assertFilter(expected, actual);
    }

    @Test
    void test_collect_consumes_supported_predicates_and_leaves_the_rest()
    {
        IPredicate supported = IPredicateMock.eq("col", "val");
        IPredicate unsupported = IPredicateMock.function("mongo", "someFunc", List.of());

        List<IPredicate> predicates = new ArrayList<>(List.of(supported, unsupported));
        List<IPredicate> collected = MongoFilterBuilder.collect(predicates);

        assertEquals(List.of(supported), collected);
        assertEquals(List.of(unsupported), predicates);
    }

    private Bson build(IPredicate predicate)
    {
        return MongoFilterBuilder.build(context, List.of(predicate))
                .get(0);
    }

    private void assertFilter(Bson expected, IPredicate predicate)
    {
        assertFilter(expected, build(predicate));
    }

    private void assertFilter(Bson expected, Bson actual)
    {
        assertEquals(toJson(expected), toJson(actual));
    }

    private static String toJson(Bson bson)
    {
        return bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry())
                .toJson();
    }
}
