package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockSortItem;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Sorts;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;

/** Unit tests of {@link MongoSortBuilder}. */
class MongoSortBuilderTest
{
    @Test
    void test_single_ascending_column_is_consumed()
    {
        List<ISortItem> items = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("col"), Order.ASC)));

        Bson sort = MongoSortBuilder.build(items);

        assertEquals(toJson(Sorts.ascending("col")), toJson(sort));
        assertEquals(0, items.size());
    }

    @Test
    void test_multiple_columns_mixed_order()
    {
        List<ISortItem> items = new ArrayList<>(asList(mockSortItem(QualifiedName.of("a"), Order.ASC), mockSortItem(QualifiedName.of("b"), Order.DESC)));

        Bson sort = MongoSortBuilder.build(items);

        assertEquals(toJson(Sorts.orderBy(Sorts.ascending("a"), Sorts.descending("b"))), toJson(sort));
        assertEquals(0, items.size());
    }

    @Test
    void test_empty_list_returns_null()
    {
        assertNull(MongoSortBuilder.build(new ArrayList<>()));
    }

    @Test
    void test_explicit_null_order_is_not_pushed_down()
    {
        List<ISortItem> items = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("col"), Order.ASC, NullOrder.FIRST)));

        Bson sort = MongoSortBuilder.build(items);

        assertNull(sort);
        // All-or-none - list must be left untouched when not consumed
        assertEquals(1, items.size());
    }

    private static String toJson(Bson bson)
    {
        return bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry())
                .toJson();
    }
}
