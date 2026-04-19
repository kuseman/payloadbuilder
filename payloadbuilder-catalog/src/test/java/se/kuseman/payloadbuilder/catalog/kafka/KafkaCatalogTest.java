package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockSortItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.catalog.TestUtils;

class KafkaCatalogTest
{
    @Test
    void test_sort_items_timestamp_desc_are_consumed()
    {
        KafkaCatalog catalog = new KafkaCatalog();
        List<ISortItem> sortItems = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("timestamp"), ISortItem.Order.DESC)));

        IDatasource ds = catalog.getScanDataSource(TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.TOPIC, "orders"), 0, null)
                .getSession(), "kafka", QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, emptyList()));

        assertEquals(KafkaDatasource.class, ds.getClass());
        assertEquals(0, sortItems.size());
    }

    @Test
    void test_sort_items_offset_desc_are_consumed()
    {
        KafkaCatalog catalog = new KafkaCatalog();
        List<ISortItem> sortItems = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("offset"), ISortItem.Order.DESC)));

        IDatasource ds = catalog.getScanDataSource(TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.TOPIC, "orders"), 0, null)
                .getSession(), "kafka", QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, emptyList()));

        assertEquals(KafkaDatasource.class, ds.getClass());
        assertEquals(0, sortItems.size());
    }

    @Test
    void test_sort_items_mixed_directions_not_consumed()
    {
        KafkaCatalog catalog = new KafkaCatalog();
        List<ISortItem> sortItems = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("timestamp"), ISortItem.Order.DESC), mockSortItem(QualifiedName.of("offset"), ISortItem.Order.ASC)));

        catalog.getScanDataSource(TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.TOPIC, "orders"), 0, null)
                .getSession(), "kafka", QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, emptyList()));

        assertEquals(2, sortItems.size());
    }

    @Test
    void test_sort_items_unknown_column_not_consumed()
    {
        KafkaCatalog catalog = new KafkaCatalog();
        List<ISortItem> sortItems = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("key"), ISortItem.Order.DESC)));

        catalog.getScanDataSource(TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.TOPIC, "orders"), 0, null)
                .getSession(), "kafka", QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, emptyList()));

        assertEquals(1, sortItems.size());
    }

    @Test
    void test_sort_items_pushdown_newest_rejected_in_stream_mode()
    {
        KafkaCatalog catalog = new KafkaCatalog();
        List<ISortItem> sortItems = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("timestamp"), ISortItem.Order.DESC)));

        List<se.kuseman.payloadbuilder.api.catalog.Option> options = List
                .of(new se.kuseman.payloadbuilder.api.catalog.Option(KafkaOptions.MODE, se.kuseman.payloadbuilder.test.ExpressionTestUtils.createStringExpression("stream")));

        IDatasource ds = catalog
                .getScanDataSource(TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.TOPIC, "orders", KafkaCatalog.BOOTSTRAP_SERVERS, "localhost:9092"), 0, new KafkaNodeData())
                        .getSession(), "kafka", QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, options));

        se.kuseman.payloadbuilder.api.execution.IExecutionContext context = TestUtils.mockExecutionContext("kafka",
                Map.of(KafkaCatalog.TOPIC, "orders", KafkaCatalog.BOOTSTRAP_SERVERS, "localhost:9092"), 0, new KafkaNodeData());

        assertThrows(IllegalArgumentException.class, () -> ds.execute(context));
    }

    @Test
    void test_sort_items_asc_not_consumed()
    {
        KafkaCatalog catalog = new KafkaCatalog();
        List<ISortItem> sortItems = new ArrayList<>(List.of(mockSortItem(QualifiedName.of("timestamp"), ISortItem.Order.ASC)));

        assertDoesNotThrow(() -> catalog.getScanDataSource(TestUtils.mockExecutionContext("kafka", Map.of(KafkaCatalog.TOPIC, "orders"), 0, null)
                .getSession(), "kafka", QualifiedName.of("topic"), new DatasourceData(0, new ArrayList<>(), sortItems, Projection.ALL, emptyList())));

        assertEquals(1, sortItems.size());
    }
}
