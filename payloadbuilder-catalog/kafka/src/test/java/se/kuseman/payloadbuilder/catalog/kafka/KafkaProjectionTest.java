package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;

class KafkaProjectionTest
{
    @Test
    void test_from()
    {
        assertEquals(KafkaProjection.ALL, KafkaProjection.from(null));
        assertEquals(KafkaProjection.ALL, KafkaProjection.from(Projection.ALL));
        assertEquals(new KafkaProjection(false, false), KafkaProjection.from(Projection.NONE));
        assertEquals(new KafkaProjection(false, false), KafkaProjection.from(Projection.columns(List.of("key", "offset"))));
        assertEquals(new KafkaProjection(true, false), KafkaProjection.from(Projection.columns(List.of("key", "Value"))));
        assertEquals(new KafkaProjection(false, true), KafkaProjection.from(Projection.columns(List.of("HEADERS"))));
    }
}
