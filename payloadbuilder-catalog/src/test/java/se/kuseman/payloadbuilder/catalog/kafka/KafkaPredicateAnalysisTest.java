package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.test.IPredicateMock;

/** Test of {@link KafkaPredicateAnalysis} */
class KafkaPredicateAnalysisTest
{
    @Test
    void test_empty_predicates()
    {
        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(new ArrayList<>());

        assertNull(analysis.partitionFilter);
        assertNull(analysis.offsetLower);
        assertNull(analysis.offsetUpper);
        assertNull(analysis.timestampLower);
        assertNull(analysis.timestampUpper);
    }

    @Test
    void test_null_predicates()
    {
        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(null);
        assertNull(analysis.partitionFilter);
    }

    // --- Partition predicates ---

    @Test
    void test_partition_equals()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.eq("partition", 2));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.partitionFilter);
        assertEquals(1, analysis.partitionFilter.size());
        assertEquals(0, predicates.size(), "Predicate should be removed");
    }

    @Test
    void test_partition_in()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.in("partition", List.of(0, 1, 3)));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.partitionFilter);
        assertEquals(3, analysis.partitionFilter.size());
        assertEquals(0, predicates.size());
    }

    // --- Offset predicates ---

    @Test
    void test_offset_equals()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.eq("offset", 500L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.offsetLower);
        assertTrue(analysis.offsetLower.inclusive());
        assertNotNull(analysis.offsetUpper);
        assertTrue(analysis.offsetUpper.inclusive());
        assertEquals(0, predicates.size());
    }

    @Test
    void test_offset_gt()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.gt("offset", 100L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.offsetLower);
        assertFalse(analysis.offsetLower.inclusive());
        assertNull(analysis.offsetUpper);
        assertEquals(0, predicates.size());
    }

    @Test
    void test_offset_gte()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.gte("offset", 100L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.offsetLower);
        assertTrue(analysis.offsetLower.inclusive());
        assertEquals(0, predicates.size());
    }

    @Test
    void test_offset_lt()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.lt("offset", 500L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.offsetUpper);
        assertFalse(analysis.offsetUpper.inclusive());
        assertEquals(0, predicates.size());
    }

    @Test
    void test_offset_lte()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.lte("offset", 500L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.offsetUpper);
        assertTrue(analysis.offsetUpper.inclusive());
        assertEquals(0, predicates.size());
    }

    // --- Timestamp predicates ---

    @Test
    void test_timestamp_gte()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.gte("timestamp", 1704067200000L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.timestampLower);
        assertTrue(analysis.timestampLower.inclusive());
        assertEquals(0, predicates.size());
    }

    @Test
    void test_timestamp_gt()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.gt("timestamp", 1704067200000L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.timestampLower);
        assertFalse(analysis.timestampLower.inclusive());
        assertEquals(0, predicates.size());
    }

    @Test
    void test_timestamp_lt()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.lt("timestamp", 1704153600000L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.timestampUpper);
        assertFalse(analysis.timestampUpper.inclusive());
        assertEquals(0, predicates.size());
    }

    @Test
    void test_timestamp_lte()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.lte("timestamp", 1704153600000L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.timestampUpper);
        assertTrue(analysis.timestampUpper.inclusive());
        assertEquals(0, predicates.size());
    }

    // --- Non-pushable predicates ---

    @Test
    void test_unknown_column_not_extracted()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.eq("key", "user-123"));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNull(analysis.partitionFilter);
        assertNull(analysis.offsetLower);
        assertEquals(1, predicates.size(), "Unknown predicate should NOT be removed");
    }

    @Test
    void test_not_equal_not_extracted()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.neq("offset", 100L));

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNull(analysis.offsetLower);
        assertNull(analysis.offsetUpper);
        assertEquals(1, predicates.size(), "NOT_EQUAL should not be extracted");
    }

    // --- Combined predicates ---

    @Test
    void test_multiple_predicates()
    {
        List<IPredicate> predicates = new ArrayList<>();
        predicates.add(IPredicateMock.eq("partition", 0));
        predicates.add(IPredicateMock.gte("offset", 10L));
        predicates.add(IPredicateMock.lt("offset", 100L));
        predicates.add(IPredicateMock.eq("key", "user-1")); // not pushable

        KafkaPredicateAnalysis analysis = KafkaPredicateAnalysis.analyze(predicates);

        assertNotNull(analysis.partitionFilter);
        assertNotNull(analysis.offsetLower);
        assertTrue(analysis.offsetLower.inclusive());
        assertNotNull(analysis.offsetUpper);
        assertFalse(analysis.offsetUpper.inclusive());
        assertEquals(1, predicates.size(), "Only unpushable predicate should remain");
    }
}
