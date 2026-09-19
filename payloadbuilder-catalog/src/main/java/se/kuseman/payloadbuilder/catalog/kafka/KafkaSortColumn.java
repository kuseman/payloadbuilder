package se.kuseman.payloadbuilder.catalog.kafka;

/** A column that a pushed-down (descending) {@code ORDER BY} can be built from */
enum KafkaSortColumn
{
    OFFSET,
    TIMESTAMP
}
