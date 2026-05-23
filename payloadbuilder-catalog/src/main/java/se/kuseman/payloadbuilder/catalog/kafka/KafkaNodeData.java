package se.kuseman.payloadbuilder.catalog.kafka;

import se.kuseman.payloadbuilder.api.execution.NodeData;

/** Extended node data for Kafka datasource statistics */
class KafkaNodeData extends NodeData
{
    long recordsPolled;
    long bytesRead;
    long pollCount;
    int deserializationErrors;
}
