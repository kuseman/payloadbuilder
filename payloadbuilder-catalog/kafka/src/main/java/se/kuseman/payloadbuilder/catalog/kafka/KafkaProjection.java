package se.kuseman.payloadbuilder.catalog.kafka;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;

/**
 * Which of the costly-to-retain columns a topic scan must keep. Every other column is cheap (primitives or shared instances) and is always produced. A column that is not kept is returned as null, as
 * the {@link Projection} contract allows.
 *
 * @param value Keep the raw value payload (the bulk of a record's memory)
 * @param headers Keep the record headers (a map per record)
 */
record KafkaProjection(boolean value, boolean headers)
{
    static final KafkaProjection ALL = new KafkaProjection(true, true);
    private static final KafkaProjection NONE = new KafkaProjection(false, false);

    static KafkaProjection from(Projection projection)
    {
        if (projection == null)
        {
            return ALL;
        }
        return switch (projection.type())
        {
            case ALL -> ALL;
            case NONE -> NONE;
            case COLUMNS -> new KafkaProjection(contains(projection.columns(), "value"), contains(projection.columns(), "headers"));
        };
    }

    private static boolean contains(List<String> columns, String column)
    {
        for (String c : columns)
        {
            if (column.equalsIgnoreCase(c))
            {
                return true;
            }
        }
        return false;
    }
}
