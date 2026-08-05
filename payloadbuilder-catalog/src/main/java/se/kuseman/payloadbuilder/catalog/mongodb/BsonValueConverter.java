package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Converts values between BSON (as produced by the MongoDB driver) and PLB runtime representations. */
final class BsonValueConverter
{
    private BsonValueConverter()
    {
    }

    /**
     * Converts a raw BSON value (as read from a {@link Document}) into a PLB friendly value. Nested documents/arrays are kept as dynamic maps/lists that the engine navigates as {@code Any} values.
     */
    static Object fromBson(Object bsonValue)
    {
        if (bsonValue == null)
        {
            return null;
        }
        else if (bsonValue instanceof ObjectId oid)
        {
            return oid.toHexString();
        }
        else if (bsonValue instanceof Decimal128 d)
        {
            return d.bigDecimalValue();
        }
        else if (bsonValue instanceof Date date)
        {
            return EpochDateTime.from(date.getTime());
        }
        else if (bsonValue instanceof Binary b)
        {
            return b.getData();
        }
        else if (bsonValue instanceof Document doc)
        {
            return fromBsonDocument(doc);
        }
        else if (bsonValue instanceof List<?> list)
        {
            return fromBsonList(list);
        }
        return bsonValue;
    }

    private static Map<String, Object> fromBsonDocument(Document doc)
    {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : doc.entrySet())
        {
            result.put(e.getKey(), fromBson(e.getValue()));
        }
        return result;
    }

    private static List<Object> fromBsonList(List<?> list)
    {
        List<Object> result = new ArrayList<>(list.size());
        for (Object o : list)
        {
            result.add(fromBson(o));
        }
        return result;
    }

    /** Converts a PLB runtime value (from an evaluated expression) into a value the MongoDB driver understands for filters/seek keys. */
    static Object toBson(Object plbValue, String field)
    {
        Object value = plbValue;
        if (value == null)
        {
            return null;
        }
        if (value instanceof UTF8String s)
        {
            value = s.toString();
        }
        if (MongoCatalog.ID_COLUMN.equals(field)
                && value instanceof String s
                && ObjectId.isValid(s))
        {
            return new ObjectId(s);
        }
        else if (value instanceof EpochDateTime dt)
        {
            return new Date(dt.getEpoch());
        }
        else if (value instanceof EpochDateTimeOffset dt)
        {
            return new Date(dt.getEpoch());
        }
        else if (value instanceof Decimal d)
        {
            return new Decimal128(d.asBigDecimal());
        }
        return value;
    }
}
