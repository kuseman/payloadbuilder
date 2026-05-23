package se.kuseman.payloadbuilder.catalog.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;

class KafkaTupleIteratorTest
{
    @Test
    void test_column_ordinals()
    {
        record Col(String name, int ordinal)
        {
        }

        Arrays.stream(KafkaTupleIterator.class.getDeclaredFields())
                .filter(f -> f.getName()
                        .startsWith("COL_"))
                .map(f -> new Col(f.getName()
                        .substring(4)
                        .toLowerCase(), readField(f)))
                .forEach(c -> assertEquals(c.ordinal, indexOf(c.name)));
    }

    private int readField(Field field)
    {
        try
        {
            field.setAccessible(true);
            return (int) FieldUtils.readStaticField(field);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException("Error reading field", e);
        }
    }

    private int indexOf(String column)
    {
        int size = KafkaTupleIterator.SCHEMA.getSize();
        for (int i = 0; i < size; i++)
        {
            if (column.equalsIgnoreCase(KafkaTupleIterator.SCHEMA.getColumns()
                    .get(i)
                    .getName()))
            {
                return i;
            }
        }
        throw new IllegalArgumentException("Column: " + column + " not found");
    }
}
