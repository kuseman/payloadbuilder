package se.kuseman.payloadbuilder.test;

import java.util.Arrays;
import java.util.Objects;

import org.junit.Assert;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;

/** Vector utils that can be used when testing catalogs and asserting vectors */
public class VectorTestUtils extends Assert
{
    /** Create value vector of provided values */
    public static ValueVector vv(final Type type, final Object... values)
    {
        return vv(ResolvedType.of(type), values);
    }

    /** Create value vector of provided values */
    public static ValueVector vv(final ResolvedType type, final Object... values)
    {
        boolean nullable = Arrays.stream(values)
                .anyMatch(Objects::isNull);

        return new ValueVector()
        {
            @Override
            public boolean isNullable()
            {
                return nullable;
            }

            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public ResolvedType type()
            {
                return type;
            }

            @Override
            public int size()
            {
                return values.length;
            }

            @Override
            public Object getValue(int row)
            {
                return values[row];
            }
        };
    }

    /** Create value vector of provided values that is nullable */
    public static ValueVector nvv(final Type type, final Object... values)
    {
        return nvv(ResolvedType.of(type), values);
    }

    /** Create value vector of provided values that is nullable */
    public static ValueVector nvv(final ResolvedType type, final Object... values)
    {
        return new ValueVector()
        {
            @Override
            public boolean isNullable()
            {
                return true;
            }

            @Override
            public boolean isNull(int row)
            {
                return values[row] == null;
            }

            @Override
            public ResolvedType type()
            {
                return type;
            }

            @Override
            public int size()
            {
                return values.length;
            }

            @Override
            public Object getValue(int row)
            {
                return values[row];
            }
        };
    }

    /** Assert that to value vectors equals */
    public static void assertVectorsEquals(ValueVector expected, ValueVector actual)
    {
        assertVectorsEquals("", expected, actual);
    }

    /** Assert that to value vectors equals */
    public static void assertVectorsEquals(String message, ValueVector expected, ValueVector actual)
    {
        assertVectorsEquals(message, expected, actual, false);
    }

    /** Assert that to value vectors equals */
    public static void assertVectorsEquals(String message, ValueVector expected, ValueVector actual, boolean performNumberCasts)
    {
        assertEquals(message + "Types should equal", expected.type(), actual.type());
        assertEquals(message + "Sizes should equal", expected.size(), actual.size());
        assertEquals(message + "Nullable should equal", expected.isNullable(), actual.isNullable());

        for (int i = 0; i < expected.size(); i++)
        {
            boolean expectedIsNull = expected.isNullable()
                    && expected.isNull(i);
            boolean actualIsNull = actual.isNullable()
                    && actual.isNull(i);

            if (expected.isNullable())
            {
                assertEquals("Row " + i + " not equal regarding isNull", expected.isNull(i), actual.isNull(i));
            }
            if (expectedIsNull
                    && actualIsNull)
            {
                continue;
            }

            if (message != null
                    && !message.isEmpty())
            {
                message = message + System.lineSeparator();
            }

            switch (expected.type()
                    .getType())
            {
                case Boolean:
                    assertEquals(message + "Row " + i + " not equal", expected.getBoolean(i), actual.getBoolean(i));
                    break;
                case Double:
                    // We accept some loss here when casting between floating points
                    assertEquals(message + "Row (double) " + i + " not equal (getDouble)", expected.getDouble(i), actual.getDouble(i), 0.001);
                    if (performNumberCasts)
                    {
                        assertEquals(message + "Row (double) " + i + " not equal (getInt)", (int) expected.getDouble(i), actual.getInt(i));
                        assertEquals(message + "Row (double) " + i + " not equal (getLong)", (long) expected.getDouble(i), actual.getLong(i));
                        assertEquals(message + "Row (double) " + i + " not equal (getFloat)", expected.getDouble(i), actual.getFloat(i), 0.001);
                    }
                    break;
                case Float:
                    // We accept some loss here when casting between floating points
                    assertEquals(message + "Row (float) " + i + " not equal (getFloat)", expected.getFloat(i), actual.getFloat(i), 0.001);
                    if (performNumberCasts)
                    {
                        assertEquals(message + "Row (float) " + i + " not equal (getInt)", (int) expected.getFloat(i), actual.getInt(i));
                        assertEquals(message + "Row (float) " + i + " not equal (getLong)", (long) expected.getFloat(i), actual.getLong(i));
                        assertEquals(message + "Row (float) " + i + " not equal (getDouble)", expected.getFloat(i), actual.getDouble(i), 0.001);
                    }
                    break;
                case Int:
                    assertEquals(message + "Row (int) " + i + " not equal (getInt)", expected.getInt(i), actual.getInt(i));
                    if (performNumberCasts)
                    {
                        assertEquals(message + "Row (int) " + i + " not equal (getLong)", expected.getInt(i), (int) actual.getLong(i));
                        assertEquals(message + "Row (int) " + i + " not equal (getFloat)", expected.getInt(i), (int) actual.getFloat(i));
                        assertEquals(message + "Row (int) " + i + " not equal (getDouble)", expected.getInt(i), (int) actual.getDouble(i));
                    }
                    break;
                case Long:
                    assertEquals(message + "Row (long) " + i + " not equal (getLong)", expected.getLong(i), actual.getLong(i));
                    if (performNumberCasts)
                    {
                        assertEquals(message + "Row (long) " + i + " not equal (getInt)", (int) expected.getLong(i), actual.getInt(i));
                        assertEquals(message + "Row (long) " + i + " not equal (getFloat)", expected.getLong(i), (long) actual.getFloat(i));
                        assertEquals(message + "Row (long) " + i + " not equal (getDouble)", expected.getLong(i), (long) actual.getDouble(i));
                    }
                    break;
                case String:
                    assertEquals(message + "Row " + i + " not equal", expected.getString(i), actual.getString(i));
                    break;
                case DateTime:
                    assertEquals(message + "Row " + i + " not equal", expected.getDateTime(i), actual.getDateTime(i));
                    break;
                case TupleVector:
                    assertTupleVectorsEquals("Row: " + i, (TupleVector) expected.getValue(i), (TupleVector) actual.getValue(i));
                    break;
                case ValueVector:
                    assertVectorsEquals("Vector of row: " + i + " should equal: ", (ValueVector) expected.getValue(i), (ValueVector) actual.getValue(i));
                    break;
                default:
                    Object exp = expected.getValue(i);
                    Object act = actual.getValue(i);

                    if (exp instanceof ValueVector)
                    {
                        assertVectorsEquals("Vector of row: " + i + " should equal: ", (ValueVector) exp, (ValueVector) act);
                        break;
                    }
                    else if (exp instanceof TupleVector)
                    {
                        assertTupleVectorsEquals("Row: " + i, (TupleVector) exp, (TupleVector) act);
                        break;
                    }

                    // Normalize values
                    if (exp instanceof UTF8String)
                    {
                        exp = ((UTF8String) exp).toString();
                    }
                    if (act instanceof UTF8String)
                    {
                        act = ((UTF8String) act).toString();
                    }

                    assertEquals(message + "Row " + i + " not equal", exp, act);
                    break;
            }
        }
    }

    /** Assert that to value vectors equals */
    public static void assertTupleVectorsEquals(String message, TupleVector expected, TupleVector actual)
    {
        TupleVector e = expected;
        TupleVector a = actual;
        assertEquals(message + "Schema should equal", e.getSchema(), a.getSchema());
        assertEquals(message + "Row count should equal", e.getRowCount(), a.getRowCount());
        for (int j = 0; j < e.getSchema()
                .getSize(); j++)
        {
            assertVectorsEquals(message, e.getColumn(j), a.getColumn(j));
        }
    }

    /** Assert that to value vectors equals */
    public static void assertVectorsEqualsNulls(ValueVector expected, ValueVector actual)
    {
        assertEquals(expected.size(), actual.size());
        assertTrue("Vector should be nullable to assert nulls", actual.isNullable());

        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals("Row " + i + " should be null", expected.getValue(i), actual.isNull(i));
        }
    }
}
