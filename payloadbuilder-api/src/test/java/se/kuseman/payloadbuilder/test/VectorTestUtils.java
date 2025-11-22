package se.kuseman.payloadbuilder.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Vector utils that can be used when testing catalogs and asserting vectors */
public class VectorTestUtils
{
    /** Create value vector of provided values */
    public static ValueVector vv(final Type type, final Object... values)
    {
        return vv(ResolvedType.of(type), values);
    }

    /** Create value vector of provided values */
    public static ValueVector vv(final ResolvedType type, final Object... values)
    {
        return new ValueVector()
        {
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
            public Object getAny(int row)
            {
                return values[row];
            }

            @Override
            public ValueVector getArray(int row)
            {
                return (ValueVector) values[row];
            }

            @Override
            public TupleVector getTable(int row)
            {
                return (TupleVector) values[row];
            }

            @Override
            public ObjectVector getObject(int row)
            {
                return (ObjectVector) values[row];
            }
        };
    }

    /** Assert that to tuple vectors equals */
    public static void assertTupleVectorsEquals(TupleVector expected, TupleVector actual)
    {
        assertTupleVectorsEquals("", expected, actual);
    }

    /** Assert that to tuple vectors equals */
    public static void assertTupleVectorsEquals(String message, TupleVector expected, TupleVector actual)
    {
        TupleVector e = expected;
        TupleVector a = actual;
        assertEquals(e.getSchema(), a.getSchema(), message + " Schema should equal");
        assertEquals(e.getRowCount(), a.getRowCount(), message + " Row count should equal");
        for (int j = 0; j < e.getSchema()
                .getSize(); j++)
        {
            assertVectorsEquals(message + " Column(" + j + ") ", e.getColumn(j), a.getColumn(j));
        }
    }

    /** Assert that to object vectors equals */
    public static void assertObjectVectorsEquals(ObjectVector expected, ObjectVector actual)
    {
        assertObjectVectorsEquals("", expected, actual);
    }

    /** Assert that to object vectors equals */
    public static void assertObjectVectorsEquals(String message, ObjectVector expected, ObjectVector actual)
    {
        ObjectVector e = expected;
        ObjectVector a = actual;
        assertEquals(e.getSchema(), a.getSchema(), message + " Schema should equal");
        for (int j = 0; j < e.getSchema()
                .getSize(); j++)
        {
            assertValueEquals(message + " column: " + j + " ", e.getValue(j), a.getValue(j), e.getRow(), a.getRow(), true);
        }
    }

    /** Assert that to value vectors equals */
    public static void assertVectorsEqualsNulls(ValueVector expected, ValueVector actual)
    {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals(expected.getAny(i), actual.isNull(i), "Row " + i + " should be null");
        }
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
        assertEquals(expected.type(), actual.type(), message + "Types should equal");
        assertEquals(expected.size(), actual.size(), message + "Sizes should equal");

        for (int i = 0; i < expected.size(); i++)
        {
            assertValueEquals(message, expected, actual, i, i, performNumberCasts);
        }
    }

    /** Assert that to value equals in two vectors */
    private static void assertValueEquals(String message, ValueVector expected, ValueVector actual, int expectedRow, int actualRow, boolean performNumberCasts)
    {
        boolean expectedIsNull = expected.isNull(expectedRow);
        boolean actualIsNull = actual.isNull(actualRow);

        if (expectedIsNull != actualIsNull)
        {
            assertEquals(expectedIsNull, actualIsNull, message + "Row " + expectedRow + " not equal regarding isNull");
        }
        if (expectedIsNull
                && actualIsNull)
        {
            return;
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
                assertEquals(expected.getBoolean(expectedRow), actual.getBoolean(actualRow), message + "Row " + expectedRow + " not equal");
                break;
            case Double:
                // We accept some loss here when casting between floating points
                assertEquals(expected.getDouble(expectedRow), actual.getDouble(actualRow), 0.001, message + "Row (double) " + expectedRow + " not equal (getDouble)");
                if (performNumberCasts)
                {
                    assertEquals((int) expected.getDouble(expectedRow), actual.getInt(actualRow), message + "Row (double) " + expectedRow + " not equal (getInt)");
                    assertEquals((long) expected.getDouble(expectedRow), actual.getLong(actualRow), message + "Row (double) " + expectedRow + " not equal (getLong)");
                    assertEquals(expected.getDouble(expectedRow), actual.getFloat(actualRow), 0.001, message + "Row (double) " + expectedRow + " not equal (getFloat)");
                }
                break;
            case Float:
                // We accept some loss here when casting between floating points
                assertEquals(expected.getFloat(expectedRow), actual.getFloat(actualRow), 0.001, message + "Row (float) " + expectedRow + " not equal (getFloat)");
                if (performNumberCasts)
                {
                    assertEquals((int) expected.getFloat(expectedRow), actual.getInt(actualRow), message + "Row (float) " + expectedRow + " not equal (getInt)");
                    assertEquals((long) expected.getFloat(expectedRow), actual.getLong(actualRow), message + "Row (float) " + expectedRow + " not equal (getLong)");
                    assertEquals(expected.getFloat(expectedRow), actual.getDouble(actualRow), 0.001, message + "Row (float) " + expectedRow + " not equal (getDouble)");
                }
                break;
            case Int:
                assertEquals(expected.getInt(expectedRow), actual.getInt(actualRow), message + "Row (int) " + expectedRow + " not equal (getInt)");
                if (performNumberCasts)
                {
                    assertEquals(expected.getInt(expectedRow), (int) actual.getLong(actualRow), message + "Row (int) " + expectedRow + " not equal (getLong)");
                    assertEquals(expected.getInt(expectedRow), (int) actual.getFloat(actualRow), message + "Row (int) " + expectedRow + " not equal (getFloat)");
                    assertEquals(expected.getInt(expectedRow), (int) actual.getDouble(actualRow), message + "Row (int) " + expectedRow + " not equal (getDouble)");
                }
                break;
            case Long:
                assertEquals(expected.getLong(expectedRow), actual.getLong(actualRow), message + "Row (long) " + expectedRow + " not equal (getLong)");
                if (performNumberCasts)
                {
                    assertEquals((int) expected.getLong(expectedRow), actual.getInt(actualRow), message + "Row (long) " + expectedRow + " not equal (getInt)");
                    assertEquals(expected.getLong(expectedRow), (long) actual.getFloat(actualRow), message + "Row (long) " + expectedRow + " not equal (getFloat)");
                    assertEquals(expected.getLong(expectedRow), (long) actual.getDouble(actualRow), message + "Row (long) " + expectedRow + " not equal (getDouble)");
                }
                break;
            case Decimal:
                assertEquals(expected.getDecimal(expectedRow), actual.getDecimal(actualRow), message + "Row (decimal) " + expectedRow + " not equal (getDecimal)");
                // if (performNumberCasts)
                // {
                // assertEquals(message + "Row (long) " + expectedRow + " not equal (getInt)", (int) expected.getLong(expectedRow), actual.getInt(actualRow));
                // assertEquals(message + "Row (long) " + expectedRow + " not equal (getFloat)", expected.getLong(expectedRow), (long) actual.getFloat(actualRow));
                // assertEquals(message + "Row (long) " + expectedRow + " not equal (getDouble)", expected.getLong(expectedRow), (long) actual.getDouble(actualRow));
                // }
                break;
            case String:
                assertEquals(expected.getString(expectedRow), actual.getString(actualRow), message + "Row " + expectedRow + " not equal");
                break;
            case DateTime:
                assertEquals(expected.getDateTime(expectedRow), actual.getDateTime(actualRow), message + "Row " + expectedRow + " not equal");
                break;
            case DateTimeOffset:
                assertEquals(expected.getDateTimeOffset(expectedRow), actual.getDateTimeOffset(actualRow), message + "Row " + expectedRow + " not equal");
                break;
            case Table:
                assertTupleVectorsEquals("Table of row: " + expectedRow + ": ", expected.getTable(expectedRow), actual.getTable(actualRow));
                break;
            case Array:
                assertVectorsEquals("Array of row: " + expectedRow + " should equal: ", expected.getArray(expectedRow), actual.getArray(actualRow));
                break;
            case Object:
                assertObjectVectorsEquals("Object of row: " + expectedRow + " should equal: ", expected.getObject(expectedRow), actual.getObject(actualRow));
                break;
            default:
                Object exp = expected.getAny(expectedRow);
                Object act = actual.getAny(actualRow);

                if (exp instanceof ValueVector)
                {
                    assertVectorsEquals("Vector of row: " + expectedRow + " should equal: ", (ValueVector) exp, (ValueVector) act);
                    break;
                }
                else if (exp instanceof TupleVector)
                {
                    assertTupleVectorsEquals("Row: " + expectedRow, (TupleVector) exp, (TupleVector) act);
                    break;
                }
                else if (exp instanceof ObjectVector)
                {
                    assertObjectVectorsEquals("Object of row: " + expectedRow, (ObjectVector) exp, (ObjectVector) act);
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

                assertEquals(exp, act, message + "Row " + expectedRow + " not equal");
                break;
        }
    }
}
