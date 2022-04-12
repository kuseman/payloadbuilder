package se.kuseman.payloadbuilder.test;

import static org.mockito.Mockito.when;

import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;

/** Helper class to mock (@link {@link IOrdinalValues} */
public class IOrdinalValuesMock
{
    /** Mock a single ordinal value */
    public static IOrdinalValues ordinalValues(Object value)
    {
        IOrdinalValues mock = Mockito.mock(IOrdinalValues.class);
        when(mock.size()).thenReturn(1);
        when(mock.getValue(0)).thenReturn(value);
        return mock;
    }
}
