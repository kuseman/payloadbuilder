package se.kuseman.payloadbuilder.bytes;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test of {@link PayloadReader} */
class PayloadReaderTest
{
    @Test
    void test_isSupported()
    {
        assertFalse(PayloadReader.isSupportedPayload(null));
        assertFalse(PayloadReader.isSupportedPayload(new byte[0]));
        assertFalse(PayloadReader.isSupportedPayload(new byte[] { 0, 1, 2 }));
        assertFalse(PayloadReader.isSupportedPayload(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }));
        assertFalse(PayloadReader.isSupportedPayload(new byte[] { PayloadReader.P, PayloadReader.B, PayloadReader.L, 1, 2, 3, 4, 5, 6, PayloadReader.B }));
        assertFalse(PayloadReader.isSupportedPayload(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.L, 1, 2, 3, 4, 5, 6, PayloadReader.B }));
        assertFalse(PayloadReader.isSupportedPayload(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.B, 1, 2, 3, 4, 5, 6, PayloadReader.L }));
        assertTrue(PayloadReader.isSupportedPayload(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.B, 1, 2, 3, 4, 5, 6, 5 }));
    }
}
