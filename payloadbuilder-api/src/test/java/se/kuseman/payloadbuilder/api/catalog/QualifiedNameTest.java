package se.kuseman.payloadbuilder.api.catalog;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Test of {@link QualifiedName} */
public class QualifiedNameTest extends Assert
{
    @Test
    public void test_toString()
    {
        assertEquals("part", QualifiedName.of("part")
                .toString());
        assertEquals("_part", QualifiedName.of("_part")
                .toString());

        assertEquals("part.two", QualifiedName.of("part", "two")
                .toString());

        assertEquals("part.\"t wo\"", QualifiedName.of("part", "t wo")
                .toString());

        assertEquals("part.\"t \"\"w\"\" o\"", QualifiedName.of("part", "t \"w\" o")
                .toString());

        assertEquals("\"12part\"", QualifiedName.of("12part")
                .toString());

        assertEquals("", QualifiedName.EMPTY.toString());

        assertEquals("\"key.value\"", QualifiedName.of("key.value")
                .toString());
    }

    @Test
    public void test_equalsIgnoreCase()
    {
        assertTrue(QualifiedName.EMPTY.equalsIgnoreCase(QualifiedName.EMPTY));
        assertTrue(QualifiedName.of("part")
                .equalsIgnoreCase(QualifiedName.of("PART")));
        assertFalse(QualifiedName.of("part")
                .equalsIgnoreCase(QualifiedName.of("PART", "TWO")));
        assertFalse(QualifiedName.of("TWO", "part")
                .equalsIgnoreCase(QualifiedName.of("PART", "TWO")));

    }
}
