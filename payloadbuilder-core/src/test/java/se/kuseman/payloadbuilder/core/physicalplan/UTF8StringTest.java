package se.kuseman.payloadbuilder.core.physicalplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Test of {@link UTF8String} */
class UTF8StringTest
{

    @Test
    void test_valuevector()
    {
        UTF8String str = UTF8String.from("hello");
        assertEquals(1, str.size());
        assertFalse(str.isNull(0));
        assertEquals(ResolvedType.STRING, str.type());
        assertEquals(str.getString(0), str);
    }

    @Test
    void test_get_bytes()
    {
        byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
        UTF8String str = UTF8String.utf8(bytes, 0, 5);
        assertFalse(str.hasNulls());
        assertEquals(UTF8String.from("hello"), str);

        byte[] slice = str.getBytes();
        assertEquals("hello", new String(slice, StandardCharsets.UTF_8));

        assertEquals("hello", UTF8String.from(slice)
                .toString());
    }

    @Test
    void test_get_bytes_supplied_array()
    {
        byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
        UTF8String str = UTF8String.utf8(bytes, 0, 5);

        assertFalse(str.hasString());
        assertTrue(UTF8String.from("hello")
                .hasString());

        assertEquals(UTF8String.from("hello"), str);

        byte[] slice = new byte[10];
        str.getBytes(slice);
        assertEquals("hello", new String(slice, 0, str.getByteLength(), StandardCharsets.UTF_8));
    }

    @Test
    void test_concat_builder()
    {
        List<UTF8String> strings = new ArrayList<>();
        assertEquals("", UTF8String.concat(UTF8String.from(","), strings)
                .toString());

        for (int i = 0; i < 10; i++)
        {
            strings.add(UTF8String.from("str" + i));

            if (i == 0)
            {
                assertSame(UTF8String.concat(UTF8String.from(","), strings), strings.get(0));
            }
        }
        UTF8String actual = UTF8String.concat(UTF8String.from(","), strings);
        assertEquals("str0,str1,str2,str3,str4,str5,str6,str7,str8,str9", actual.toString());
    }

    @Test
    void test_concat_bytes()
    {
        List<UTF8String> strings = new ArrayList<>();
        assertEquals("", UTF8String.concat(UTF8String.from(","), strings)
                .toString());

        for (int i = 0; i < 10; i++)
        {
            strings.add(UTF8String.utf8(("str" + i).getBytes(StandardCharsets.UTF_8)));

            if (i == 0)
            {
                assertSame(UTF8String.concat(UTF8String.from(","), strings), strings.get(0));
            }
        }
        UTF8String actual = UTF8String.concat(UTF8String.from(","), strings);
        assertEquals("str0,str1,str2,str3,str4,str5,str6,str7,str8,str9", actual.toString());
    }

    @Test
    void test_concat_latin_bytes()
    {
        List<UTF8String> strings = new ArrayList<>();
        strings.add(UTF8String.latin(("someCharStringText").getBytes(StandardCharsets.ISO_8859_1)));
        strings.add(UTF8String.latin(("_").getBytes(StandardCharsets.ISO_8859_1)));
        strings.add(UTF8String.latin(("second").getBytes(StandardCharsets.ISO_8859_1)));

        UTF8String actual = UTF8String.concat(UTF8String.EMPTY, strings);
        assertEquals("someCharStringText_second", actual.toString());
    }

    @Test
    void test_compare()
    {
        int size = 100_00;
        List<String> strings = new ArrayList<>(size);
        List<UTF8String> bytes = new ArrayList<>(size);
        Random rand = new Random();
        for (int i = 0; i < size; i++)
        {
            String str = "string" + rand.nextInt();
            strings.add(str);
            bytes.add(UTF8String.from(str));
        }

        Collections.sort(strings);
        Collections.sort(bytes);

        for (int i = 0; i < size; i++)
        {
            assertEquals(strings.get(i), bytes.get(i)
                    .toString());
        }
    }

    @Test
    void test_compareTo()
    {
        // String
        UTF8String utf1 = UTF8String.from("hello");
        UTF8String utf2 = UTF8String.from("world");
        UTF8String utf3 = UTF8String.from("hello world");
        assertEquals(0, utf1.compareTo(utf1));
        assertFalse(utf1.equals(utf2));
        assertFalse(utf1.equals(utf3));

        assertTrue(utf1.compareTo(utf2) < 0);
        assertTrue(utf2.compareTo(utf1) > 0);
        assertTrue(utf1.compareTo(utf3) < 0);
        assertTrue(utf3.compareTo(utf1) > 0);

        // Same encoding
        utf1 = UTF8String.from("hello".getBytes(StandardCharsets.UTF_8));
        utf2 = UTF8String.from("world".getBytes(StandardCharsets.UTF_8));
        utf3 = UTF8String.from("hello world".getBytes(StandardCharsets.UTF_8));
        assertEquals(0, utf1.compareTo(utf1));
        assertFalse(utf1.equals(utf2));
        assertFalse(utf1.equals(utf3));

        assertTrue(utf1.compareTo(utf2) < 0);
        assertTrue(utf2.compareTo(utf1) > 0);
        assertTrue(utf1.compareTo(utf3) < 0);
        assertTrue(utf3.compareTo(utf1) > 0);

        // Mixed encoding
        UTF8String s1 = UTF8String.utf8("hello".getBytes(StandardCharsets.UTF_8));
        UTF8String s2 = UTF8String.latin("world".getBytes(StandardCharsets.ISO_8859_1));

        assertEquals(0, s1.compareTo(s1));
        assertTrue(s1.compareTo(s2) < 0);
        assertTrue(s2.compareTo(s1) > 0);

        // Same prefix
        s1 = UTF8String.utf8("hello".getBytes(StandardCharsets.UTF_8));
        s2 = UTF8String.latin("hello 12".getBytes(StandardCharsets.ISO_8859_1));

        assertTrue(s1.compareTo(s2) < 0);
        assertTrue(s2.compareTo(s1) > 0);
    }

    @Test
    void test_equals()
    {
        // Same encoding
        UTF8String s1 = UTF8String.utf8("hello".getBytes(StandardCharsets.UTF_8));
        UTF8String s2 = UTF8String.utf8("hello".getBytes(StandardCharsets.UTF_8));

        assertNotEquals(s1, null);
        assertNotEquals(s1, 123);
        assertEquals(s1, s1);
        assertEquals(s1, s2);
        assertEquals(s2, s1);

        s2 = UTF8String.utf8("world".getBytes(StandardCharsets.UTF_8));
        assertNotEquals(s1, s2);
        assertNotEquals(s2, s1);

        s2 = UTF8String.utf8("world 123".getBytes(StandardCharsets.UTF_8));
        assertNotEquals(s1, s2);
        assertNotEquals(s2, s1);

        // Mized
        s1 = UTF8String.utf8("hello".getBytes(StandardCharsets.UTF_8));
        s2 = UTF8String.latin("hello".getBytes(StandardCharsets.ISO_8859_1));

        assertEquals(s1, s2);
        assertEquals(s2, s1);

        s2 = UTF8String.latin("world".getBytes(StandardCharsets.ISO_8859_1));
        assertNotEquals(s1, s2);
        assertNotEquals(s2, s1);

        s2 = UTF8String.latin("world 123".getBytes(StandardCharsets.ISO_8859_1));
        assertNotEquals(s1, s2);
        assertNotEquals(s2, s1);
    }

    @Test
    void test_latin1_convertsion()
    {
        for (int i = 0; i < 100_000; i++)
        {
            String rand1 = randomLatin1(100);
            String rand2 = randomLatin1(100);

            UTF8String utf1 = UTF8String.latin(rand1.getBytes(StandardCharsets.ISO_8859_1));
            UTF8String utf2 = UTF8String.latin(rand2.getBytes(StandardCharsets.ISO_8859_1));

            assertEquals(rand1, utf1.toString());
            assertEquals(utf1, UTF8String.utf8(rand1.getBytes(StandardCharsets.UTF_8)));

            assertEquals(0, utf1.compareTo(utf1));

            int tmp = rand1.compareTo(rand2);
            int cE = tmp > 0 ? 1
                    : (tmp < 0 ? -1
                            : 0);

            tmp = utf1.compareTo(utf2);
            int cA = tmp > 0 ? 1
                    : (tmp < 0 ? -1
                            : 0);

            assertEquals(cE, cA);
        }

    }

    private String randomLatin1(int size)
    {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder(size);

        for (int i = 0; i < size; i++)
        {
            char c = (char) (rand.nextInt(255) & 0xff);
            sb.append(c);
        }
        return sb.toString();
    }
}
