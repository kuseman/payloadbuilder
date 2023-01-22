package se.kuseman.payloadbuilder.core.physicalplan;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.UTF8String;

/** Test of {@link UTF8String} */
public class UTF8StringTest extends Assert
{
    @Test
    public void test_concat()
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
    public void test_compare()
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
    public void test_compareTo()
    {
        UTF8String utf1 = UTF8String.from("hello");
        UTF8String utf2 = UTF8String.from("world");
        UTF8String utf3 = UTF8String.from("hello world");
        assertEquals(0, utf1.compareTo(utf1));
        assertFalse(utf1.equals(utf2));
        assertFalse(utf1.equals(utf3));

        assertTrue(utf1.compareTo(utf2) < 0);
        assertTrue(utf2.compareTo(utf1) > 0);
        assertTrue(utf1.compareTo(utf3) < 0);
    }

    @Test
    public void test_latin1_convertsion()
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
