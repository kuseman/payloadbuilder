package se.kuseman.payloadbuilder.api.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class UTF8StringTest
{
    @Test
    void test_hash_code_equals_between_encodings()
    {
        UTF8String str1 = UTF8String.utf8("three".getBytes(StandardCharsets.UTF_8));
        UTF8String str2 = UTF8String.latin("three".getBytes(StandardCharsets.ISO_8859_1));
        assertEquals(str1.hashCode(), str2.hashCode());
    }

    @Test
    void test_charSequence_String()
    {
        // String
        UTF8String str = UTF8String.from("three");
        assertFalse(str.isLatin1());
        assertEquals(5, str.length());
        assertEquals(5, str.length());
        assertThrows(IndexOutOfBoundsException.class, () -> str.charAt(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> str.charAt(5));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(3, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(-1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(0, 6));
        assertEquals('t', str.charAt(0));
        assertEquals('h', str.charAt(1));
        assertEquals('r', str.charAt(2));
        assertEquals('e', str.charAt(3));
        assertEquals('e', str.charAt(4));
        assertEquals("ee", str.subSequence(3, 5));
        assertEquals("three", str.toString());
    }

    @Test
    void test_charSequence_UTF8()
    {
        // CSOFF
        UTF8String str = UTF8String.from("\u2705\u5F3A".getBytes(StandardCharsets.UTF_8));
        assertFalse(str.isLatin1());
        assertFalse(str.hasString());
        assertEquals(2, str.length());
        assertEquals(2, str.length());
        assertThrows(IndexOutOfBoundsException.class, () -> str.charAt(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> str.charAt(5));
        assertFalse(str.hasString());
        assertEquals('\u2705', str.charAt(0));
        assertEquals('\u5F3A', str.charAt(1));
        assertEquals("\u2705\u5F3A", str.toString());
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(3, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(-1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(0, 6));
        // CSON
    }

    @Test
    void test_complex_UTF8()
    {
        UTF8String str = UTF8String.from("Зарегистрируйтесь сейчас на Десятую Международную Конференцию по".getBytes(StandardCharsets.UTF_8));
        assertFalse(str.isLatin1());
        assertEquals(64, str.length());
        assertEquals(122, str.getByteLength());
        assertEquals('р', str.charAt(10));

        assertEquals(64, str.toString()
                .length());
        assertEquals('р', str.toString()
                .charAt(10));

        str = UTF8String.from("สิบสองกษัตริย์ก่อนหน้าแลถัดไป".getBytes(StandardCharsets.UTF_8));
        assertFalse(str.isLatin1());
        assertEquals(29, str.length());
        assertEquals(87, str.getByteLength());
        assertEquals('ร', str.charAt(10));

        assertEquals(29, str.toString()
                .length());
        assertEquals('ร', str.toString()
                .charAt(10));

        str = UTF8String.from("ድር ቢያብር አንበሳ ያስር።".getBytes(StandardCharsets.UTF_8));
        assertFalse(str.isLatin1());
        assertEquals(17, str.length());
        assertEquals(45, str.getByteLength());
        assertEquals('በ', str.charAt(10));

        assertEquals(17, str.toString()
                .length());
        assertEquals('በ', str.toString()
                .charAt(10));

        str = UTF8String.from("⠗⠑⠛⠜⠙ ⠁ ⠊⠕⠋⠋⠔⠤⠝".getBytes(StandardCharsets.UTF_8));
        assertFalse(str.isLatin1());
        assertEquals(15, str.length());
        assertEquals(41, str.getByteLength());
        assertEquals('⠋', str.charAt(10));

        assertEquals(15, str.toString()
                .length());
        assertEquals('⠋', str.toString()
                .charAt(10));

        // 4 bytes chars
        str = UTF8String.from("𓄉𓄫𓅒𓅤".getBytes(StandardCharsets.UTF_8));
        assertEquals(8, str.length());
        assertEquals(16, str.getByteLength());
        assertEquals(56619, str.charAt(3));

        assertEquals(8, str.toString()
                .length());
        assertEquals(56619, str.toString()
                .charAt(3));

    }

    @Test
    void test_charSequence_Latin1()
    {
        // Latin1 bytes
        UTF8String str = UTF8String.from("three".getBytes(StandardCharsets.ISO_8859_1));
        assertTrue(str.isLatin1());
        assertFalse(str.hasString());
        assertEquals(5, str.length());
        assertEquals(5, str.length());
        assertThrows(IndexOutOfBoundsException.class, () -> str.charAt(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> str.charAt(5));
        assertFalse(str.hasString());
        assertEquals('t', str.charAt(0));
        assertEquals('h', str.charAt(1));
        assertEquals('r', str.charAt(2));
        assertEquals('e', str.charAt(3));
        assertEquals('e', str.charAt(4));
        assertEquals("three", str.toString());
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(3, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(-1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> str.subSequence(0, 6));
    }
}
