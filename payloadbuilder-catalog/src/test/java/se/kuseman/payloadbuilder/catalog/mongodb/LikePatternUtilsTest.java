package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/** Unit tests of {@link LikePatternUtils}. */
class LikePatternUtilsTest
{
    @Test
    void test_percent_wildcard()
    {
        assertTrue(Pattern.matches(LikePatternUtils.toRegex("abc%", null), "abcdef"));
        assertTrue(Pattern.matches(LikePatternUtils.toRegex("abc%", null), "abc"));
        assertFalse(Pattern.matches(LikePatternUtils.toRegex("abc%", null), "xabcdef"));
    }

    @Test
    void test_underscore_wildcard()
    {
        assertTrue(Pattern.matches(LikePatternUtils.toRegex("a_c", null), "abc"));
        assertFalse(Pattern.matches(LikePatternUtils.toRegex("a_c", null), "abbc"));
        assertFalse(Pattern.matches(LikePatternUtils.toRegex("a_c", null), "ac"));
    }

    @Test
    void test_regex_special_characters_are_escaped()
    {
        assertTrue(Pattern.matches(LikePatternUtils.toRegex("a.b*c", null), "a.b*c"));
        assertFalse(Pattern.matches(LikePatternUtils.toRegex("a.b*c", null), "aXbYYc"));
    }

    @Test
    void test_escape_character()
    {
        String regex = LikePatternUtils.toRegex("100\\%", '\\');
        assertTrue(Pattern.matches(regex, "100%"));
        assertFalse(Pattern.matches(regex, "100anything"));
    }
}
