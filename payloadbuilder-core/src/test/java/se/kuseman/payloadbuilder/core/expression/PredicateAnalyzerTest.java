package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeItem;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link PredicateAnalyzer} */
public class PredicateAnalyzerTest extends APhysicalPlanTest
{
    private TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
    private TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
    private TableSourceReference tableC = new TableSourceReference(2, "", QualifiedName.of("tableC"), "c");

    @Test
    public void test_outerreference_column_expressions()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(eq(cre("col1", tableA), ocre("col2", tableB)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, cre("col1", tableA), asSet(tableA), "col1", ocre("col2", tableB), emptySet(), "col2"));

        assertEquals(expected, actual);
        assertEquals(eq(cre("col1", tableA), ocre("col2", tableB)), actual.getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);
        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getValue());

        actualPair = actualPairs.getLeft()
                .get(0);

        assertEquals("a.col1 = b.col2", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.EQUAL, actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableB));

        assertTrue(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));
    }

    @Test
    public void test_equal_comparison_single_column_reference()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(null);
        assertEquals(AnalyzeResult.EMPTY, actual);
        assertNull(actual.getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);
        assertEquals(emptyList(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getValue());

        actual = PredicateAnalyzer.analyze(cre("flag", tableA));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, cre("flag", tableA), asSet(tableA), "flag", LiteralBooleanExpression.TRUE, emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(eq(cre("flag", tableA), LiteralBooleanExpression.TRUE), actual.getPredicate());

        assertEquals(asList(actual.getPairs()
                .get(0)), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actualPairs.getLeft()
                .get(0);
        assertEquals("a.flag = true", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.EQUAL, actualPair.getComparisonType());
        assertEquals("flag", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableB));
        try
        {
            actualPair.getExpressionPair(tableB);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableB"));
        }
        assertEquals(cre("flag", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(LiteralBooleanExpression.TRUE, actualPair.getExpressionPair(tableA)
                .getRight());
        assertTrue(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));

        /////////////////////////////////////////////////////////
        // Now make the column reference appear on the right side
        /////////////////////////////////////////////////////////

        actual = PredicateAnalyzer.analyze(eq(LiteralBooleanExpression.TRUE, cre("flag", tableA)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, LiteralBooleanExpression.TRUE, emptySet(), null, cre("flag", tableA), asSet(tableA), "flag"));

        assertEquals(expected, actual);
        assertEquals(eq(LiteralBooleanExpression.TRUE, cre("flag", tableA)), actual.getPredicate());

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actualPairs.getLeft()
                .get(0);
        assertEquals("true = a.flag", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals("flag", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableB));
        try
        {
            actualPair.getExpressionPair(tableB);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableB"));
        }
        assertEquals(cre("flag", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(LiteralBooleanExpression.TRUE, actualPair.getExpressionPair(tableA)
                .getRight());
        assertTrue(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));
    }

    @Test
    public void test_greater_than_comparison_single_column_reference()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(gt(cre("flag", tableA), new LiteralIntegerExpression(100)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.GREATER_THAN, cre("flag", tableA), asSet(tableA), "flag", new LiteralIntegerExpression(100), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(gt(cre("flag", tableA), new LiteralIntegerExpression(100)), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actualPairs.getLeft()
                .get(0);
        assertEquals("a.flag > 100", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.GREATER_THAN, actualPair.getComparisonType());
        assertEquals("flag", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableB));
        try
        {
            actualPair.getExpressionPair(tableB);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableB"));
        }
        assertEquals(cre("flag", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(new LiteralIntegerExpression(100), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));

        /////////////////////////////////////////////////////////
        // Now make the column reference appear on the right side
        /////////////////////////////////////////////////////////

        actual = PredicateAnalyzer.analyze(gt(new LiteralIntegerExpression(100), cre("flag", tableA)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.GREATER_THAN, new LiteralIntegerExpression(100), emptySet(), null, cre("flag", tableA), asSet(tableA), "flag"));

        assertEquals(expected, actual);
        assertEquals(gt(new LiteralIntegerExpression(100), cre("flag", tableA)), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actualPairs.getLeft()
                .get(0);
        assertEquals("100 > a.flag", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.GREATER_THAN, actualPair.getComparisonType());
        assertEquals("flag", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableB));
        try
        {
            actualPair.getExpressionPair(tableB);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableB"));
        }
        assertEquals(cre("flag", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(new LiteralIntegerExpression(100), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));
    }

    @Test
    public void test_equal_comparison_multi_column_reference()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(eq(cre("col1", tableA), cre("col2", tableB)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, cre("col1", tableA), asSet(tableA), "col1", cre("col2", tableB), asSet(tableB), "col2"));

        assertEquals(expected, actual);
        assertEquals(eq(cre("col1", tableA), cre("col2", tableB)), actual.getPredicate());

        assertEquals(asList(actual.getPairs()
                .get(0)), actual.getEquiPairs(tableA));
        assertEquals(asList(actual.getPairs()
                .get(0)), actual.getEquiPairs(tableB));
        assertEquals(emptyList(), actual.getEquiPairs(tableC));

        // No matches for either tableA or tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        assertEquals(0, actual.extractPushdownPairs(tableB)
                .getLeft()
                .size());

        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA)
                .getRight()
                .getPredicate());
        assertEquals(0, actual.extractPushdownPairs(tableA)
                .getLeft()
                .size());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 = b.col2", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.EQUAL, actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertEquals("col2", actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableA)
                .getRight());
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableB)
                .getLeft());
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableB)
                .getRight());
        assertTrue(actualPair.isEqui(tableA));
        assertTrue(actualPair.isEqui(tableB));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_greater_than_comparison_multi_column_reference()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(gt(cre("col1", tableA), cre("col2", tableB)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.GREATER_THAN, cre("col1", tableA), asSet(tableA), "col1", cre("col2", tableB), asSet(tableB), "col2"));

        assertEquals(expected, actual);
        assertEquals(gt(cre("col1", tableA), cre("col2", tableB)), actual.getPredicate());

        // Not an equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));
        assertEquals(emptyList(), actual.getEquiPairs(tableC));

        // No matches for either tableA or tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        assertEquals(0, actual.extractPushdownPairs(tableB)
                .getLeft()
                .size());

        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA)
                .getRight()
                .getPredicate());
        assertEquals(0, actual.extractPushdownPairs(tableA)
                .getLeft()
                .size());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 > b.col2", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.GREATER_THAN, actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertEquals("col2", actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableA)
                .getRight());
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableB)
                .getLeft());
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableB)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_equal_comparison_multi_column_reference_with_ambiguous_column()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(eq(ce("col1"), cre("col2", tableB)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, ce("col1"), asSet(AnalyzeItem.UNKNOWN_TABLE_SOURCE), "col1", cre("col2", tableB), asSet(tableB), "col2"));

        assertEquals(expected, actual);
        assertEquals(eq(ce("col1"), cre("col2", tableB)), actual.getPredicate());

        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(asList(actual.getPairs()
                .get(0)), actual.getEquiPairs(tableB));
        assertEquals(emptyList(), actual.getEquiPairs(tableC));

        // No matches for either tableA or tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        assertEquals(0, actual.extractPushdownPairs(tableB)
                .getLeft()
                .size());

        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA)
                .getRight()
                .getPredicate());
        assertEquals(0, actual.extractPushdownPairs(tableA)
                .getLeft()
                .size());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("col1 = b.col2", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.EQUAL, actualPair.getComparisonType());
        assertNull(actualPair.getColumn(tableA));
        assertEquals("col2", actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));
        assertFalse(actualPair.isPushdown(tableA));
        assertFalse(actualPair.isPushdown(tableB));

        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableB)
                .getLeft());
        assertEquals(ce("col1"), actualPair.getExpressionPair(tableB)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertTrue(actualPair.isEqui(tableB));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_not_equal_single_column_reference()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(not(cre("col1", tableA)));
        expected = result(pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, cre("col1", tableA), asSet(tableA), "col1", LiteralBooleanExpression.FALSE, emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(eq(cre("col1", tableA), LiteralBooleanExpression.FALSE), actual.getPredicate());

        assertEquals(asList(actual.getPairs()
                .get(0)), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 = false", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.EQUAL, actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(LiteralBooleanExpression.FALSE, actualPair.getExpressionPair(tableA)
                .getRight());
        assertTrue(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_null_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(nullP(cre("col1", tableA), false));
        expected = result(pair(IPredicate.Type.NULL, null, cre("col1", tableA), asSet(tableA), "col1", nullP(cre("col1", tableA), false), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(nullP(cre("col1", tableA), false), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        // Null predicate is not allowed to be pushed down in certain cases so extracting for tableA
        // should not yield any result
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 IS NULL", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.NULL, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(nullP(cre("col1", tableA), false), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_not_null_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(nullP(cre("col1", tableA), true));
        expected = result(pair(IPredicate.Type.NULL, null, cre("col1", tableA), asSet(tableA), "col1", nullP(cre("col1", tableA), true), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(nullP(cre("col1", tableA), true), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original predicate back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        // Not Null predicate is allowed to be pushed down even in LEFT JOINS etc.
        assertNull(actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());
        // Whole expression should be consumed since all pairs is pushdown
        assertEquals(expected.getPairs(), actual.extractPushdownPairs(tableA, false)
                .getLeft());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 IS NOT NULL", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.NULL, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(nullP(cre("col1", tableA), true), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_in_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), false));
        expected = result(pair(IPredicate.Type.IN, null, cre("col1", tableA), asSet(tableA), "col1", in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), false), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), false), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        assertNull(actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 IN (10.0)", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.IN, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), false), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_not_in_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), true));
        expected = result(pair(IPredicate.Type.IN, null, cre("col1", tableA), asSet(tableA), "col1", in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), true), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), true), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        assertNull(actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 NOT IN (10.0)", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.IN, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(in(cre("col1", tableA), asList(new LiteralFloatExpression(10f)), true), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_in_predicate_multi_column_references()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(in(cre("col1", tableA), asList(cre("col2", tableB)), false));
        expected = result(pair(IPredicate.Type.IN, null, cre("col1", tableA), asSet(tableA), "col1", in(cre("col1", tableA), asList(cre("col2", tableB)), false), asSet(tableB), null));

        assertEquals(expected, actual);
        assertEquals(in(cre("col1", tableA), asList(cre("col2", tableB)), false), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(emptyList(), actualPairs.getLeft());
        assertEquals(expected, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 IN (b.col2)", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.IN, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(in(cre("col1", tableA), asList(cre("col2", tableB)), false), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_in_predicate_with_column_reference_inside_argument_list()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10f, 20f)), asList(cre("col1", tableA)), false));
        expected = result(pair(IPredicate.Type.UNDEFINED, null, in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10f, 20f)), asList(cre("col1", tableA)), false), asSet(tableA), null, null,
                null, null));

        assertEquals(expected, actual);
        assertEquals(in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10f, 20f)), asList(cre("col1", tableA)), false), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        assertNull(actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("array(10, 20) IN (a.col1)", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.UNDEFINED, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertNull(actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));

        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10f, 20f)), asList(cre("col1", tableA)), false), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertNull(actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_like_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(like(cre("col1", tableA), new LiteralStringExpression("%val%"), false));
        expected = result(pair(IPredicate.Type.LIKE, null, cre("col1", tableA), asSet(tableA), "col1", like(cre("col1", tableA), new LiteralStringExpression("%val%"), false), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(like(cre("col1", tableA), new LiteralStringExpression("%val%"), false), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        assertNull(actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 LIKE '%val%'", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.LIKE, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(like(cre("col1", tableA), new LiteralStringExpression("%val%"), false), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_not_like_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(like(cre("col1", tableA), new LiteralStringExpression("%val%"), true));
        expected = result(pair(IPredicate.Type.LIKE, null, cre("col1", tableA), asSet(tableA), "col1", like(cre("col1", tableA), new LiteralStringExpression("%val%"), true), emptySet(), null));

        assertEquals(expected, actual);
        assertEquals(like(cre("col1", tableA), new LiteralStringExpression("%val%"), true), actual.getPredicate());

        // No equi
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // No matches for tableB and hence we will get the original expression back when extracting
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());

        assertNull(actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.col1 NOT LIKE '%val%'", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.LIKE, actualPair.getType());
        assertNull(actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(like(cre("col1", tableA), new LiteralStringExpression("%val%"), true), actualPair.getExpressionPair(tableA)
                .getRight());
        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableC));
    }

    @Test
    public void test_function_call()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        FunctionCallExpression fce = new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("isnull"), null, asList(cre("flag", tableA), cre("col1", tableA)));

        actual = PredicateAnalyzer.analyze(fce);
        //@formatter:off
        expected = result(
                pair(IPredicate.Type.FUNCTION_CALL, null, fce, asSet(tableA), null, null, null, null)
                );
        //@formatter:on

        assertEquals(expected, actual);
        assertEquals(fce, actual.getPredicate());

        // No equi pairs for function calls
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // Function call is push-able to tableA so no analyze result should be left
        assertEquals(null, actual.extractPushdownPairs(tableA)
                .getRight()
                .getPredicate());
        assertEquals(null, actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(expected.getPairs(), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("isnull(a.flag, a.col1)", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.FUNCTION_CALL, actualPair.getType());
        assertEquals(null, actualPair.getComparisonType());
        assertEquals(null, actualPair.getColumn(tableA));
        assertEquals(null, actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));

        try
        {
            actualPair.getExpressionPair(tableB);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableB"));
        }

        assertEquals(Pair.of(fce, null), actualPair.getExpressionPair(tableA));
    }

    @Test
    public void test_function_call_no_column_references()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        FunctionCallExpression fce = new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("isnull"), null, asList(intLit(10)));

        actual = PredicateAnalyzer.analyze(fce);
        //@formatter:off
        expected = result(
                pair(IPredicate.Type.FUNCTION_CALL, null, fce, asSet(), null, null, null, null)
                );
        //@formatter:on

        assertEquals(expected, actual);
        assertEquals(fce, actual.getPredicate());

        // No equi pairs for function calls
        assertEquals(emptyList(), actual.getEquiPairs(tableA));
        assertEquals(emptyList(), actual.getEquiPairs(tableB));

        // Function call is not referencing any table sources to pushable to all
        assertEquals(null, actual.extractPushdownPairs(tableA)
                .getRight()
                .getPredicate());
        assertEquals(null, actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());
        assertEquals(null, actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        assertEquals(null, actual.extractPushdownPairs(tableB, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(asList(expected.getPairs()
                .get(0)), actualPairs.getLeft());
        assertEquals(AnalyzeResult.EMPTY, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("isnull(10)", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.FUNCTION_CALL, actualPair.getType());
        assertEquals(null, actualPair.getComparisonType());
        assertEquals(null, actualPair.getColumn(tableA));
        assertEquals(null, actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));

        assertEquals(Pair.of(fce, null), actualPair.getExpressionPair(tableA));
        assertEquals(Pair.of(fce, null), actualPair.getExpressionPair(tableB));
    }

    @Test
    public void test_complex_predicate()
    {
        AnalyzeResult actual;
        AnalyzeResult expected;
        Pair<List<AnalyzePair>, AnalyzeResult> actualPairs;
        AnalyzePair actualPair;

        actual = PredicateAnalyzer.analyze(and(eq(cre("col1", tableA), cre("col2", tableB)), gt(cre("flag", tableA), cre("col3", tableB))));
        //@formatter:off
        expected = result(
                pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.GREATER_THAN, cre("flag", tableA), asSet(tableA), "flag", cre("col3", tableB), asSet(tableB), "col3"),
                pair(IPredicate.Type.COMPARISION, IComparisonExpression.Type.EQUAL, cre("col1", tableA), asSet(tableA), "col1", cre("col2", tableB), asSet(tableB), "col2")
                );
        //@formatter:on

        assertEquals(expected, actual);
        assertEquals(and(eq(cre("col1", tableA), cre("col2", tableB)), gt(cre("flag", tableA), cre("col3", tableB))), actual.getPredicate());

        assertEquals(asList(expected.getPairs()
                .get(1)), actual.getEquiPairs(tableA));
        // tableB is included in the equal pair
        assertEquals(asList(expected.getPairs()
                .get(1)), actual.getEquiPairs(tableB));

        // No push downs
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA)
                .getRight()
                .getPredicate());
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableA, false)
                .getRight()
                .getPredicate());
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB)
                .getRight()
                .getPredicate());
        assertEquals(actual.getPredicate(), actual.extractPushdownPairs(tableB, false)
                .getRight()
                .getPredicate());

        actualPairs = actual.extractPushdownPairs(tableA);

        assertEquals(emptyList(), actualPairs.getLeft());
        assertEquals(expected, actualPairs.getRight());

        actualPair = actual.getPairs()
                .get(0);
        assertEquals("a.flag > b.col3", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.GREATER_THAN, actualPair.getComparisonType());
        assertEquals("flag", actualPair.getColumn(tableA));
        assertEquals("col3", actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("flag", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(cre("col3", tableB), actualPair.getExpressionPair(tableA)
                .getRight());
        assertEquals(cre("col3", tableB), actualPair.getExpressionPair(tableB)
                .getLeft());
        assertEquals(cre("flag", tableA), actualPair.getExpressionPair(tableB)
                .getRight());

        assertFalse(actualPair.isEqui(tableA));
        assertFalse(actualPair.isEqui(tableB));
        assertFalse(actualPair.isEqui(tableC));

        actualPair = actual.getPairs()
                .get(1);
        assertEquals("a.col1 = b.col2", actualPair.getSqlRepresentation());
        assertEquals(IPredicate.Type.COMPARISION, actualPair.getType());
        assertEquals(IComparisonExpression.Type.EQUAL, actualPair.getComparisonType());
        assertEquals("col1", actualPair.getColumn(tableA));
        assertEquals("col2", actualPair.getColumn(tableB));
        assertNull(actualPair.getColumn(tableC));
        try
        {
            actualPair.getExpressionPair(tableC);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("No expressions could be found in this pair for table source tableC"));
        }
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableA)
                .getLeft());
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableA)
                .getRight());
        assertEquals(cre("col2", tableB), actualPair.getExpressionPair(tableB)
                .getLeft());
        assertEquals(cre("col1", tableA), actualPair.getExpressionPair(tableB)
                .getRight());
        assertTrue(actualPair.isEqui(tableA));
        assertTrue(actualPair.isEqui(tableB));
        assertFalse(actualPair.isEqui(tableC));
    }

    private AnalyzeResult result(AnalyzePair... pairs)
    {
        return new AnalyzeResult(asList(pairs));
    }

    //@formatter:off
    private AnalyzePair pair(
            IPredicate.Type type,
            IComparisonExpression.Type comparisonType,
            IExpression leftExpression,
            Set<TableSourceReference> leftTableSources,
            String leftColumn,
            
            IExpression rightExpression,
            Set<TableSourceReference> rightTableSources,
            String rightColumn)
    //@formatter:on
    {
        AnalyzeItem left = new AnalyzeItem(leftExpression, leftTableSources, leftColumn);
        AnalyzeItem right = rightExpression != null ? new AnalyzeItem(rightExpression, rightTableSources, rightColumn)
                : null;

        return new AnalyzePair(type, comparisonType, left, right);
    }
}
