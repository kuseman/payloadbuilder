package se.kuseman.payloadbuilder.core.catalog.system;

import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link IsNullFunction} */
public class IsNullFunctionTest extends APhysicalPlanTest
{
    // @Test
    // public void test_dataType()
    // {
    // assertEquals(DataType.ANY, e("isnull(a, b)").getDataType());
    // assertEquals(DataType.ANY, e("isnull(a, 10)").getDataType());
    // assertEquals(DataType.INT, e("isnull(20, 10)").getDataType());
    // }
    //
    // @Test
    // public void test_codeGen()
    // {
    // Predicate<ExecutionContext> p = CODE_GENERATOR.generatePredicate(e("isnull(a, true)"));
    // context.getStatementContext()
    // .setTuple(NoOpTuple.NO_OP);
    // assertTrue(p.test(context));
    //
    // p = CODE_GENERATOR.generatePredicate(e("isnull(10, 20) = 10"));
    // assertTrue(p.test(context));
    //
    // p = CODE_GENERATOR.generatePredicate(e("isnull(null, 20l) = 20l"));
    // assertTrue(p.test(context));
    //
    // // Set data type of reference
    // Expression e = e("isnull(a, 20l) = 20l", Mockito.mock(Tuple.class), ofEntries(entry("a", Pair.of(null, DataType.INT))));
    //
    // p = CODE_GENERATOR.generatePredicate(e);
    // assertTrue(p.test(context));
    // }
}
