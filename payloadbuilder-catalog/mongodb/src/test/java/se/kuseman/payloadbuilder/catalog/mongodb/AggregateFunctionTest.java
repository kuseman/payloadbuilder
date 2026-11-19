package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.test.ExpressionTestUtils;

/**
 * Unit tests of {@link AggregateFunction}.
 *
 * <pre>
 * NOTE! Pipeline validation happens before a MongoClient is ever acquired, so all of this - including the malformed-pipeline cases - is
 * unit testable without Docker/a live connection. Actually running a well-formed pipeline requires a live MongoClient and is covered by the
 * testcontainers based integration tests instead.
 * </pre>
 */
class AggregateFunctionTest
{
    private final AggregateFunction function = new AggregateFunction(new MongoCatalog());
    private final IExecutionContext context = mockExecutionContext("mongo", emptyMap(), 0, null);

    @Test
    void test_description_is_not_blank()
    {
        assertFalse(StringUtils.isBlank(function.getDescription()));
    }

    @Test
    void test_wrong_argument_count_throws()
    {
        assertThrows(IllegalArgumentException.class, () -> function.execute(context, "mongo", emptyList(), new FunctionData(-1, emptyList())));
    }

    @Test
    void test_pipeline_stage_that_is_a_nested_array_is_rejected_with_a_clear_message()
    {
        // Reproduces a real user report: a stray nested array instead of a stage document used to pass an unchecked cast here
        // and only fail later as a confusing ClassCastException deep inside the driver's BSON encoding.
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression("mydb.mycollection"),
                ExpressionTestUtils.createStringExpression("[{}, [{\"$set\": {\"modifiedAt\": \"1900-01-01T00:00:00.000\"}}]]"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> function.execute(context, "mongo", arguments, new FunctionData(-1, emptyList())));
        assertTrue(e.getMessage()
                .contains("stage 2"));
        assertTrue(e.getMessage()
                .contains("ArrayList"));
    }

    @Test
    void test_pipeline_stage_that_is_a_scalar_is_rejected()
    {
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression("mydb.mycollection"), ExpressionTestUtils.createStringExpression("[{\"$match\": {}}, 123]"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> function.execute(context, "mongo", arguments, new FunctionData(-1, emptyList())));
        assertTrue(e.getMessage()
                .contains("stage 2"));
    }

    @Test
    void test_pipeline_that_is_not_an_array_is_rejected()
    {
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression("mydb.mycollection"), ExpressionTestUtils.createStringExpression("{\"$match\": {}}"));

        assertThrows(IllegalArgumentException.class, () -> function.execute(context, "mongo", arguments, new FunctionData(-1, emptyList())));
    }

    @Test
    void test_invalid_pipeline_json_is_rejected_with_a_clear_message()
    {
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression("mydb.mycollection"), ExpressionTestUtils.createStringExpression("not json at all"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> function.execute(context, "mongo", arguments, new FunctionData(-1, emptyList())));
        assertTrue(e.getMessage()
                .contains("not valid JSON"));
    }
}
