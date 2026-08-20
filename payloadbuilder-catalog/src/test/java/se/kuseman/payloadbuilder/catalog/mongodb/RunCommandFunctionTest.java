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
 * Unit tests of {@link RunCommandFunction}.
 *
 * <pre>
 * NOTE! Only argument validation is unit tested here - actually running a command requires a live MongoClient and is covered by the testcontainers based integration tests instead.
 * </pre>
 */
class RunCommandFunctionTest
{
    private final RunCommandFunction function = new RunCommandFunction(new MongoCatalog());
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
    void test_invalid_command_json_is_rejected_with_a_clear_message()
    {
        List<IExpression> arguments = List.of(ExpressionTestUtils.createStringExpression("mydb"), ExpressionTestUtils.createStringExpression("not json at all"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> function.execute(context, "mongo", arguments, new FunctionData(-1, emptyList())));
        assertTrue(e.getMessage()
                .contains("not a valid JSON object"));
    }
}
