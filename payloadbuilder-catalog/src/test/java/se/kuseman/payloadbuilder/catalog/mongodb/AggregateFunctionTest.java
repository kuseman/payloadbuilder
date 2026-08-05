package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static se.kuseman.payloadbuilder.catalog.TestUtils.mockExecutionContext;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.FunctionData;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/**
 * Unit tests of {@link AggregateFunction}.
 *
 * <pre>
 * NOTE! Only argument validation is unit tested here - actually running a pipeline requires a live MongoClient and is covered by the testcontainers based integration tests instead.
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
}
