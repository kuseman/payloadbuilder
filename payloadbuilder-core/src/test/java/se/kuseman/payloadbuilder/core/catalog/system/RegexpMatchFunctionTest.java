package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link RegexpMatchFunction} */
class RegexpMatchFunctionTest extends APhysicalPlanTest
{
    private final ScalarFunctionInfo f = SystemCatalog.get()
            .getScalarFunction("regexp_match");

    @Test
    void test()
    {
        assertEquals(ResolvedType.array(Type.String), f.getType(emptyList()));

        IExpression col1 = ce("col1", ResolvedType.of(Type.String));
        IExpression col2 = ce("col2", ResolvedType.of(Type.Any));

      //@formatter:off
        Schema schema = Schema.of(
                Column.of("col1", ResolvedType.of(Type.String)),
                Column.of("col2", ResolvedType.of(Type.Any))
                );
                
        TupleVector input = TupleVector.of(schema, asList(
                // Value
                vv(Type.String,
                        null,
                        null,
                        "text",
                        "text",
                        "text",
                        "text",
                        "123-456-abc",
                        "123-456-abc 798-103-def",
                        "123-456-abc 798-103-def",
                        "123-456-abc 798-103-def",
                        "123-456-abc 798-103-def",
                        "123-456-abc 798-103-def"
                        ),
                // Regexp
                vv(Type.Any,
                        null,
                        "",
                        null,
                        "",
                        1234,
                        "nono",
                        "([0-9][0-9][0-9])",
                        "([0-9]{3})-[0-9]{3}-([a-z]{3})",
                        "^([0-9]{3})-[0-9]{3}.*$",
                        "[0-9]{3}-([0-9]{3})",
                        "[i-k]{3}-([i-k]{3})",
                        "([a-z]{3})"
                        )
                ));

        ValueVector actual = f.evalScalar(context, input, "", asList(col1, col2));
        assertVectorsEquals(vv(ResolvedType.array(Type.String),
                null,
                null,
                null,
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String, "123", "456"),
                vv(Type.String, "123", "abc", "798", "def"),
                vv(Type.String, "123"),
                vv(Type.String, "456", "103"),
                vv(Type.String),
                vv(Type.String, "abc", "def")
                ), actual);
        
        // Constant pattern
        actual = f.evalScalar(context, input, "", asList(col1, new LiteralStringExpression("([a-z]{3})")));
        assertVectorsEquals(vv(ResolvedType.array(Type.String),
                null,
                null,
                vv(Type.String, "tex"),
                vv(Type.String, "tex"),
                vv(Type.String, "tex"),
                vv(Type.String, "tex"),
                vv(Type.String, "abc"),
                vv(Type.String, "abc", "def"),
                vv(Type.String, "abc", "def"),
                vv(Type.String, "abc", "def"),
                vv(Type.String, "abc", "def"),
                vv(Type.String, "abc", "def")
                ), actual);
        
        // Constant value and pattern
        actual = f.evalScalar(context, input, "", asList(new LiteralStringExpression("123"), new LiteralStringExpression("([a-z]{3})")));
        assertVectorsEquals(vv(ResolvedType.array(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String),
                vv(Type.String)
                ), actual);
    }
}
