package se.kuseman.payloadbuilder.core.execution;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.io.StringWriter;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.JsonOutputWriter;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link OutputWriterUtils} */
public class OutputWriterUtilsTest extends APhysicalPlanTest
{
    @Test
    public void test_write_tuplevector_root()
    {
        //@formatter:off
        TupleVector vector = TupleVector.of(Schema.of(
                Column.of("col3", ResolvedType.array(ResolvedType.of(Type.Any))),
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)),
                Column.of("col6", ResolvedType.of(Type.Float)),
                Column.of("col7", ResolvedType.of(Type.Double)),
                Column.of("col8", ResolvedType.of(Type.String))),
                asList(
                        vv(ResolvedType.array(ResolvedType.of(Type.Any)), vv(ResolvedType.of(Type.Any), 5, 6), vv(ResolvedType.of(Type.Any), 6, 7)),
                        vv(ResolvedType.of(Type.Boolean), false, true),
                        vv(ResolvedType.of(Type.Long), 1L, 2L),
                        vv(ResolvedType.of(Type.Float), 10.1F, 20.2F),
                        vv(ResolvedType.of(Type.Double), 100.10D, 200.20D),
                        vv(ResolvedType.of(Type.String), "one", "two")));
        //@formatter:on

        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        OutputWriterUtils.write(vector, w, context, true);
        w.close();

        assertEquals("{\"col3\":[5,6],\"col4\":false,\"col5\":1,\"col6\":10.1,\"col7\":100.1,\"col8\":\"one\"} {\"col3\":[6,7],\"col4\":true,\"col5\":2,\"col6\":20.2,\"col7\":200.2,\"col8\":\"two\"}",
                writer.toString());
    }

    @Test
    public void test_tuplevector_write()
    {
        //@formatter:off
        TupleVector vector = TupleVector.of(Schema.of(
                Column.of("col3", ResolvedType.array(ResolvedType.of(Type.Any))),
                Column.of("col4", ResolvedType.of(Type.Boolean)),
                Column.of("col5", ResolvedType.of(Type.Long)),
                Column.of("col6", ResolvedType.of(Type.Float)),
                Column.of("col7", ResolvedType.of(Type.Double)),
                Column.of("col8", ResolvedType.of(Type.String))),
                asList(
                        vv(ResolvedType.array(ResolvedType.of(Type.Any)), vv(ResolvedType.of(Type.Any), 5, 6), vv(ResolvedType.of(Type.Any), 6, 7)),
                        vv(ResolvedType.of(Type.Boolean), false, true),
                        vv(ResolvedType.of(Type.Long), 1L, 2L),
                        vv(ResolvedType.of(Type.Float), 10.1F, 20.2F),
                        vv(ResolvedType.of(Type.Double), 100.10D, 200.20D),
                        vv(ResolvedType.of(Type.String), "one", "two")));
        //@formatter:on

        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        OutputWriterUtils.write(vector, w, context, false);
        w.close();

        assertEquals(
                "[{\"col3\":[5,6],\"col4\":false,\"col5\":1,\"col6\":10.1,\"col7\":100.1,\"col8\":\"one\"},{\"col3\":[6,7],\"col4\":true,\"col5\":2,\"col6\":20.2,\"col7\":200.2,\"col8\":\"two\"}]",
                writer.toString());
    }

    @Test
    public void test_valuevector_write()
    {
        ValueVector vector = ValueVector.literalInt(10, 5);

        StringWriter writer = new StringWriter();
        JsonOutputWriter w = new JsonOutputWriter(writer);
        OutputWriterUtils.write(vector, w, context);
        w.close();

        assertEquals("[10,10,10,10,10]", writer.toString());

        vector = ValueVector.literalNull(ResolvedType.of(Type.Int), 5);

        writer = new StringWriter();
        w = new JsonOutputWriter(writer);
        OutputWriterUtils.write(vector, w, context);
        w.close();

        assertEquals("[null,null,null,null,null]", writer.toString());
    }
}
