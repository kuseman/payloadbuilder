package se.kuseman.payloadbuilder.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext.VectorWriterFormat;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.VectorWriter;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link VectorWriter}'s */
class VectorWriterTest
{
    private ExecutionContext context = new ExecutionContext(new QuerySession(new CatalogRegistry()));
    private TupleVector vector = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
            List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));

    @Test
    void test_text() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorWriter vectorWriter = context.getVectorWriter(VectorWriterFormat.TEXT, baos, Collections.emptyList()))
        {
            vectorWriter.write(vector);
        }
        Assertions.assertEquals("""
                intCol stringCol
                10 ten
                20 twenty
                30 thirty åäö
                """, new String(baos.toByteArray(), StandardCharsets.UTF_8).replaceAll("\\r", ""));
    }

    @Test
    void test_csv() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorWriter vectorWriter = context.getVectorWriter(VectorWriterFormat.CSV, baos, Collections.emptyList()))
        {
            vectorWriter.write(vector);
        }
        Assertions.assertEquals("""
                intCol,stringCol
                10,ten
                20,twenty
                30,thirty åäö
                """, new String(baos.toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void test_csv_escape() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorWriter vectorWriter = context.getVectorWriter(VectorWriterFormat.CSV, baos, List.of(new Option(QualifiedName.of("separatorchar"), new LiteralStringExpression(";")))))
        {
            vectorWriter.write(vector);
        }
        assertEquals("""
                intCol;stringCol
                10;ten
                20;twenty
                30;thirty åäö
                """, new String(baos.toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void test_json() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorWriter vectorWriter = context.getVectorWriter(VectorWriterFormat.JSON, baos, Collections.emptyList()))
        {
            vectorWriter.write(vector);
        }
        Assertions.assertEquals("[{\"intCol\":10,\"stringCol\":\"ten\"},{\"intCol\":20,\"stringCol\":\"twenty\"},{\"intCol\":30,\"stringCol\":\"thirty åäö\"}]",
                new String(baos.toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void test_json_pretty() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorWriter vectorWriter = context.getVectorWriter(VectorWriterFormat.JSON, baos, List.of(new Option(QualifiedName.of("prettyprint"), LiteralBooleanExpression.TRUE))))
        {
            vectorWriter.write(vector);
        }
        Assertions.assertEquals("""
                [ {
                  "intCol" : 10,
                  "stringCol" : "ten"
                }, {
                  "intCol" : 20,
                  "stringCol" : "twenty"
                }, {
                  "intCol" : 30,
                  "stringCol" : "thirty åäö"
                } ]
                """, new String(baos.toByteArray(), StandardCharsets.UTF_8).replaceAll("\\r", "") + "\n");
    }
}
