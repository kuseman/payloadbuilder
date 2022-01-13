package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;

import org.junit.Test;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.codegen.CodeGenerator;
import org.kuse.payloadbuilder.core.parser.AParserTest;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

/** Test of {@link ExpressionProjection} */
public class ExpressionProjectionTest extends AParserTest
{
    private final CodeGenerator generator = new CodeGenerator();

    @Test
    public void test()
    {
        QualifiedReferenceExpression qre;
        ExpressionProjection p;

        // Object.class QRE
        qre = new QualifiedReferenceExpression(QualifiedName.of("col"), -1, new ResolvePath[0], null);
        p = new ExpressionProjection(qre);

        OutputWriter writer = generateAndWrite(p, tuple -> when(tuple.getValue(0)).thenReturn("value"));
        verify(writer).startObject();
        verify(writer).writeFieldName("col");
        verify(writer).writeValue("value");
        verify(writer).endObject();
        verifyNoMoreInteractions(writer);

        for (DataType type : DataType.values())
        {
            if (!type.isDefinedValueType())
            {
                continue;
            }
            testPrimitive(false, type);
            testPrimitive(true, type);
        }
    }

    private void testPrimitive(boolean nullResult, DataType type)
    {
        QualifiedReferenceExpression qre = new QualifiedReferenceExpression(
                QualifiedName.of("col"),
                -1,
                new ResolvePath[] {
                        new ResolvePath(-1, -1, asList("col"), -1, type)
                },
                null);
        Projection p = new ExpressionProjection(qre);

        OutputWriter writer = generateAndWrite(p, tuple ->
        {
            if (nullResult)
            {
                when(tuple.isNull(0)).thenReturn(true);
            }
            else if (type == DataType.INT)
            {
                when(tuple.getInt(0)).thenReturn(123);
            }
            else if (type == DataType.LONG)
            {
                when(tuple.getLong(0)).thenReturn(456L);
            }
            else if (type == DataType.FLOAT)
            {
                when(tuple.getFloat(0)).thenReturn(789f);
            }
            else if (type == DataType.DOUBLE)
            {
                when(tuple.getDouble(0)).thenReturn(101112d);
            }
            else if (type == DataType.BOOLEAN)
            {
                when(tuple.getBool(0)).thenReturn(true);
            }
            else
            {
                throw new IllegalArgumentException("Invalid primitive type " + type);
            }
        });
        verify(writer).startObject();
        verify(writer).writeFieldName("col");
        if (nullResult)
        {
            verify(writer).writeValue(null);
        }
        else if (type == DataType.INT)
        {
            verify(writer).writeInt(123);
        }
        else if (type == DataType.LONG)
        {
            verify(writer).writeLong(456L);
        }
        else if (type == DataType.FLOAT)
        {
            verify(writer).writeFloat(789f);
        }
        else if (type == DataType.DOUBLE)
        {
            verify(writer).writeDouble(101112d);
        }
        else if (type == DataType.BOOLEAN)
        {
            verify(writer).writeBool(true);
        }
        verify(writer).endObject();
        verifyNoMoreInteractions(writer);
    }

    private OutputWriter generateAndWrite(Projection p, Consumer<Tuple> c)
    {
        RootProjection r = new RootProjection(asList("col"), asList(p));
        Projection projection = generator.generateProjection(r);

        Tuple tuple = mock(Tuple.class);
        when(tuple.getColumnOrdinal("col")).thenReturn(0);
        c.accept(tuple);
        OutputWriter writer = mock(OutputWriter.class);
        context.getStatementContext().setTuple(tuple);
        projection.writeValue(writer, context);
        return writer;
    }
}
