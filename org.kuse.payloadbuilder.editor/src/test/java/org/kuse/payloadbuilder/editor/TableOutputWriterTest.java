package org.kuse.payloadbuilder.editor;

import static java.util.Collections.emptyList;

import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.editor.PayloadbuilderService.TableOutputWriter;

/** Test {@link TableOutputWriter} */
public class TableOutputWriterTest extends Assert
{
    @Test
    public void test_column_expanding()
    {
        ResultModel model = new ResultModel(new QueryFileModel(emptyList()));
        TableOutputWriter ow = new TableOutputWriter();
        ow.setResultModel(model);

        ow.startRow();
        ow.endRow();

        // First row
        ow.startRow();
        ow.startObject();
        ow.writeFieldName("col1");
        ow.writeValue(123);
        ow.endObject();
        ow.endRow();

        String[] actual = model.getColumns();
        assertArrayEquals(actual, new String[] {"", "col1"});
        assertArrayEquals(new Object[] {1, 123}, row(model, 0));

        // Second row
        ow.startRow();
        ow.startObject();
        ow.writeFieldName("col1");
        ow.writeValue(456);
        ow.endObject();
        ow.endRow();

        // Assert that the columns hasn't be changed
        assertSame(actual, model.getColumns());
        assertArrayEquals(new Object[] {1, 123}, row(model, 0));
        assertArrayEquals(new Object[] {2, 456}, row(model, 1));

        // Third row, new column last
        ow.startRow();
        ow.startObject();
        ow.writeFieldName("col1");
        ow.writeValue(789);
        ow.writeFieldName("col2");
        ow.writeValue("hello");
        ow.endObject();
        ow.endRow();

        assertArrayEquals(model.getColumns(), new String[] {"", "col1", "col2"});
        assertArrayEquals(new Object[] {1, 123, null}, row(model, 0));
        assertArrayEquals(new Object[] {2, 456, null}, row(model, 1));
        assertArrayEquals(new Object[] {3, 789, "hello"}, row(model, 2));

        // Fourth row, new column in the middle
        ow.startRow();
        ow.startObject();
        ow.writeFieldName("col1");
        ow.writeValue(1337);
        ow.writeFieldName("newOne");
        ow.writeValue(true);
        ow.writeFieldName("col2");
        ow.writeValue("world");
        ow.endObject();
        ow.endRow();

        assertArrayEquals(model.getColumns(), new String[] {"", "col1", "newOne", "col2"});
        assertArrayEquals(new Object[] {1, 123, null, null}, row(model, 0));
        assertArrayEquals(new Object[] {2, 456, null, null}, row(model, 1));
        assertArrayEquals(new Object[] {3, 789, null, "hello"}, row(model, 2));
        assertArrayEquals(new Object[] {4, 1337, true, "world"}, row(model, 3));

        // Fifth row, less columns than previous rows
        ow.startRow();
        ow.startObject();
        ow.writeFieldName("col1");
        ow.writeValue(99999);
        ow.writeFieldName("col2");
        ow.writeValue("666");
        ow.endObject();
        ow.endRow();

        assertArrayEquals(model.getColumns(), new String[] {"", "col1", "newOne", "col2"});
        assertArrayEquals(new Object[] {1, 123, null, null}, row(model, 0));
        assertArrayEquals(new Object[] {2, 456, null, null}, row(model, 1));
        assertArrayEquals(new Object[] {3, 789, null, "hello"}, row(model, 2));
        assertArrayEquals(new Object[] {4, 1337, true, "world"}, row(model, 3));
        assertArrayEquals(new Object[] {5, 99999, null, "666"}, row(model, 4));
    }

    private Object[] row(ResultModel model, int row)
    {
        Object[] result = new Object[model.getColumnCount()];
        for (int i = 0; i < result.length; i++)
        {
            result[i] = model.getValueAt(row, i);
        }
        return result;
    }
}
