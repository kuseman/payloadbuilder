package org.kuse.payloadbuilder.editor;

import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JTable;
import javax.swing.TransferHandler;
import javax.swing.plaf.UIResource;

/** Transfer handler for query table */
class TableTransferHandler extends TransferHandler implements UIResource
{
    private static final DataFlavor[] FLAVORS;

    static
    {
        //CSOFF
        FLAVORS = new DataFlavor[8];
        try
        {
            FLAVORS[0] = new DataFlavor("text/html;class=java.lang.String");
            FLAVORS[1] = new DataFlavor("text/html;class=java.io.Reader");
            FLAVORS[2] = new DataFlavor("text/html;charset=unicode;class=java.io.InputStream");

            FLAVORS[3] = new DataFlavor("text/plain;class=java.lang.String");
            FLAVORS[4] = new DataFlavor("text/plain;class=java.io.Reader");
            FLAVORS[5] = new DataFlavor("text/plain;charset=unicode;class=java.io.InputStream");

            FLAVORS[6] = new DataFlavor("plb/sql-in;class=java.lang.String");
            FLAVORS[7] = new DataFlavor("plb/sql-in-new-line;class=java.lang.String");
            //CSON
        }
        catch (Exception e)
        {
            System.err.println("Error initalizing data flavours for JTable TransferHandler. " + e);
        }
    }

    /**
     * Create a Transferable to use as the source for a data transfer.
     *
     * @param c The component holding the data to be transfered. This argument is provided to enable sharing of TransferHandlers by multiple
     *            components.
     * @return The representation of the data to be transfered.
     */
    @Override
    protected Transferable createTransferable(JComponent c)
    {
        if (c instanceof JTable)
        {
            // Default action includes headers
            return generate((JTable) c, true);
        }

        return null;
    }

    @Override
    public int getSourceActions(JComponent c)
    {
        return COPY;
    }

    /** Create a data transferable from provided jtable */
    //CSOFF
    static DataTransferable generate(JTable table, boolean includeHeaders)
    //CSON
    {
        int[] rows;
        int[] cols;

        if (!table.getRowSelectionAllowed() && !table.getColumnSelectionAllowed())
        {
            return null;
        }

        if (!table.getRowSelectionAllowed())
        {
            int rowCount = table.getRowCount();

            rows = new int[rowCount];
            for (int counter = 0; counter < rowCount; counter++)
            {
                rows[counter] = counter;
            }
        }
        else
        {
            rows = table.getSelectedRows();
        }

        if (!table.getColumnSelectionAllowed())
        {
            int colCount = table.getColumnCount();

            cols = new int[colCount];
            for (int counter = 0; counter < colCount; counter++)
            {
                cols[counter] = counter;
            }
        }
        else
        {
            cols = table.getSelectedColumns();
        }

        if (rows == null || cols == null || rows.length == 0 || cols.length == 0)
        {
            return null;
        }

        String[] headerNames = new String[cols.length];
        List<Object[]> rowsValues = new ArrayList<>(rows.length);

        for (int col = 0; col < cols.length; col++)
        {
            String columnName = table.getColumnName(cols[col]);
            if (!includeHeaders)
            {
                columnName = "";
            }
            headerNames[col] = columnName;
        }

        for (int row = 0; row < rows.length; row++)
        {
            Object[] values = new Object[cols.length];
            rowsValues.add(values);
            for (int col = 0; col < cols.length; col++)
            {
                Object obj = table.getValueAt(rows[row], cols[col]);
                values[col] = obj;
            }
        }

        return new DataTransferable(headerNames, rowsValues);
    }

    /** Trsnaferable */
    static class DataTransferable implements Transferable
    {
        private final String[] headerNames;
        private final List<Object[]> rowsValues;

        DataTransferable(String[] headerNames, List<Object[]> rowsValues)
        {
            this.headerNames = headerNames;
            this.rowsValues = rowsValues;
        }

        @Override
        public boolean isDataFlavorSupported(DataFlavor flavor)
        {
            for (int i = 0; i < FLAVORS.length; i++)
            {
                if (FLAVORS[i].equals(flavor))
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public DataFlavor[] getTransferDataFlavors()
        {
            return FLAVORS;
        }

        @Override
        public Object getTransferData(DataFlavor flavor) throws UnsupportedFlavorException, IOException
        {
            String mimeType = flavor.getMimeType();
            if (mimeType.contains("html"))
            {
                return toHtml();
            }
            else if (mimeType.contains("sql-in-new-line"))
            {
                return toSqlIn(true);
            }
            else if (mimeType.contains("sql-in"))
            {
                return toSqlIn(false);
            }

            return toPlain();
        }

        private String toSqlIn(boolean newLines)
        {
            int rows = rowsValues.size();
            StringBuffer sqlIn = new StringBuffer();

            for (int row = 0; row < rows; row++)
            {
                Object[] values = rowsValues.get(row);

                // In uses first copied column only
                Object obj = values[0];
                String val = (obj == null) ? "" : obj.toString();
                if (!(obj instanceof Number))
                {
                    sqlIn.append('\'').append(val).append('\'');
                }
                else
                {
                    sqlIn.append(val);
                }
                if (row < rows - 1)
                {
                    sqlIn.append(",");
                    if (newLines)
                    {
                        sqlIn.append(System.lineSeparator());
                    }
                }
            }
            return sqlIn.toString();
        }

        private String toHtml()
        {
            int cols = headerNames.length;
            int rows = rowsValues.size();

            StringBuffer htmlBuf = new StringBuffer();

            htmlBuf.append("<html>\n<body>\n<table>\n");
            htmlBuf.append("<tr>\n");
            for (int col = 0; col < cols; col++)
            {
                String columnName = headerNames[col];
                htmlBuf.append("  <th>").append(columnName).append("</th>\n");
            }
            htmlBuf.append("</tr>\n");

            for (int row = 0; row < rows; row++)
            {
                Object[] values = rowsValues.get(row);
                for (int col = 0; col < cols; col++)
                {
                    Object obj = values[col];
                    String val = (obj == null) ? "" : obj.toString();
                    htmlBuf.append("  <td>" + val + "</td>\n");
                }
                htmlBuf.append("</tr>\n");
            }
            htmlBuf.append("</table>\n</body>\n</html>");
            return htmlBuf.toString();
        }

        private String toPlain()
        {
            int cols = headerNames.length;
            int rows = rowsValues.size();

            StringBuffer plainBuf = new StringBuffer();
            for (int row = 0; row < rows; row++)
            {
                Object[] values = rowsValues.get(row);
                for (int col = 0; col < cols; col++)
                {
                    Object obj = values[col];
                    String val = (obj == null) ? "" : obj.toString();

                    plainBuf.append(val + "\t");
                }
                // we want a newline at the end of each line and not a tab
                plainBuf.deleteCharAt(plainBuf.length() - 1).append("\n");
            }
            return plainBuf.toString();
        }
    }
}
