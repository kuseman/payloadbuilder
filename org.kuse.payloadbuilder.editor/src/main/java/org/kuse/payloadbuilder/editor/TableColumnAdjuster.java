package org.kuse.payloadbuilder.editor;

import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

class TableColumnAdjuster
{
    private final JTable table;
    private final int spacing;

    TableColumnAdjuster(JTable table)
    {
        this(table, 6);
    }

    TableColumnAdjuster(JTable table, int spacing)
    {
        this.table = table;
        this.spacing = spacing;
    }

    void adjustColumns(int maxWidth)
    {
        TableColumnModel tcm = table.getColumnModel();

        for (int i = 0; i < tcm.getColumnCount(); i++)
        {
            adjustColumn(i, maxWidth);
        }
    }

    /*
     *  Adjust the width of the specified column in the table
     */
    void adjustColumn(final int column, int maxWidth)
    {
        TableColumn tableColumn = table.getColumnModel().getColumn(column);
        if (!tableColumn.getResizable())
        {
            return;
        }

        int columnHeaderWidth = getColumnHeaderWidth(column);
        int columnDataWidth = getColumnDataWidth(column);
        int preferredWidth = Math.max(columnHeaderWidth, columnDataWidth);

        updateTableColumn(column, preferredWidth, maxWidth);
    }

    /*
     *  Calculated the width based on the column name
     */
    private int getColumnHeaderWidth(int column)
    {
        TableColumn tableColumn = table.getColumnModel().getColumn(column);
        Object value = tableColumn.getHeaderValue();
        TableCellRenderer renderer = tableColumn.getHeaderRenderer();

        if (renderer == null)
        {
            renderer = table.getTableHeader().getDefaultRenderer();
        }

        Component c = renderer.getTableCellRendererComponent(table, value, false, false, -1, column);
        return c.getPreferredSize().width;
    }

    private int getColumnDataWidth(int column)
    {
        int preferredWidth = 0;
        int maxWidth = table.getColumnModel().getColumn(column).getMaxWidth();
        for (int row = 0; row < table.getRowCount(); row++)
        {
            preferredWidth = Math.max(preferredWidth, getCellDataWidth(row, column));
            if (preferredWidth >= maxWidth)
            {
                break;
            }
        }

        return preferredWidth;
    }

    private int getCellDataWidth(int row, int column)
    {
        TableCellRenderer cellRenderer = table.getCellRenderer(row, column);
        Component c = table.prepareRenderer(cellRenderer, row, column);
        int width = c.getPreferredSize().width + table.getIntercellSpacing().width;

        return width;
    }

    private void updateTableColumn(int column, int width, int maxWidth)
    {
        TableColumn tableColumn = table.getColumnModel().getColumn(column);
        if (!tableColumn.getResizable())
        {
            return;
        }

        width += spacing;
        table.getTableHeader().setResizingColumn(tableColumn);
        tableColumn.setWidth(maxWidth != -1 ? Math.min(width, maxWidth) : width);
    }
}
