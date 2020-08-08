package org.kuse.payloadbuilder.editor;

import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

/*
 *  Class to manage the widths of colunmns in a table.
 *
 *  Various properties control how the width of the column is calculated.
 *  Another property controls whether column width calculation should be dynamic.
 *  Finally, various Actions will be added to the table to allow the user
 *  to customize the functionality.
 *
 *  This class was designed to be used with tables that use an auto resize mode
 *  of AUTO_RESIZE_OFF. With all other modes you are constrained as the width
 *  of the columns must fit inside the table. So if you increase one column, one
 *  or more of the other columns must decrease. Because of this the resize mode
 *  of RESIZE_ALL_COLUMNS will work the best.
 */
class TableColumnAdjuster// implements PropertyChangeListener, TableModelListener
{
    private final JTable table;
    private final int spacing;
    private boolean isColumnHeaderIncluded;
    private boolean isColumnDataIncluded;
    private boolean isOnlyAdjustLarger;

    /*
     *  Specify the table and use default spacing
     */
    TableColumnAdjuster(JTable table)
    {
        this(table, 6);
    }

    /*
     *  Specify the table and spacing
     */
    TableColumnAdjuster(JTable table, int spacing)
    {
        this.table = table;
        this.spacing = spacing;
        setColumnHeaderIncluded(true);
        setColumnDataIncluded(true);
        setOnlyAdjustLarger(false);
    }
    
    /*
     *  Indicates whether to include the header in the width calculation
     */
    public void setColumnHeaderIncluded(boolean isColumnHeaderIncluded)
    {
        this.isColumnHeaderIncluded = isColumnHeaderIncluded;
    }

    /*
     *  Indicates whether to include the model data in the width calculation
     */
    public void setColumnDataIncluded(boolean isColumnDataIncluded)
    {
        this.isColumnDataIncluded = isColumnDataIncluded;
    }

    /*
     *  Indicates whether columns can only be increased in size
     */
    public void setOnlyAdjustLarger(boolean isOnlyAdjustLarger)
    {
        this.isOnlyAdjustLarger = isOnlyAdjustLarger;
    }

    /*
     *  Adjust the widths of all the columns in the table
     */
    public void adjustColumns(int maxWidth)
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
    public void adjustColumn(final int column, int maxWidth)
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
        if (!isColumnHeaderIncluded)
        {
            return 0;
        }

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

    /*
     *  Calculate the width based on the widest cell renderer for the
     *  given column.
     */
    private int getColumnDataWidth(int column)
    {
        if (!isColumnDataIncluded)
        {
            return 0;
        }

        int preferredWidth = 0;
        int maxWidth = table.getColumnModel().getColumn(column).getMaxWidth();

        for (int row = 0; row < table.getRowCount(); row++)
        {
            preferredWidth = Math.max(preferredWidth, getCellDataWidth(row, column));

            //  We've exceeded the maximum width, no need to check other rows

            if (preferredWidth >= maxWidth)
            {
                break;
            }
        }

        return preferredWidth;
    }

    /*
     *  Get the preferred width for the specified cell
     */
    private int getCellDataWidth(int row, int column)
    {
        //  Inovke the renderer for the cell to calculate the preferred width

        TableCellRenderer cellRenderer = table.getCellRenderer(row, column);
        Component c = table.prepareRenderer(cellRenderer, row, column);
        int width = c.getPreferredSize().width + table.getIntercellSpacing().width;

        return width;
    }

    /*
     *  Update the TableColumn with the newly calculated width
     */
    private void updateTableColumn(int column, int width, int maxWidth)
    {
        final TableColumn tableColumn = table.getColumnModel().getColumn(column);

        if (!tableColumn.getResizable())
        {
            return;
        }

        width += spacing;

        //  Don't shrink the column width

        if (isOnlyAdjustLarger)
        {
            width = Math.max(width, tableColumn.getPreferredWidth());
        }

        table.getTableHeader().setResizingColumn(tableColumn);
        tableColumn.setWidth(maxWidth != -1 ? Math.min(width, maxWidth) : width);
    }
}
