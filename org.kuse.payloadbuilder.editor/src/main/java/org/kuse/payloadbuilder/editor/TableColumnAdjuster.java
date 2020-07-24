package org.kuse.payloadbuilder.editor;

import java.awt.Component;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableModel;

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
class TableColumnAdjuster implements PropertyChangeListener, TableModelListener
{
    private final JTable table;
    private final int spacing;
    private boolean isColumnHeaderIncluded;
    private boolean isColumnDataIncluded;
    private boolean isOnlyAdjustLarger;
    private boolean isDynamicAdjustment;
    private final int maxWidth;
    //    private final Map<TableColumn, Integer> columnSizes = new HashMap<>();

    /*
     *  Specify the table and use default spacing
     */
    TableColumnAdjuster(JTable table)
    {
        this(table, 6, -1);
    }

    /*
     *  Specify the table and spacing
     */
    TableColumnAdjuster(JTable table, int spacing, int maxWidth)
    {
        this.table = table;
        this.spacing = spacing;
        this.maxWidth = maxWidth;
        setColumnHeaderIncluded(true);
        setColumnDataIncluded(true);
        setOnlyAdjustLarger(false);
        setDynamicAdjustment(false);
    }

    /*
     *  Adjust the widths of all the columns in the table
     */
    public void adjustColumns()
    {
        TableColumnModel tcm = table.getColumnModel();

        for (int i = 0; i < tcm.getColumnCount(); i++)
        {
            adjustColumn(i);
        }
    }

    /*
     *  Adjust the width of the specified column in the table
     */
    public void adjustColumn(final int column)
    {
        TableColumn tableColumn = table.getColumnModel().getColumn(column);

        if (!tableColumn.getResizable())
        {
            return;
        }

        int columnHeaderWidth = getColumnHeaderWidth(column);
        int columnDataWidth = getColumnDataWidth(column);
        int preferredWidth = Math.max(columnHeaderWidth, columnDataWidth);

        updateTableColumn(column, preferredWidth);
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
    private void updateTableColumn(int column, int width)
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
     *  Indicate whether changes to the model should cause the width to be
     *  dynamically recalculated.
     */
    public void setDynamicAdjustment(boolean isDynamicAdjustment)
    {
        //  May need to add or remove the TableModelListener when changed

        if (this.isDynamicAdjustment != isDynamicAdjustment)
        {
            if (isDynamicAdjustment)
            {
                table.addPropertyChangeListener(this);
                table.getModel().addTableModelListener(this);
            }
            else
            {
                table.removePropertyChangeListener(this);
                table.getModel().removeTableModelListener(this);
            }
        }

        this.isDynamicAdjustment = isDynamicAdjustment;
    }

    //
    //  Implement the PropertyChangeListener
    //
    @Override
    public void propertyChange(PropertyChangeEvent e)
    {
        //  When the TableModel changes we need to update the listeners
        //  and column widths

        if ("model".equals(e.getPropertyName()))
        {
            TableModel model = (TableModel) e.getOldValue();
            model.removeTableModelListener(this);

            model = (TableModel) e.getNewValue();
            model.addTableModelListener(this);
            adjustColumns();
        }
    }

    //
    //  Implement the TableModelListener
    //
    @Override
    public void tableChanged(TableModelEvent e)
    {
        if (!isColumnDataIncluded)
        {
            return;
        }

        //  Needed when table is sorted.

        SwingUtilities.invokeLater(() ->
        {
            //  A cell has been updated

            int column = table.convertColumnIndexToView(e.getColumn());

            if (e.getType() == TableModelEvent.UPDATE && column != -1)
            {
                //  Only need to worry about an increase in width for this cell

                if (isOnlyAdjustLarger)
                {
                    int row = e.getFirstRow();
                    TableColumn tableColumn = table.getColumnModel().getColumn(column);

                    if (tableColumn.getResizable())
                    {
                        int width = getCellDataWidth(row, column);
                        updateTableColumn(column, width);
                    }
                }

                //  Could be an increase of decrease so check all rows

                else
                {
                    adjustColumn(column);
                }
            }

            //  The update affected more than one column so adjust all columns

            else
            {
                adjustColumns();
            }
        });
    }
}
