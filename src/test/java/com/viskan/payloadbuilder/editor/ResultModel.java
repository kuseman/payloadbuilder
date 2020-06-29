package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.editor.QueryFileModel.Output;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Resulting model of a query */
class ResultModel extends AbstractTableModel
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Notify changes eveth x:th row */
    private static final int NOTIFY_ROWS_THRESHOLD = 1000;
    private final QueryFileModel file;
    private List<Object[]> rows = new ArrayList<>(50);
    private String[] columns = EMPTY_STRING_ARRAY;
    
    private int lastNotifyRowIndex = 0;
    
    ResultModel(QueryFileModel file)
    {
        this.file = file;
    }
    
    /** Add row */
    void addRow(Object[] row)
    {
        rows.add(row);
        if (file.getOutput() == Output.NONE)
        {
            return;
        }

        if (rows.size() >= lastNotifyRowIndex + NOTIFY_ROWS_THRESHOLD)
        {
            fireRowsInserted(lastNotifyRowIndex, lastNotifyRowIndex + NOTIFY_ROWS_THRESHOLD - 1);
            lastNotifyRowIndex = rows.size();
        }
    }
    
    /** Called when result is completed. */
    void done()
    {
        if (file.getOutput() == Output.NONE)
        {
            return;
        }

        if (rows.size() - 1 >= lastNotifyRowIndex)
        {
            fireRowsInserted(lastNotifyRowIndex, rows.size() - 1);
        }
    }
    
    /** Set columns */
    void setColumns(String[] columns)
    {
        this.columns = requireNonNull(columns);
        SwingUtilities.invokeLater(() -> fireTableStructureChanged());
    }

    /** Clear model */
    void clear()
    {
        columns = EMPTY_STRING_ARRAY;
        rows = new ArrayList<>(50);
        lastNotifyRowIndex = 0;
        
        if (SwingUtilities.isEventDispatchThread())
        {        
            fireTableStructureChanged();
            fireTableDataChanged();
        }
        else
        {
            SwingUtilities.invokeLater(() -> 
            {
                fireTableStructureChanged();
                fireTableDataChanged();
            });
        }
    }
    
    /** A non table model method to get row count */
    int getActualRowCOunt()
    {
        return rows.size();
    }
    
    @Override
    public int getRowCount()
    {
        if (file.getOutput() == Output.NONE)
        {
            return 0;
        }

        return rows.size();
    }

    @Override
    public int getColumnCount()
    {
        return columns.length;
    }
    
    @Override
    public String getColumnName(int column)
    {
        return columns[column];
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return rows.get(rowIndex)[columnIndex];
    }
    
    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex)
    {
        return false;
    }
    
    private void fireRowsInserted(int rowStart, int rowEnd)
    {
        SwingUtilities.invokeLater(() -> 
        {
            super.fireTableRowsInserted(rowStart, rowEnd);
        });
    }
    
    /** Get cell label for provided object.
     * Produces a minimal json for array and map objects
     */
    static String getLabel(Object value, int size)
    {
        StringWriter sw = new StringWriter(size);
        try(JsonGenerator generator = MAPPER.getFactory().createGenerator(sw))
        {
            if (value instanceof List)
            {
                generator.writeStartArray();
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) value;
                
                for (Object obj : list)
                {
                    generator.writeObject(obj);
                    if (sw.getBuffer().length() > size)
                    {
                        sw.append("...");
                        return sw.toString();
                    }
                }
            }
            else
            {
                generator.writeObject(value);
            }
            
        }
        catch (IOException e)
        {}
        
        return sw.getBuffer().toString();
    }

    /** Return pretty json for provided value */
    static String getPrettyJson(Object value)
    {
        try
        {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
        }
        catch (JsonProcessingException e)
        {
            return StringUtils.EMPTY;
        }
    }
}
