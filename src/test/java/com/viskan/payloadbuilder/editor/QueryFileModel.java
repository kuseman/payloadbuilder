package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;

import static java.util.stream.Collectors.toMap;

import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.swing.SwingUtilities;
import javax.swing.event.SwingPropertyChangeSupport;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Model of a query file. Has information about filename, execution state etc.
 **/
class QueryFileModel
{
    private final SwingPropertyChangeSupport pcs = new SwingPropertyChangeSupport(this, true);
    static final String RESULT_MODEL = "resultModel";
    static final String DIRTY = "dirty";
    static final String FILENAME = "filename";
    static final String QUERY = "query";
    static final String STATE = "state";

    private boolean dirty;
    private boolean newFile = true;
    private String filename = "";
    private State state = State.COMPLETED;
    /** Query before modifications */
    private String savedQuery = "";
    private String query = "";
    private Output output = Output.TABLE;

    private final QuerySession querySession = new QuerySession(new CatalogRegistry());
    private Map<ICatalogExtension, CatalogExtensionModel> catalogExtensions;

    /** Execution fields */
    private final List<ResultModel> results = new ArrayList<>();
    private StopWatch sw;
    private String error;
    private Pair<Integer, Integer> parseErrorLocation;

    QueryFileModel(List<ICatalogExtension> extensions)
    {
        init(extensions);
    }

    /** Initialize a file from filename */
    QueryFileModel(List<ICatalogExtension> extensions, File file)
    {
        this.filename = file.getAbsolutePath();
        try
        {
            query = FileUtils.readFileToString(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        dirty = false;
        newFile = false;
        savedQuery = query;

        init(extensions);
    }

    private void init(List<ICatalogExtension> extensions)
    {
        catalogExtensions = extensions.stream()
                .collect(toMap(Function.identity(), v -> new CatalogExtensionModel(v.getDefaultAlias())));
        // Set first extension as default
        if (extensions.size() > 0)
        {
            querySession.setDefaultCatalog(extensions.get(0).getDefaultAlias());
        }
    }

    boolean isDirty()
    {
        return dirty;
    }

    void setDirty(boolean dirty)
    {
        boolean oldValue = this.dirty;
        boolean newValue = dirty;
        if (newValue != oldValue)
        {
            this.dirty = dirty;
            pcs.firePropertyChange(DIRTY, oldValue, newValue);
        }
    }

    State getState()
    {
        return state;
    }

    void setState(State state)
    {
        State oldValue = this.state;
        State newValue = state;
        if (oldValue != newValue)
        {
            this.state = state;

            // Reset execution fields when starting
            if (state == State.EXECUTING)
            {
                sw = new StopWatch();
                sw.start();
                clearForExecution();
            }
            else if (sw.isStarted())
            {
                sw.stop();
            }

            // Let UI update correctly when chaning state of query model
            if (SwingUtilities.isEventDispatchThread())
            {
                pcs.firePropertyChange(STATE, oldValue, newValue);
            }
            else
            {
                try
                {
                    SwingUtilities.invokeAndWait(() -> pcs.firePropertyChange(STATE, oldValue, newValue));
                }
                catch (InvocationTargetException | InterruptedException e)
                {
                    throw new RuntimeException("Error updating state of query", e);
                }
            }
        }
    }

    /** Get current execution time in millis */
    long getExecutionTime()
    {
        return sw != null ? sw.getTime(TimeUnit.MILLISECONDS) : 0;
    }

    void clearForExecution()
    {
        error = "";
        parseErrorLocation = null;
        results.clear();
    }

    boolean isNew()
    {
        return newFile;
    }

    void setNew(boolean newFile)
    {
        this.newFile = newFile;
    }

    String getFilename()
    {
        return filename;
    }

    void setFilename(String filename)
    {
        String newValue = filename;
        String oldValue = this.filename;
        if (!newValue.equals(oldValue))
        {
            pcs.firePropertyChange(FILENAME, oldValue, newValue);
            this.filename = filename;
        }
    }

    /** Saves file to disk */
    void save()
    {
        try
        {
            FileUtils.write(new File(filename), query);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to save file " + filename, e);
        }
        newFile = false;
        savedQuery = query;
        setDirty(false);
    }

    String getQuery()
    {
        return query;
    }

    void setQuery(String query)
    {
        String newValue = query;
        String oldValue = this.query;
        if (!newValue.equals(oldValue))
        {
            pcs.firePropertyChange(QUERY, oldValue, newValue);
            this.query = query;
        }

        setDirty(!Objects.equals(query, savedQuery));
    }

    void addPropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }

    Output getOutput()
    {
        return output;
    }

    void setOutput(Output output)
    {
        this.output = output;
    }

    String getError()
    {
        return error;
    }

    void setError(String error)
    {
        this.error = error;
    }

    Pair<Integer, Integer> getParseErrorLocation()
    {
        return parseErrorLocation;
    }

    void setParseErrorLocation(Pair<Integer, Integer> parseErrorLocation)
    {
        this.parseErrorLocation = parseErrorLocation;
    }

    String getTabTitle()
    {
        String filename = FilenameUtils.getName(getFilename());
        StringBuilder sb = new StringBuilder();
        if (isDirty())
        {
            sb.append("*");
        }
        sb.append(filename);
        if (getState() == State.EXECUTING)
        {
            sb.append(" Executing ...");
        }
        return sb.toString();
    }

    QuerySession getQuerySession()
    {
        return querySession;
    }

    List<ResultModel> getResults()
    {
        return results;
    }

    void addResult(ResultModel model)
    {
        results.add(model);
        if (output != Output.NONE)
        {
            pcs.firePropertyChange(RESULT_MODEL, null, model);
        }
    }

    public Map<ICatalogExtension, CatalogExtensionModel> getCatalogExtensions()
    {
        return catalogExtensions;
    }

    /** Execution state of this file */
    enum State
    {
        COMPLETED("Stopped"),
        EXECUTING("Executing"),
        ABORTED("Aborted"),
        ERROR("Error");

        String tooltip;

        State(String tooltip)
        {
            this.tooltip = tooltip;
        }

        public String getToolTip()
        {
            return null;
        }
    }

    /** Current output of this file */
    enum Output
    {
        TABLE,
        FILE,
        NONE;
    }
}
