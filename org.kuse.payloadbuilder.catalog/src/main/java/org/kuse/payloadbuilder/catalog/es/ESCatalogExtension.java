package org.kuse.payloadbuilder.catalog.es;

import static org.kuse.payloadbuilder.catalog.es.ESOperator.CLIENT;
import static org.kuse.payloadbuilder.catalog.es.ESOperator.MAPPER;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.editor.AutoCompletionComboBox;
import org.kuse.payloadbuilder.editor.ICatalogExtension;

/** Editor extension for {@link ESCatalog} */
class ESCatalogExtension implements ICatalogExtension
{
    private static final String ENDPOINTS = "endpoints";
    private static final Catalog CATALOG = new ESCatalog();

    private final Map<String, Object> properties = new HashMap<>();
    private final PropertyChangeSupport pcs = new PropertyChangeSupport(this);
    private final QuickPropertiesPanel quickPropertiesPanel;
    private final ConfigPanel configPanel;
    
    ESCatalogExtension()
    {
        quickPropertiesPanel = new QuickPropertiesPanel();
        configPanel = new ConfigPanel();
    }

    @Override
    public String getTitle()
    {
        return "Elasticsearch";
    }

    @Override
    public String getDefaultAlias()
    {
        return "es";
    }
    
    @Override
    public Catalog getCatalog()
    {
        return CATALOG;
    }
    
    @Override
    public void addPropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }
    
    @Override
    public void removePropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.removePropertyChangeListener(listener);
    }
    
    @Override
    public Component getQuickPropertiesComponent()
    {
        return quickPropertiesPanel;
    }

    @Override
    public Component getConfigComponent()
    {
        return configPanel;
    }

    @Override
    public void setup(String catalogAlias, QuerySession querySession)
    {
        if (quickPropertiesPanel.indices.getSelectedItem() instanceof EsIndex)
        {
            EsIndex index = (EsIndex) quickPropertiesPanel.indices.getSelectedItem();
            if (index != null)
            {
                querySession.setCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY, index.endpoint);
                querySession.setCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY, index.index);
            }
        }
    }

    @Override
    public void update(String catalogAlias, QuerySession querySession)
    {
        String index = (String) querySession.getCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY);
        EsIndex valueToSelect = null; 
        int count = quickPropertiesPanel.indices.getItemCount();
        for (int i = 0; i < count; i++)
        {
            EsIndex esIndex = quickPropertiesPanel.indices.getItemAt(i);
            if (StringUtils.equalsIgnoreCase(index, esIndex.index))
            {
                valueToSelect = esIndex;
                break;
            }
        }
        
        if (SwingUtilities.isEventDispatchThread())
        {
            quickPropertiesPanel.indices.getModel().setSelectedItem(valueToSelect);
        }
        else
        {
            final EsIndex temp = valueToSelect;
            SwingUtilities.invokeLater(() -> quickPropertiesPanel.indices.getModel().setSelectedItem(temp));
        }
    }
    
    @Override
    public Map<String, Object> getProperties()
    {
        return properties;
    }
    
    @Override
    public void load(Map<String, Object> properties)
    {
        this.properties.put(ENDPOINTS, properties.get(ENDPOINTS));
        configPanel.update();
    }

    /** Get endpoints from properties */
    @SuppressWarnings("unchecked")
    private List<String> getEndpoints()
    {
        return (List<String>) properties.computeIfAbsent(ENDPOINTS, k -> new ArrayList<String>());
    }

    private void reloadIndices()
    {
        Thread thread = new Thread(() ->
        {
            Object selectedItem = quickPropertiesPanel.indices.getSelectedItem();

            List<EsIndex> esIndices = new ArrayList<>();
            List<Exception> errors = new ArrayList<>();
            for (String endpoint : getEndpoints())
            {
                HttpGet getIndices = new HttpGet(endpoint + "/*/_aliases");
                try (CloseableHttpResponse response = CLIENT.execute(getIndices))
                {
                    Map<String, Object> indices = MAPPER.readValue(response.getEntity().getContent(), Map.class);
                    for (String index : indices.keySet())
                    {
                        esIndices.add(new EsIndex(endpoint, index, false));
                    }

                }
                catch (IOException e)
                {
                    errors.add(new RuntimeException("Error fetching instances from endpont " + endpoint, e));
                }
            }

            esIndices.sort((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a.index, b.index));
            quickPropertiesPanel.indices.setModel(new DefaultComboBoxModel<>(esIndices.toArray(new EsIndex[0])));
            quickPropertiesPanel.indices.setSelectedItem(selectedItem);

            if (!errors.isEmpty())
            {
                // TODO: Central logging panel/window
                errors.forEach(e -> e.printStackTrace());
            }
        });

        thread.start();
    }

    /** Class representing a endpoint/index combo */
    private static class EsIndex
    {
        private final String endpoint;
        private final String index;
        private final boolean showEndpoint;

        EsIndex(String endpoint, String index, boolean showEndpoint)
        {
            this.endpoint = endpoint;
            this.index = index;
            this.showEndpoint = showEndpoint;
        }

        @Override
        public int hashCode()
        {
            return 17
                + (37 * endpoint.hashCode())
                + (37 * index.hashCode());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof EsIndex)
            {
                EsIndex that = (EsIndex) obj;
                return endpoint.equals(that.endpoint)
                    && index.equals(that.index);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return (showEndpoint ? endpoint + "/" : "") + index;
        }
    }

    /** Quick properties panel */
    private class QuickPropertiesPanel extends JPanel
    {
        private final EsIndex prototype = new EsIndex("", "somelongindexname", false);
        private final JComboBox<EsIndex> indices;

        QuickPropertiesPanel()
        {
            setLayout(new GridBagLayout());

            add(new JLabel("Index"), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 5), 0, 0));
            indices = new JComboBox<>();
            indices.addItemListener(l -> pcs.firePropertyChange(ICatalogExtension.PROPERTIES, null, null));
            indices.setPrototypeDisplayValue(prototype);
            indices.setMaximumRowCount(25);
            AutoCompletionComboBox.enable(indices);

            add(indices, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            JButton reloadInstances = new JButton("Reload");
            reloadInstances.addActionListener(l -> reloadIndices());
            add(reloadInstances, new GridBagConstraints(1, 1, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            
            setPreferredSize(new Dimension(200, 50));
        }
    }

    /** Configuration panel */
    private class ConfigPanel extends JPanel
    {
        private final JList<String> listEndpoints;

        ConfigPanel()
        {
            setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

            JPanel panelEndpoints = new JPanel();
            panelEndpoints.setBorder(BorderFactory.createTitledBorder("Endpoints"));
            panelEndpoints.setLayout(new GridBagLayout());

            listEndpoints = new JList<>();
            listEndpoints.setModel(new DefaultComboBoxModel<>(getEndpoints().toArray(ArrayUtils.EMPTY_STRING_ARRAY)));
            panelEndpoints.add(new JScrollPane(listEndpoints),
                    new GridBagConstraints(1, 0, 1, 3, 1.0, 1.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.BOTH, new Insets(0, 5, 0, 0), 0, 0));

            JButton addEndpoint = new JButton("Add");
            addEndpoint.addActionListener(l ->
            {
                String endpoint = JOptionPane.showInputDialog(null, "Endpoint", "Add endpoint", JOptionPane.QUESTION_MESSAGE);
                if (endpoint != null)
                {
                    List<String> endpoints = getEndpoints();
                    if (!endpoints.contains(endpoint))
                    {
                        endpoints.add(endpoint);
                        update();
                        pcs.firePropertyChange(CONFIG, null, null);
                    }
                }
            });
            panelEndpoints.add(addEndpoint, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            JButton editEndpoint = new JButton("Edit");
            addEndpoint.addActionListener(l ->
            {
                //                if (cbEndpoints.getSelectedItem() == null)
                //                {
                //                    return;
                //                }
                //
                //                String endpoint = (String) JOptionPane.showInputDialog(
                //                        null,
                //                        "Endpoint",
                //                        "Edit endpoint",
                //                        JOptionPane.QUESTION_MESSAGE,
                //                        null,
                //                        null,
                //                        cbEndpoints.getSelectedItem());
                //                if (endpoint != null)
                //                {
                //
                ////                    List<String> endpoints = getEndpoints();
                ////                    if (!endpoints.contains(endpoint))
                ////                    {
                ////                        endpoints.add(endpoint);
                ////                        cbEndpoints.setModel(new DefaultComboBoxModel<>(endpoints.toArray(ArrayUtils.EMPTY_STRING_ARRAY)));
                ////                    }
                //                }
            });
            panelEndpoints.add(editEndpoint, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            editEndpoint.setEnabled(false);

            JButton removeEndpoint = new JButton("Remove");
            removeEndpoint.addActionListener(l ->
            {
                getEndpoints().remove(listEndpoints.getSelectedValue());
                update();
                pcs.firePropertyChange(CONFIG, null, null);
            });
            panelEndpoints.add(removeEndpoint, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            add(panelEndpoints, BorderLayout.CENTER);
        }

        private void update()
        {
            listEndpoints.setModel(new DefaultComboBoxModel<>(getEndpoints().toArray(ArrayUtils.EMPTY_STRING_ARRAY)));
            if (listEndpoints.getModel().getSize() > 0)
            {
                reloadIndices();
            }
        }
    }
}
