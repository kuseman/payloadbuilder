package org.kuse.payloadbuilder.catalog.es;

import static java.util.Collections.emptyList;
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
import java.util.Map.Entry;

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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
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
    private final Map<String, List<String>> indicesByEndpoint = new HashMap<>();

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
        String endpoint = (String) quickPropertiesPanel.endpoints.getSelectedItem();
        String index = (String) quickPropertiesPanel.indices.getSelectedItem();
        querySession.setCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY, endpoint);
        querySession.setCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY, index);
    }

    @Override
    public void update(String catalogAlias, QuerySession querySession)
    {
        String endpoint = (String) querySession.getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
        String index = (String) querySession.getCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY);
        String endointToSet = null;
        String indexToSet = null;
        for (Entry<String, List<String>> e : indicesByEndpoint.entrySet())
        {
            if (StringUtils.equalsIgnoreCase(endpoint, e.getKey()))
            {
                endointToSet = e.getKey();
                for (String idx : e.getValue())
                {
                    if (StringUtils.equalsIgnoreCase(index, idx))
                    {
                        indexToSet = idx;
                        break;
                    }
                }

                if (indexToSet != null)
                {
                    break;
                }
            }
        }

        if (SwingUtilities.isEventDispatchThread())
        {
            quickPropertiesPanel.endpoints.getModel().setSelectedItem(endointToSet);
            quickPropertiesPanel.indices.getModel().setSelectedItem(indexToSet);
        }
        else
        {
            final String tempA = endointToSet;
            final String tempB = indexToSet;

            SwingUtilities.invokeLater(() ->
            {
                quickPropertiesPanel.endpoints.getModel().setSelectedItem(tempA);
                quickPropertiesPanel.indices.getModel().setSelectedItem(tempB);
            });
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
            indicesByEndpoint.clear();

            Object selectedEndpoint = quickPropertiesPanel.endpoints.getSelectedItem();
            Object selectedIndex = quickPropertiesPanel.indices.getSelectedItem();

            List<String> endpoints = getEndpoints();
            List<Exception> errors = new ArrayList<>();
            for (String endpoint : endpoints)
            {
                List<String> indices = new ArrayList<>();
                indicesByEndpoint.put(endpoint, indices);
                HttpGet getIndices = new HttpGet(endpoint + "/_aliases");
                try (CloseableHttpResponse response = CLIENT.execute(getIndices))
                {
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
                    {
                        throw new RuntimeException("Error query Elastic. " + IOUtils.toString(response.getEntity().getContent()));
                    }
                    indices.addAll(MAPPER.readValue(response.getEntity().getContent(), Map.class).keySet());
                }
                catch (IOException e)
                {
                    errors.add(new RuntimeException("Error fetching instances from endpont " + endpoint, e));
                }
                indices.sort((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a, b));
            }

            quickPropertiesPanel.endpointsModel.removeAllElements();
            endpoints.forEach(endpoint -> quickPropertiesPanel.endpointsModel.addElement(endpoint));
            quickPropertiesPanel.endpoints.setSelectedItem(selectedEndpoint);
            quickPropertiesPanel.indices.setSelectedItem(selectedIndex);

            if (!errors.isEmpty())
            {
                // TODO: Central logging panel/window
                errors.forEach(e -> e.printStackTrace());
            }
        });

        thread.start();
    }

    /** Quick properties panel */
    private class QuickPropertiesPanel extends JPanel
    {
        private final String endpointPrototype = "http://elasticsearch.domain.com";
        private final String indexPrototype = "somelongindexname";
        private final JComboBox<String> endpoints;
        private final JComboBox<String> indices;
        private final DefaultComboBoxModel<String> indicesModel = new DefaultComboBoxModel<>();
        private final DefaultComboBoxModel<String> endpointsModel = new DefaultComboBoxModel<>();

        QuickPropertiesPanel()
        {
            setLayout(new GridBagLayout());

            //CSOFF
            add(new JLabel("Endpoint"), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 5), 0, 0));
            endpoints = new JComboBox<>();
            endpoints.setModel(endpointsModel);
            endpoints.addItemListener(l ->
            {
                indicesModel.removeAllElements();
                for (String index : indicesByEndpoint.getOrDefault(endpoints.getSelectedItem(), emptyList()))
                {
                    indicesModel.addElement(index);
                }
                pcs.firePropertyChange(ICatalogExtension.PROPERTIES, null, null);
            });
            endpoints.setPrototypeDisplayValue(endpointPrototype);
            endpoints.setMaximumRowCount(25);
            AutoCompletionComboBox.enable(endpoints);
            add(endpoints, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            add(new JLabel("Index"), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 5), 0, 0));
            indices = new JComboBox<>();
            indices.setModel(indicesModel);
            indices.addItemListener(l -> pcs.firePropertyChange(ICatalogExtension.PROPERTIES, null, null));
            indices.setPrototypeDisplayValue(indexPrototype);
            indices.setMaximumRowCount(25);
            AutoCompletionComboBox.enable(indices);
            add(indices, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            JButton reloadInstances = new JButton("Reload");
            reloadInstances.addActionListener(l -> reloadIndices());
            add(reloadInstances, new GridBagConstraints(1, 2, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            setPreferredSize(new Dimension(240, 75));
            //CSON
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
                    new GridBagConstraints(1, 0, 1, 3, 1.0, 1.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.BOTH, new Insets(0, 3, 0, 0), 0, 0));

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
                    }
                }
            });
            panelEndpoints.add(addEndpoint, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            JButton editEndpoint = new JButton("Edit");
            panelEndpoints.add(editEndpoint, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            editEndpoint.setEnabled(false);

            JButton removeEndpoint = new JButton("Remove");
            removeEndpoint.addActionListener(l ->
            {
                getEndpoints().remove(listEndpoints.getSelectedValue());
                update();
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
