package com.viskan.payloadbuilder.catalog.elastic;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.editor.AutoCompletionComboBox;
import com.viskan.payloadbuilder.editor.ICatalogExtension;

import static com.viskan.payloadbuilder.catalog.elastic.EtmArticleCategoryESOperator.CLIENT;

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

import com.fasterxml.jackson.databind.ObjectMapper;

/** Extension for EtmArticleCategory (V2) data located in ES */
public class EtmArticleCategoryESCatalogExtension implements ICatalogExtension
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENDPOINTS = "endpoints";
//    private static final String TYPEMAPPINGS = "typemappings";
//    private static final String[] TYPE_MAPPING_COLUMNS = new String[] {"Type", "Table names", "DocType", "SubType", "DocId pattern", "DocId columns"};
    private static final Catalog CATALOG = new EtmArticleCategoryESCatalog();

    private final PropertyChangeSupport pcs = new PropertyChangeSupport(this);
    private final Map<String, Object> properties = new HashMap<>();
    private final QuickPropertiesPanel quickPropertiesPanel;
    private final ConfigPanel configPanel;
    
    public EtmArticleCategoryESCatalogExtension()
    {
        quickPropertiesPanel = new QuickPropertiesPanel();
        configPanel = new ConfigPanel();
    }

    @Override
    public String getTitle()
    {
        return "EtmArticleCategory ES";
    }

    @Override
    public String getDefaultAlias()
    {
        return "etmes";
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
        if (quickPropertiesPanel.instances.getSelectedItem() instanceof EsInstance)
        {
            EsInstance instance = (EsInstance) quickPropertiesPanel.instances.getSelectedItem();
            if (instance != null)
            {
                querySession.setCatalogProperty(catalogAlias, EtmArticleCategoryESCatalog.ENDPOINT_KEY, instance.endpoint);
                querySession.setCatalogProperty(catalogAlias, EtmArticleCategoryESCatalog.INSTANCE_KEY, instance.instance);
            }
        }
    }

    @Override
    public void update(String catalogAlias, QuerySession querySession)
    {
        String instance = (String) querySession.getCatalogProperty(catalogAlias, EtmArticleCategoryESCatalog.INSTANCE_KEY);
        EsInstance valueToSelect = null; 
        int count = quickPropertiesPanel.instances.getItemCount();
        for (int i = 0; i < count; i++)
        {
            EsInstance esInstance = quickPropertiesPanel.instances.getItemAt(i);
            if (StringUtils.equalsIgnoreCase(instance, esInstance.instance))
            {
                valueToSelect = esInstance;
                break;
            }
        }
        
        if (SwingUtilities.isEventDispatchThread())
        {
            quickPropertiesPanel.instances.getModel().setSelectedItem(valueToSelect);
        }
        else
        {
            final EsInstance temp = valueToSelect;
            SwingUtilities.invokeLater(() -> quickPropertiesPanel.instances.getModel().setSelectedItem(temp));
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

//    @SuppressWarnings("unchecked")
//    private List<TypeMapping> getTypeMappings()
//    {
//        return (List<TypeMapping>) properties.computeIfAbsent(TYPEMAPPINGS, k -> new ArrayList<TypeMapping>());
//    }

    private void reloadInstances()
    {
        Thread thread = new Thread(() ->
        {
            Object selectedItem = quickPropertiesPanel.instances.getSelectedItem();

            List<EsInstance> esInstances = new ArrayList<>();
            List<Exception> errors = new ArrayList<>();
            for (String endpoint : getEndpoints())
            {
                HttpGet getIndices = new HttpGet(endpoint + "/*/_aliases");
                try (CloseableHttpResponse response = CLIENT.execute(getIndices))
                {
                    Map<String, Object> indices = MAPPER.readValue(response.getEntity().getContent(), Map.class);
                    for (String index : indices.keySet())
                    {
                        if (StringUtils.startsWithIgnoreCase(index, "ramos") && !StringUtils.endsWithIgnoreCase(index, "_store"))
                        {
                            // ramosvnptestmain_c0_v3
                            // Remove v3
                            index = index.substring(0, index.lastIndexOf("_"));
                            // ramosvnptestmain_c0
                            int comp_id = Integer.parseInt(index.substring(index.lastIndexOf("_") + 2));
                            // remove cX
                            index = index.substring(0, index.lastIndexOf("_"));
                            String instance = index;
                            if (comp_id > 0)
                            {
                                instance = instance + "_" + comp_id;
                            }

                            esInstances.add(new EsInstance(endpoint, instance));
                        }
                    }

                }
                catch (IOException e)
                {
                    errors.add(new RuntimeException("Error fetching instances from endpont " + endpoint, e));
                }
            }

            esInstances.sort((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a.instance, b.instance));
            quickPropertiesPanel.instances.setModel(new DefaultComboBoxModel<>(esInstances.toArray(new EsInstance[0])));
            quickPropertiesPanel.instances.setSelectedItem(selectedItem);

            if (!errors.isEmpty())
            {
                // TODO: Central logging panel/window
                errors.forEach(e -> e.printStackTrace());
            }
        });

        thread.start();
    }

    /** Class representing a endpoint/instance combo */
    private static class EsInstance
    {
        private final String endpoint;
        private final String instance;

        EsInstance(String endpoint, String instance)
        {
            this.endpoint = endpoint;
            this.instance = instance;
        }

        @Override
        public int hashCode()
        {
            return 17
                + (37 * endpoint.hashCode())
                + (37 * instance.hashCode());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof EsInstance)
            {
                EsInstance that = (EsInstance) obj;
                return endpoint.equals(that.endpoint)
                    && instance.equals(that.instance);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return instance;
        }
    }

    /** Quick properties panel */
    private class QuickPropertiesPanel extends JPanel
    {
        private final EsInstance prototype = new EsInstance("", "ramosdatabaseprod");
        private final JComboBox<EsInstance> instances;

        QuickPropertiesPanel()
        {
            setLayout(new GridBagLayout());

            add(new JLabel("Instance"), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 5), 0, 0));
            instances = new JComboBox<>();
            instances.addItemListener(l -> pcs.firePropertyChange(ICatalogExtension.PROPERTIES, null, null));
            instances.setPrototypeDisplayValue(prototype);
            instances.setMaximumRowCount(25);
            AutoCompletionComboBox.enable(instances);

            add(instances, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

            JButton reloadInstances = new JButton("Reload");
            reloadInstances.addActionListener(l -> reloadInstances());
            add(reloadInstances, new GridBagConstraints(1, 1, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            
            setPreferredSize(new Dimension(200, 50));
        }
    }

    /** Configuration panel */
    private class ConfigPanel extends JPanel
    {
        private final JList<String> listEndpoints;
//        private final JTable mappingsTable;

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
            });
            panelEndpoints.add(removeEndpoint, new GridBagConstraints(0, 2, 1, 1, 0.0, 1.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

//            JPanel panelTypeMappings = new JPanel();
//            panelTypeMappings.setBorder(BorderFactory.createTitledBorder("Type mappings"));
//            panelTypeMappings.setLayout(new GridBagLayout());
//            
//            mappingsTable = createTypeMappingsTable();
//            
//            panelTypeMappings.add(new JScrollPane(mappingsTable),
//                    new GridBagConstraints(0, 0, 4, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0));
//
//            JButton addTypeMapping = new JButton("Add");
//            addTypeMapping.addActionListener(l -> 
//            {
//                List<TypeMapping> mappings = getTypeMappings();
//                TypeMapping mapping = new TypeMapping();
//                mapping.name = "mapping_" + mappings.size();
//                mappings.add(mapping);
//                ((AbstractTableModel) mappingsTable.getModel()).fireTableDataChanged();
//            });
//            panelTypeMappings.add(addTypeMapping, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0));
//            JButton removeTypeMapping = new JButton("Remove");
//            panelTypeMappings.add(removeTypeMapping, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0));

            add(panelEndpoints, BorderLayout.CENTER);
//            add(panelTypeMappings, BorderLayout.CENTER);
        }

        private void update()
        {
            listEndpoints.setModel(new DefaultComboBoxModel<>(getEndpoints().toArray(ArrayUtils.EMPTY_STRING_ARRAY)));
//            ((AbstractTableModel) mappingsTable.getModel()).fireTableDataChanged();
        }
        
//        private JTable createTypeMappingsTable()
//        {
//            JTable table = new JTable();
//
//            TableModel model = new AbstractTableModel()
//            {
//                @Override
//                public String getColumnName(int column)
//                {
//                    return TYPE_MAPPING_COLUMNS[column];
//                }
//
//                @Override
//                public boolean isCellEditable(int rowIndex, int columnIndex)
//                {
//                    return false;
//                }
////
////                @Override
////                public void setValueAt(Object aValue, int rowIndex, int columnIndex)
////                {
////                    TypeMapping typeMapping = getTypeMappings().get(rowIndex);
////                    switch (columnIndex)
////                    {
////                        case 0:
////                            typeMapping.name = (String) aValue;
////                            break;
////                        case 1:
////                            typeMapping.tableNames = stream(((String) aValue).split(","))
////                                    .map(n -> StringUtils.trim(n))
////                                    .collect(toSet());
////                            break;
////                        case 2:
////                            typeMapping.doctype = (String) aValue;
////                            break;
////                        case 3:
////                            typeMapping.subtype = (String) aValue;
////                            break;
////                        case 4:
////                            typeMapping.docIdPattern = (String) aValue;
////                            break;
////                        case 5:
////                            typeMapping.docIdPatternColumnNames = stream(((String) aValue).split(","))
////                                    .map(n -> StringUtils.trim(n))
////                                    .collect(toList());
////                            break;
////                    }
////                }
//
//                @Override
//                public Object getValueAt(int rowIndex, int columnIndex)
//                {
//                    TypeMapping typeMapping = getTypeMappings().get(rowIndex);
//                    switch (columnIndex)
//                    {
//                        case 0:
//                            return typeMapping.name;
//                        case 1:
//                            return typeMapping.tableNames.stream().collect(joining(", "));
//                        case 2:
//                            return typeMapping.doctype;
//                        case 3:
//                            return typeMapping.subtype;
//                        case 4:
//                            return typeMapping.docIdPattern;
//                        case 5:
//                            return typeMapping.docIdPatternColumnNames.stream().collect(joining(", "));
//                        default:
//                            return null;
//                    }
//                }
//
//                @Override
//                public int getRowCount()
//                {
//                    return getTypeMappings().size();
//                }
//
//                @Override
//                public int getColumnCount()
//                {
//                    return TYPE_MAPPING_COLUMNS.length;
//                }
//            };
//
//            table.setModel(model);
//            return table;
//        }
    }
}
