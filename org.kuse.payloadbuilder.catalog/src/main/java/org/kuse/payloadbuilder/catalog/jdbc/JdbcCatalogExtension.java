package org.kuse.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.event.AncestorEvent;
import javax.swing.event.AncestorListener;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogException;
import org.kuse.payloadbuilder.editor.AutoCompletionComboBox;
import org.kuse.payloadbuilder.editor.ICatalogExtension;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Catalog extension for {@link JdbcCatalog} */
class JdbcCatalogExtension implements ICatalogExtension
{
    private static final String CONNECTIONS = "connections";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PASSWORD_FIELD_LENGTH = 20;
    private static final JdbcCatalog CATALOG = new JdbcCatalog();

    private final DefaultListModel<ServerConnection> connectionsListModel = new DefaultListModel<>();
    private final ConfigComponent configComponent = new ConfigComponent();
    private final PropertiesComponent propertiesComponent = new PropertiesComponent();

    private final PropertyChangeSupport pcs = new PropertyChangeSupport(this);

    @Override
    public String getTitle()
    {
        return "Jdbc";
    }

    @Override
    public String getDefaultAlias()
    {
        return "jdbc";
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
    public void setup(String catalogAlias, QuerySession querySession)
    {
        ServerConnection connection = (ServerConnection) propertiesComponent.connections.getSelectedItem();
        String url = "";
        if (connection != null)
        {
            url = connection.getURL();
            querySession.setCatalogProperty(catalogAlias, JdbcCatalog.USERNAME, connection.username);
            querySession.setCatalogProperty(catalogAlias, JdbcCatalog.PASSWORD, connection.password);
        }
        String database = (String) propertiesComponent.databases.getSelectedItem();
        querySession.setCatalogProperty(catalogAlias, JdbcCatalog.URL, url);
        querySession.setCatalogProperty(catalogAlias, JdbcCatalog.DATABASE, database);
    }

    @Override
    public void update(String catalogAlias, QuerySession querySession)
    {
        String url = (String) querySession.getCatalogProperty(catalogAlias, JdbcCatalog.URL);
        String database = (String) querySession.getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE);

        MutableObject<ServerConnection> connectionToSelect = new MutableObject<>();
        MutableObject<String> databaseToSelect = new MutableObject<>();

        int size = propertiesComponent.connectionsModel.getSize();
        for (int i = 0; i < size; i++)
        {
            ServerConnection connection = propertiesComponent.connectionsModel.getElementAt(i);

            if (!equalsIgnoreCase(url, connection.getURL()))
            {
                continue;
            }

            connectionToSelect.setValue(connection);

            int size2 = connection.databases.size();
            for (int j = 0; j < size2; j++)
            {
                if (equalsIgnoreCase(database, connection.databases.get(j)))
                {
                    databaseToSelect.setValue(connection.databases.get(j));
                    break;
                }
            }
            break;
        }

        SwingUtilities.invokeLater(() ->
        {
            propertiesComponent.connections.setSelectedItem(connectionToSelect.getValue());
            propertiesComponent.databases.setSelectedItem(databaseToSelect.getValue());
        });
    }

    @Override
    public ExceptionAction handleException(QuerySession querySession, CatalogException exception)
    {
        String catalogAlias = exception.getCatalogAlias();
        boolean askForCredentials = false;
        // Credentials exception thrown, ask for credentials
        if (exception instanceof CredentialsException || exception instanceof ConnectionException)
        {
            askForCredentials = true;
        }

        if (askForCredentials)
        {
            ServerConnection connection = (ServerConnection) propertiesComponent.connections.getSelectedItem();
            String url = (String) querySession.getCatalogProperty(catalogAlias, JdbcCatalog.URL);

            boolean isSelectedConnection = equalsIgnoreCase(url, connection.getURL());

            String connectionDescription = isSelectedConnection ? connection.toString() : url;
            String prefilledUsername = isSelectedConnection ? connection.username : (String) querySession.getCatalogProperty(catalogAlias, JdbcCatalog.USERNAME);

            Pair<String, char[]> credentials = getCredentials(connectionDescription, prefilledUsername);
            if (credentials != null)
            {
                //CSOFF
                if (isSelectedConnection)
                //CSON
                {
                    connection.username = credentials.getKey();
                    connection.password = credentials.getValue();

                    // Utilize connection to reload databases if not already done
                    //CSOFF
                    if (connection.databases.size() == 0)
                    //CSON
                    {
                        propertiesComponent.reload(connection, true);
                    }
                }
                querySession.setCatalogProperty(catalogAlias, JdbcCatalog.USERNAME, connection.username);
                querySession.setCatalogProperty(catalogAlias, JdbcCatalog.PASSWORD, connection.password);
                return ExceptionAction.RERUN;
            }
        }

        return ExceptionAction.NONE;
    }

    @Override
    public Component getConfigComponent()
    {
        return configComponent;
    }

    @Override
    public Component getQuickPropertiesComponent()
    {
        return propertiesComponent;
    }

    @Override
    public void load(Map<String, Object> properties)
    {
        List<ServerConnection> connections = MAPPER.convertValue(properties.get(CONNECTIONS), new TypeReference<List<ServerConnection>>()
        {
        });
        if (connections == null)
        {
            return;
        }
        connectionsListModel.removeAllElements();
        for (ServerConnection connection : connections)
        {
            connectionsListModel.addElement(connection);
        }
        propertiesComponent.populateConnections();
    }

    @Override
    public Map<String, Object> getProperties()
    {
        return ofEntries(entry(CONNECTIONS, connectionsListModel.toArray()));
    }

    private Pair<String, char[]> getCredentials(String connectionDescription, String prefilledUsername)
    {
        JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        JTextField usernameField = new JTextField();
        JPasswordField passwordField = new JPasswordField(PASSWORD_FIELD_LENGTH);
        panel.add(new JLabel("Connection: "), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 0), 0, 0));
        panel.add(new JLabel(connectionDescription), new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 0), 0, 0));
        panel.add(new JLabel("Username: "), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 0), 0, 0));
        panel.add(usernameField, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 3, 0));
        panel.add(new JLabel("Password: "), new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        panel.add(passwordField, new GridBagConstraints(1, 2, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        usernameField.setText(prefilledUsername);
        JTextField focusCompoennt = isBlank(usernameField.getText()) ? usernameField : passwordField;

        focusCompoennt.addAncestorListener(new AncestorListener()
        {
            @Override
            public void ancestorRemoved(AncestorEvent event)
            {
            }

            @Override
            public void ancestorMoved(AncestorEvent event)
            {
            }

            @Override
            public void ancestorAdded(final AncestorEvent event)
            {
                event.getComponent().requestFocusInWindow();
                event.getComponent().removeAncestorListener(this);
            }
        });

        String[] options = new String[] {"OK", "Cancel"};
        int option = JOptionPane.showOptionDialog(
                null,
                panel,
                "Enter Credentials",
                JOptionPane.NO_OPTION,
                JOptionPane.PLAIN_MESSAGE,
                null,
                options,
                options[0]);
        if (option == 0)
        {
            String username = usernameField.getText();
            char[] password = passwordField.getPassword();

            if (!isBlank(username) && !ArrayUtils.isEmpty(password))
            {
                return Pair.of(username, password);
            }
        }
        return null;
    }

    /** Properties component */
    private class PropertiesComponent extends JPanel
    {
        private static final String PROTOTYPE_CATALOG = "somedatabasewithalongname";
        private final DefaultComboBoxModel<ServerConnection> connectionsModel = new DefaultComboBoxModel<>();
        private final DefaultComboBoxModel<String> databasesModel = new DefaultComboBoxModel<>();
        private final JComboBox<ServerConnection> connections = new JComboBox<>(connectionsModel);
        private final JComboBox<String> databases = new JComboBox<>(databasesModel);

        PropertiesComponent()
        {
            setLayout(new GridBagLayout());

            JButton reload = new JButton("Reload");
            reload.addActionListener(l ->
            {
                ServerConnection connection = (ServerConnection) connections.getSelectedItem();
                if (connection == null)
                {
                    return;
                }
                reload(connection, false);
            });

            connections.addItemListener(l ->
            {
                ServerConnection connection = (ServerConnection) l.getItem();
                populateDatabases(connection);
            });

            AutoCompletionComboBox.enable(databases);
            databases.setPrototypeDisplayValue(PROTOTYPE_CATALOG);
            //CSOFF
            databases.setMaximumRowCount(25);
            //CSON
            add(new JLabel("Server"), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 1, 3), 0, 0));
            add(connections, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 1, 0), 0, 0));
            add(new JLabel("Database"), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 1, 3), 0, 0));
            add(databases, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 1, 0), 0, 0));
            add(reload, new GridBagConstraints(1, 2, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            //CSOFF
            setPreferredSize(new Dimension(240, 75));
            //CSON
        }

        private void reload(ServerConnection connection, boolean suppressError)
        {
            if (!connection.hasCredentials())
            {
                final Pair<String, char[]> credentials = getCredentials(connection.toString(), connection.username);
                if (credentials == null)
                {
                    return;
                }
                connection.username = credentials.getKey();
                connection.password = credentials.getRight();
            }
            Thread t = new Thread(() ->
            {
                try (Connection sqlConnection = CATALOG.getConnection(
                        connection.getURL(),
                        connection.username,
                        new String(connection.password),
                        ""))
                {
                    connection.databases = new ArrayList<>();
                    try (ResultSet rs = sqlConnection.getMetaData().getCatalogs())
                    {
                        while (rs.next())
                        {
                            connection.databases.add(rs.getString(1));
                        }
                    }
                    populateDatabases(connection);
                }
                catch (Exception e)
                {
                    connection.password = null;
                    if (!suppressError)
                    {
                        JOptionPane.showMessageDialog(null, e.getMessage(), "Error reloading databases", JOptionPane.ERROR_MESSAGE);
                    }
                }
            });
            t.start();
        }

        private void populateConnections()
        {
            int size = connectionsListModel.size();
            for (int i = 0; i < size; i++)
            {
                connectionsModel.addElement(connectionsListModel.elementAt(i));
            }
        }

        private void populateDatabases(ServerConnection connection)
        {
            databasesModel.removeAllElements();
            connection.databases.sort((a, b) -> a.compareTo(b));
            for (String database : connection.databases)
            {
                databasesModel.addElement(database);
            }
        }
    }

    /** Config component */
    private class ConfigComponent extends JPanel
    {
        // - List of connections
        private final JPanel providerProperties;
        private final JList<ServerConnection> connectionsList;

        ConfigComponent()
        {
            setLayout(new GridBagLayout());

            JPanel connections = new JPanel();
            connections.setBorder(BorderFactory.createTitledBorder("Connections"));
            connections.setLayout(new GridBagLayout());

            connectionsList = new JList<>();
            connectionsList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
            connectionsList.setModel(connectionsListModel);
            JButton add = new JButton("Add");
            add.addActionListener(l ->
            {
                connectionsListModel.addElement(new ServerConnection("Connection", SqlType.JDBC_URL));
                connectionsList.setSelectedIndex(connectionsList.getModel().getSize() - 1);
                populateProviderProperties();
            });
            JButton remove = new JButton("Remove");
            remove.addActionListener(l ->
            {
                int index = connectionsList.getSelectedIndex();
                if (index >= 0)
                {
                    connectionsListModel.remove(index);
                    if (index >= connectionsList.getModel().getSize())
                    {
                        index = connectionsList.getModel().getSize() - 1;
                    }
                    connectionsList.setSelectedIndex(index);
                    populateProviderProperties();
                }
            });

            //CSOFF
            connections.add(connectionsList, new GridBagConstraints(0, 0, 2, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            connections.add(add, new GridBagConstraints(0, 1, 1, 1, 0.5, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            connections.add(remove, new GridBagConstraints(1, 1, 1, 1, 0.5, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            //CSON

            JPanel connectionProperties = new JPanel();
            connectionProperties.setBorder(BorderFactory.createTitledBorder("Properties"));
            connectionProperties.setLayout(new GridBagLayout());

            JTextField name = new JTextField();
            name.addKeyListener(new KeyAdapter()
            {
                @Override
                public void keyReleased(KeyEvent e)
                {
                    ServerConnection connection = connectionsList.getSelectedValue();
                    if (connection != null)
                    {
                        connection.name = name.getText();
                        connectionsList.repaint();
                    }
                }
            });
            JComboBox<SqlType> type = new JComboBox<>();
            type.addActionListener(l ->
            {
                ServerConnection connection = connectionsList.getSelectedValue();
                if (connection != null)
                {
                    connection.type = (SqlType) type.getSelectedItem();
                    connectionsList.repaint();
                    populateProviderProperties();
                }
            });
            type.setModel(new DefaultComboBoxModel<>(SqlType.values()));
            providerProperties = new JPanel();
            providerProperties.setLayout(new BorderLayout());
            connectionProperties.add(new JLabel("Name"), new GridBagConstraints(0, 0, 1, 1, 0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 3), 0, 0));
            connectionProperties.add(new JLabel("Type"), new GridBagConstraints(0, 1, 1, 1, 0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 3), 0, 0));
            connectionProperties.add(name, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            connectionProperties.add(type, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
            connectionProperties.add(providerProperties, new GridBagConstraints(0, 2, 2, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.BOTH, new Insets(3, 0, 0, 0), 0, 0));

            //CSOFF
            add(connections, new GridBagConstraints(0, 0, 1, 1, 0.25, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            add(connectionProperties, new GridBagConstraints(1, 0, 1, 1, 0.75, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            //CSON

            connectionsList.addListSelectionListener(l ->
            {
                if (l.getValueIsAdjusting())
                {
                    return;
                }
                ServerConnection connection = connectionsList.getSelectedValue();
                if (connection != null)
                {
                    name.setText(connection.name);
                    type.setSelectedItem(connection.type);
                    populateProviderProperties();
                }
            });
        }

        private void populateProviderProperties()
        {
            providerProperties.removeAll();

            ServerConnection connection = connectionsList.getSelectedValue();
            if (connection != null)
            {
                ConnectionProvider provider = connection.type.getProvider();
                Component component = provider.getComponent();
                if (component != null)
                {
                    providerProperties.add(component, BorderLayout.CENTER);
                    provider.initComponent(connection.properties);
                }
            }
            providerProperties.revalidate();
            providerProperties.repaint();
        }
    }

    /** Server connection */
    private static class ServerConnection
    {
        @JsonProperty
        private String name;
        @JsonProperty
        private SqlType type;
        @JsonProperty
        private final Map<String, Object> properties = new HashMap<>();
        @JsonIgnore
        private String username = System.getProperty("user.name");
        @JsonIgnore
        private char[] password;
        @JsonIgnore
        private List<String> databases = emptyList();

        @SuppressWarnings("unused")
        ServerConnection()
        {
        }

        boolean hasCredentials()
        {
            return !isBlank(username) && !ArrayUtils.isEmpty(password);
        }

        ServerConnection(String name, SqlType type)
        {
            this.name = name;
            this.type = type;
        }

        String getURL()
        {
            if (type == null)
            {
                return null;
            }
            return type.provider.getURL(properties);
        }

        @Override
        public String toString()
        {
            return name + " (" + type.title + ")";
        }
    }

    /** Type of sql */
    enum SqlType
    {
        JDBC_URL("Raw JDBC URL", new JdbcURLProvider()),
        SQLSERVER("MSSql Server", new SqlServerProvider());

        private final String title;
        private final ConnectionProvider provider;

        SqlType(String title, ConnectionProvider provider)
        {
            this.title = title;
            this.provider = provider;
        }

        String getTitle()
        {
            return title;
        }

        ConnectionProvider getProvider()
        {
            return provider;
        }

        @Override
        public String toString()
        {
            return title;
        }
    }
}
