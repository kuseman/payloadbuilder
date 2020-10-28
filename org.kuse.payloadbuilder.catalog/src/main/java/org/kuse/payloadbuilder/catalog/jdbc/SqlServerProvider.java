package org.kuse.payloadbuilder.catalog.jdbc;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.Map;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

/** MS SQL Provider */
class SqlServerProvider implements ConnectionProvider
{
    private static final String SERVER = "server";
    private static final String DOMAIN = "domain";
    private static final String AUTHENTICATION_TYPE = "authenticationType";
    private static final String APPLICATION_NAME = "applicationName";
    private static final String URL_SUFFIX = "urlSuffix";

    private final JPanel component = new JPanel();
    private final JComboBox<AuthenticationType> authenticationType = new JComboBox<>(AuthenticationType.values());
    private final JTextField server = new JTextField();
    private final JTextField domain = new JTextField();
    private final JTextField applicationName = new JTextField("PayloadBuilder");
    private final JTextField urlSuffix = new JTextField("selectMode=cursor");
    private Map<String, Object> properties;

    SqlServerProvider()
    {
        component.setLayout(new GridBagLayout());
        //CSOFF
        component.add(new JLabel("Server"), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 3), 0, 0));
        component.add(new JLabel("Authentication Type"), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 3), 0, 0));
        component.add(new JLabel("Domain"), new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 3), 0, 0));
        component.add(new JLabel("Application Name"), new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 3), 0, 0));
        component.add(new JLabel("URL Suffix"), new GridBagConstraints(0, 4, 1, 1, 0.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 3), 0, 0));

        component.add(server, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 3), 0, 0));
        component.add(authenticationType, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 0), 0, 0));
        component.add(domain, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 0), 0, 0));
        component.add(applicationName, new GridBagConstraints(1, 3, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 3, 0), 0, 0));
        component.add(urlSuffix, new GridBagConstraints(1, 4, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        //CSON

        bind(server, SERVER);
        bind(domain, DOMAIN);
        bind(applicationName, APPLICATION_NAME);
        bind(urlSuffix, URL_SUFFIX);
        authenticationType.addActionListener(l ->
        {
            if (properties != null)
            {
                properties.put(AUTHENTICATION_TYPE, ((AuthenticationType) authenticationType.getSelectedItem()).name());
            }
        });
    }

    @Override
    public Component getComponent()
    {
        return component;
    }

    @Override
    public void initComponent(Map<String, Object> properties)
    {
        this.properties = properties;
        if (properties == null)
        {
            return;
        }
        load(server, SERVER);
        load(domain, DOMAIN);
        load(applicationName, APPLICATION_NAME);
        load(urlSuffix, URL_SUFFIX);
        AuthenticationType authenticationType = AuthenticationType.valueOf((String) properties.getOrDefault(AUTHENTICATION_TYPE, AuthenticationType.SQL_SERVER_AUTHENTICATION.name()));
        this.authenticationType.setSelectedItem(authenticationType);
    }

    @Override
    public String getURL(Map<String, Object> properties)
    {
        AuthenticationType authenticationType = AuthenticationType.valueOf((String) properties.getOrDefault(AUTHENTICATION_TYPE, AuthenticationType.SQL_SERVER_AUTHENTICATION.name()));
        String server = (String) properties.getOrDefault(SERVER, "");
        String domain = (String) properties.getOrDefault(DOMAIN, "");
        String applicationName = (String) properties.getOrDefault(APPLICATION_NAME, "");
        String urlSuffix = (String) properties.getOrDefault(URL_SUFFIX, "");

        return authenticationType.generateURL(server, domain, applicationName, urlSuffix);
    }

    private void load(JTextField tf, String property)
    {
        tf.setText((String) properties.getOrDefault(property, ""));
    }

    private void bind(JTextField tf, String property)
    {
        tf.addKeyListener(new KeyAdapter()
        {
            @Override
            public void keyReleased(KeyEvent e)
            {
                if (properties != null)
                {
                    properties.put(property, tf.getText());
                }
            }
        });
    }

    /** Authentication type */
    enum AuthenticationType
    {
        /** Authentication with windows domain user and password */
        WINDOWS_AUTHENTICATION_CREDENTIAlS("Windows NTLM Authentication")
        {
            @Override
            String generateURL(String server, String domain, String applicationName, String urlSuffix)
            {
                return "jdbc:sqlserver://"
                    + server
                    + ";integratedSecurity=true;authenticationScheme=NTLM"
                    + ";domain=" + domain
                    + (!isBlank(applicationName) ? (";applicationName=" + applicationName) : "")
                    + (!isBlank(urlSuffix) ? (";" + urlSuffix) : "");
            }
        },

        /** Authentication with sql server user and password */
        SQL_SERVER_AUTHENTICATION("SQL Server Authentication")
        {
            @Override
            String generateURL(String server, String domain, String applicationName, String urlSuffix)
            {
                return "jdbc:sqlserver://"
                    + server
                    + (!isBlank(applicationName) ? (";applicationName=" + applicationName) : "")
                    + (!isBlank(urlSuffix) ? (";" + urlSuffix) : "");
            }
        };

        private final String title;

        AuthenticationType(String title)
        {
            this.title = title;
        }

        /** Generate connection string */
        abstract String generateURL(String server, String domain, String applicationName, String urlSuffix);

        @Override
        public String toString()
        {
            return title;
        }
    }
}
