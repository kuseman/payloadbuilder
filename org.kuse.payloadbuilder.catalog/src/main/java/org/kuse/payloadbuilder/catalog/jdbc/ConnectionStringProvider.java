package org.kuse.payloadbuilder.catalog.jdbc;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.Map;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

/** Simple Sql provider consisting of a connection string */
class ConnectionStringProvider implements ConnectionProvider
{
    private static final String CONNECTIONSTRING = "connectionstring";
    private final JPanel component = new JPanel();
    private final JTextField connectionString = new JTextField();
    private Map<String, Object> properties;

    ConnectionStringProvider()
    {
        component.setLayout(new GridBagLayout());
        //CSOFF
        component.add(new JLabel("Connection String"), new GridBagConstraints(0, 0, 1, 1, 0.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 3), 0, 0));
        component.add(connectionString, new GridBagConstraints(1, 0, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        //CSOn

        connectionString.addKeyListener(new KeyAdapter()
        {
            @Override
            public void keyReleased(KeyEvent e)
            {
                if (properties != null)
                {
                    properties.put(CONNECTIONSTRING, connectionString.getText());
                }
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
        connectionString.setText((String) properties.getOrDefault(CONNECTIONSTRING, ""));
    }

    @Override
    public String getConnectionString(Map<String, Object> properties)
    {
        return (String) properties.getOrDefault(CONNECTIONSTRING, "");
    }
}
