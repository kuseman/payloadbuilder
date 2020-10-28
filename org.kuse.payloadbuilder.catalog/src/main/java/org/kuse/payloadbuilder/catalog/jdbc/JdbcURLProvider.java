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

/** Simple Sql provider consisting of a plain jdbc url */
class JdbcURLProvider implements ConnectionProvider
{
    private static final String URL = "url";
    private final JPanel component = new JPanel();
    private final JTextField url = new JTextField();
    private Map<String, Object> properties;

    JdbcURLProvider()
    {
        component.setLayout(new GridBagLayout());
        //CSOFF
        component.add(new JLabel("Jdbc URL"), new GridBagConstraints(0, 0, 1, 1, 0.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 3), 0, 0));
        component.add(url, new GridBagConstraints(1, 0, 1, 1, 1.0, 1.0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        //CSOn

        url.addKeyListener(new KeyAdapter()
        {
            @Override
            public void keyReleased(KeyEvent e)
            {
                if (properties != null)
                {
                    properties.put(URL, url.getText());
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
        url.setText((String) properties.getOrDefault(URL, ""));
    }

    @Override
    public String getURL(Map<String, Object> properties)
    {
        return (String) properties.getOrDefault(URL, "");
    }
}
