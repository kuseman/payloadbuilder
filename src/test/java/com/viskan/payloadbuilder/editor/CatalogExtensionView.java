package com.viskan.payloadbuilder.editor;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

import org.kordamp.ikonli.fontawesome.FontAwesome;
import org.kordamp.ikonli.swing.FontIcon;

/** View for extension */
class CatalogExtensionView extends JPanel
{
    private static final Icon COG = FontIcon.of(FontAwesome.COG);
    
    CatalogExtensionView(
            CatalogExtensionModel extension,
            ButtonGroup defaultGroup,
            Runnable configChanged)
    {
        /*
         * QueryFile
         *      CatalogRegistry
         *          default PropertyChangeListener ?
         *          Catalogs with inherited class implementing ICatalogExtension
         */
        
        setBorder(BorderFactory.createTitledBorder(extension.getExtension().getTitle()));
        setLayout(new BorderLayout());

        JPanel topPanel = new JPanel();
        JPanel extensionPanel = new JPanel();
        extensionPanel.setLayout(new BorderLayout());

        topPanel.setLayout(new GridBagLayout());

        JTextField tfAlias = new JTextField(extension.getAlias());
        tfAlias.getDocument().addDocumentListener(new ADocumentListenerAdapter()
        {
            @Override
            protected void update()
            {
                extension.setAlias(tfAlias.getText());
            }
        });
        
        JRadioButton rbDefault = new JRadioButton();
        rbDefault.setToolTipText("Set default catalog");
        defaultGroup.add(rbDefault);

        JButton config = new JButton(COG);
        JCheckBox cbEnabled = new JCheckBox();
        cbEnabled.setToolTipText("Enable/disable extension");
        cbEnabled.setSelected(true);
        cbEnabled.addActionListener(l ->
        {
            if (rbDefault.isSelected())
            {
                defaultGroup.clearSelection();
            }
            rbDefault.setEnabled(cbEnabled.isSelected());
            tfAlias.setEnabled(cbEnabled.isSelected());
            config.setEnabled(cbEnabled.isSelected());
            setPanelEnabled(extensionPanel, cbEnabled.isSelected());
            extension.setEnabled(cbEnabled.isSelected());
        });
        config.addActionListener(l -> 
        {
            JFrame dialog = new JFrame("Config " + extension.getExtension().getTitle());
            dialog.getContentPane().setLayout(new BorderLayout());
            dialog.getContentPane().add(extension.getExtension().getConfigComponent(), BorderLayout.CENTER);
            dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            dialog.setPreferredSize(new Dimension(800, 600));
            dialog.pack();
            dialog.setLocationRelativeTo(null);
            dialog.pack();
            dialog.setVisible(true);
            dialog.addWindowListener(new WindowAdapter()
            {
                @Override
                public void windowClosed(WindowEvent e)
                {
                    configChanged.run();
                }
            });
        });
        config.setEnabled(extension.getExtension().getConfigComponent() != null);
        
        topPanel.add(rbDefault, new GridBagConstraints(0, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 5, 0), 0, 0));
        topPanel.add(cbEnabled, new GridBagConstraints(1, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 5, 0), 0, 0));
        topPanel.add(new JLabel("Alias: "), new GridBagConstraints(2, 0, 1, 0, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        topPanel.add(tfAlias,
                new GridBagConstraints(3, 0, 1, 0, 1, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        topPanel.add(config,
                new GridBagConstraints(4, 0, 1, 0, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 5, 0), 0, -4));


        add(topPanel, BorderLayout.NORTH);
        add(extensionPanel, BorderLayout.CENTER);
        
        extensionPanel.add(extension.getExtension().getQuickPropertiesComponent(), BorderLayout.CENTER);
    }
    
    void setPanelEnabled(Container container, boolean isEnabled)
    {
        container.setEnabled(isEnabled);

        Component[] components = container.getComponents();

        for (Component component : components)
        {
            if (component instanceof Container)
            {
                setPanelEnabled((Container) component, isEnabled);
            }
            component.setEnabled(isEnabled);
        }
    }
}
