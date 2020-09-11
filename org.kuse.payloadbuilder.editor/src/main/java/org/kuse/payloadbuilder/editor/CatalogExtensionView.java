/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.editor;

import static org.kuse.payloadbuilder.editor.ICatalogExtension.CONFIG;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Objects;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
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
    private final ICatalogExtension extension;
    private final JTextField tfAlias;
    private final JCheckBox cbEnabled;
    private final JRadioButton rbDefault;
    private final JButton btnConfig;
    private final JPanel extensionPanel;
    private final Runnable propertiesChangedAction;
    private boolean fireEvents = true;
    private boolean configChanged;
    
    CatalogExtensionView(
            ICatalogExtension extension,
            ButtonGroup defaultGroup,
            Runnable configChangedAction,
            Runnable propertiesChangedAction,
            Consumer<String> aliasChangedAction,
            Consumer<String> defaultCatalogChangedAction,
            Consumer<Boolean> enabledChangedAction)
    {
        this.extension = extension;
        this.propertiesChangedAction = propertiesChangedAction;
        setBorder(BorderFactory.createTitledBorder(extension.getTitle()));
        setLayout(new BorderLayout());

        JPanel topPanel = new JPanel();
        extensionPanel = new JPanel();
        extensionPanel.setLayout(new BorderLayout());

        topPanel.setLayout(new GridBagLayout());

        tfAlias = new JTextField(extension.getDefaultAlias());
        tfAlias.getDocument().addDocumentListener(new ADocumentListenerAdapter()
        {
            @Override
            protected void update()
            {
                if (fireEvents)
                {
                    aliasChangedAction.accept(tfAlias.getText());
                    // If this is default catalog also trigger that
                    if (rbDefault.isSelected())
                    {
                        defaultCatalogChangedAction.accept(tfAlias.getText());
                    }
                }
            }
        });
        
        rbDefault = new JRadioButton();
        rbDefault.setToolTipText("Set default catalog");
        rbDefault.addActionListener(l -> 
        {
            if (rbDefault.isSelected() && fireEvents)
            {
                defaultCatalogChangedAction.accept(tfAlias.getText());
            }
        });
        defaultGroup.add(rbDefault);

        btnConfig = new JButton(COG);
        cbEnabled = new JCheckBox();
        cbEnabled.setToolTipText("Enable/disable extension");
        cbEnabled.setSelected(true);
        cbEnabled.addActionListener(l ->
        {
            setEnabled();
            if (fireEvents)
            {
                enabledChangedAction.accept(cbEnabled.isSelected());
            }
        });
        btnConfig.addActionListener(l -> 
        {
            JDialog dialog = new JDialog((Frame) null, true);
            dialog.setIconImages(PayloadbuilderEditorView.APPLICATION_ICONS);
            dialog.setTitle("Config " + extension.getTitle());
            dialog.getContentPane().setLayout(new BorderLayout());
            dialog.getContentPane().add(extension.getConfigComponent(), BorderLayout.CENTER);
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
                    if (configChanged)
                    {
                        configChanged = false;
                        configChangedAction.run();
                    }
                }
            });
        });
        btnConfig.setEnabled(extension.getConfigComponent() != null);
        
        topPanel.add(rbDefault, new GridBagConstraints(0, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 5, 0), 0, 0));
        topPanel.add(cbEnabled, new GridBagConstraints(1, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 5, 0), 0, 0));
        topPanel.add(new JLabel("Alias: "), new GridBagConstraints(2, 0, 1, 0, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        topPanel.add(tfAlias,
                new GridBagConstraints(3, 0, 1, 0, 1, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
        topPanel.add(btnConfig,
                new GridBagConstraints(4, 0, 1, 0, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 5, 0), 0, -4));

        add(topPanel, BorderLayout.NORTH);
        add(extensionPanel, BorderLayout.CENTER);
        
        Component propertiesComponent = extension.getQuickPropertiesComponent();
        if (propertiesComponent != null)
        {
            extensionPanel.add(propertiesComponent, BorderLayout.CENTER);
        }

        // Add listener
        extension.addPropertyChangeListener(l ->  handlePropertyChanged(l.getPropertyName()));
    }
    
    private void handlePropertyChanged(String property)
    {
        if (CONFIG.equals(property))
        {
            configChanged = true;
        }
        else if (ICatalogExtension.PROPERTIES.equals(property))
        {
            propertiesChangedAction.run();
        }
    }
    
    private void setEnabled()
    {
        rbDefault.setEnabled(cbEnabled.isSelected());
        tfAlias.setEnabled(cbEnabled.isSelected());
        // Config cannot be enabled once disabled
        btnConfig.setEnabled(btnConfig.isEnabled() && cbEnabled.isSelected());
        setPanelEnabled(extensionPanel, cbEnabled.isSelected());
    }
    
    /** Init view from QueryFileModel */
    void init(QueryFileModel model)
    {
        fireEvents = false;
        if (Objects.equals(model.getQuerySession().getDefaultCatalogAlias(), tfAlias.getText()))
        {
            rbDefault.setSelected(true);
        }
        
        CatalogExtensionModel extensionModel = model.getCatalogExtensions().get(extension);
        tfAlias.setText(extensionModel.getAlias());
        cbEnabled.setSelected(extensionModel.isEnabled());
        setEnabled();
        // Update extension UI from query session
        extension.update(extensionModel.getAlias(), model.getQuerySession());
        fireEvents = true;
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
