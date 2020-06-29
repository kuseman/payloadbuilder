package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.editor.catalog.ICatalogExtension;
import com.viskan.payloadbuilder.editor.catalog.Property;
import com.viskan.payloadbuilder.editor.catalog.Property.Presentation;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.lang3.tuple.Pair;

/** View for extension */
class CatalogExtensionView extends JPanel
{
    private final ICatalogExtension extension;

    CatalogExtensionView(ICatalogExtension extension, ButtonGroup defaultGroup)
    {
        this.extension = extension;

        setBorder(BorderFactory.createTitledBorder(extension.getTitle()));

        setLayout(new BorderLayout());

        JPanel topPanel = new JPanel();
        JPanel extensionPanel = new JPanel();

        topPanel.setLayout(new GridBagLayout());

        JTextField tfAlias = new JTextField(extension.getDefaultAlias());

        JRadioButton rbDefault = new JRadioButton();
        rbDefault.setToolTipText("Set default catalog");
        defaultGroup.add(rbDefault);

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
            setPanelEnabled(extensionPanel, cbEnabled.isSelected());
        });

        topPanel.add(rbDefault, new GridBagConstraints(0, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        topPanel.add(cbEnabled, new GridBagConstraints(1, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        topPanel.add(new JLabel("Alias"), new GridBagConstraints(2, 0, 1, 0, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 5), 0, 0));
        topPanel.add(tfAlias,
                new GridBagConstraints(3, 0, 1, 0, 1, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        add(topPanel, BorderLayout.NORTH);
        add(extensionPanel, BorderLayout.CENTER);

        Map<String, List<Pair<Method, Property>>> groups = MethodUtils.getMethodsListWithAnnotation(extension.getClass(), Property.class)
                .stream()
                .map(m -> Pair.of(m, m.getAnnotation(Property.class)))
                .sorted((a, b) -> a.getValue().sort() - b.getValue().sort())
                .collect(groupingBy(p -> p.getValue().group(),
                        LinkedHashMap::new,
                        toList()));

        JTabbedPane tpGroups = null;
        if (groups.size() == 1)
        {
            extensionPanel.setLayout(new GridBagLayout());
        }
        else
        {
            extensionPanel.setLayout(new BorderLayout());
            tpGroups = new JTabbedPane();
            tpGroups.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);
            extensionPanel.add(tpGroups, BorderLayout.CENTER);
        }

        Map<String, Runnable> propertySetterByName = new HashMap<>();
        Map<String, Consumer<Object>> propertyListSetterByName = new HashMap<>();

        for (Entry<String, List<Pair<Method, Property>>> entry : groups.entrySet())
        {
            JPanel content = extensionPanel;
            if (tpGroups != null)
            {
                content = new JPanel();
                content.setLayout(new GridBagLayout());
                tpGroups.add(entry.getKey(), content);
            }

            int row = 0;
            for (int i = 0; i < entry.getValue().size(); i++)
            {
                Method getMethod = entry.getValue().get(i).getKey();
                final Property property = entry.getValue().get(i).getValue();
                if (property.presentation() != Presentation.ACTION && (getMethod.getReturnType() == Void.TYPE || getMethod.getParameterCount() != 0))
                {
                    throw new IllegalArgumentException("Property annotation should be placed on getter, for: " + getMethod.getName());
                }
                Method setMethod = getSetMethod(getMethod);

                int weightY = i == entry.getValue().size() - 1 ? 1 : 0;
                GridBagConstraints labelConstraints = new GridBagConstraints(0, row, 1, 1, 0, weightY, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 5, 5), 0, 0);
                if (property.presentation() != Presentation.ACTION)
                {
                    content.add(new JLabel(property.title()), labelConstraints);
                }
                GridBagConstraints componentConstraints = new GridBagConstraints(1, row, 1, 1, 1, weightY, GridBagConstraints.BASELINE_TRAILING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 5, 0),
                        0,
                        0);

                Runnable propertySetter = null;
                Consumer<Object> propertyListSetter = null;

                switch (property.presentation())
                {
                    case STRING:
                    case INTEGER:
                        final JTextField tf = new JTextField();
                        if (setMethod == null)
                        {
                            throw new IllegalArgumentException("Could not find set method for property " + property.title());
                        }

                        // Flag to avoid propagation of value during list population
                        final AtomicBoolean setValue = new AtomicBoolean(true);

                        propertySetter = () ->
                        {
                            if (setValue.get())
                            {
                                tf.setText(String.valueOf(invoke(getMethod)));
                            }
                        };
                        tf.getDocument().addDocumentListener(new ADocumentListenerAdapter()
                        {
                            @Override
                            protected void update()
                            {
                                setValue.set(false);
                                invoke(setMethod, tf.getText());
                                setValue.set(true);
                            }
                        });
                        content.add(tf, componentConstraints);
                        break;
                    case PASSWORD:
                        final JPasswordField pf = new JPasswordField();
                        if (setMethod == null)
                        {
                            throw new IllegalArgumentException("Could not find set method for property " + property.title());
                        }
                        propertySetter = () -> pf.setText(String.valueOf(invoke(getMethod)));
                        pf.getDocument().addDocumentListener(new ADocumentListenerAdapter()
                        {
                            @Override
                            protected void update()
                            {
                                invoke(setMethod, pf.getPassword());
                            }
                        });
                        content.add(pf, componentConstraints);
                        break;
                    case LIST:
                        if (setMethod == null)
                        {
                            throw new IllegalArgumentException("Could not find set method for property " + property.title());
                        }
                        else if (isBlank(property.itemsPropertyName()))
                        {
                            throw new IllegalArgumentException("Missing itemsPropertyName for property " + property.title());
                        }
                        final JComboBox<Object> cbItems = new JComboBox<>();
                        cbItems.setMaximumRowCount(property.itemsListSize());
                        DefaultComboBoxModel<Object> model = new DefaultComboBoxModel<>();
                        cbItems.setModel(model);

                        // Flag to avoid propagation of value during list population
                        final AtomicBoolean setValue1 = new AtomicBoolean(true);
                        propertySetter = () ->
                        {
                            if (setValue1.get())
                            {
                                model.setSelectedItem(invoke(getMethod));
                            }
                        };
                        propertyListSetter = items ->
                        {
                            setValue1.set(false);
                            Object prevItem = model.getSelectedItem();
                            model.removeAllElements();
                            @SuppressWarnings("unchecked")
                            Collection<Object> col = (Collection<Object>) items;
                            for (Object obj : col)
                            {
                                model.addElement(obj);
                            }
                            if (model.getSize() > 0)
                            {
                                model.setSelectedItem(prevItem);
                            }
                            setValue1.set(true);
                        };

                        cbItems.addActionListener(e ->
                        {
                            if (setValue1.get())
                            {
                                invoke(setMethod, model.getSelectedItem());
                            }
                        });
                        content.add(cbItems, componentConstraints);
                        break;
                    case ACTION:
                        final JButton btn = new JButton(property.title());
                        btn.addActionListener(l -> invoke(getMethod));
                        content.add(btn, componentConstraints);
                        break;
                }

                if (propertySetter != null)
                {
                    if (propertySetterByName.put(property.name(), propertySetter) != null)
                    {
                        throw new IllegalArgumentException("Duplicate property " + property.name());
                    }
                    // Run property setting initially to set default values
                    propertySetter.run();
                }
                if (propertyListSetter != null
                    && propertyListSetterByName.put(property.itemsPropertyName(), propertyListSetter) != null)
                {
                    throw new IllegalArgumentException("Duplicate items property " + property.itemsPropertyName());
                }

                row++;
            }
        }

        extension.addPropertyChangeListener(l ->
        {
            Runnable propertySetter = propertySetterByName.get(l.getPropertyName());
            if (propertySetter != null)
            {
                propertySetter.run();
                return;
            }
            Consumer<Object> propertyListSetter = propertyListSetterByName.get(l.getPropertyName());
            if (propertyListSetter != null)
            {
                propertyListSetter.accept(l.getNewValue());
            }
        });
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

    private Method getSetMethod(Method getter)
    {
        String setMethodName = getter.getName().replace("get", "set");
        try
        {
            return ClassUtils.getPublicMethod(extension.getClass(), setMethodName, getter.getReturnType());
        }
        catch (NoSuchMethodException e)
        {
            return null;
        }
    }

    private Object invoke(Method method, Object... args)
    {
        try
        {
            return method.invoke(extension, args);
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
        {
            throw new RuntimeException("Error invoking method " + method.getName(), e);
        }
    }
}
