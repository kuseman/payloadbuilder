package org.kuse.payloadbuilder.editor;

import static org.kuse.payloadbuilder.editor.PayloadbuilderEditorController.MAPPER;

import java.io.File;
import java.io.IOException;

import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

/** Main of PayloadbuilderEditor */
public class Main
{
    private static final File CONFIG_FILE = new File("config.json");

    /** Main */
    public static void main(String[] args)
    {
        try
        {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException ex)
        {
        }

        Config config = loadConfig();

        // Start all Swing applications on the EDT.
        SwingUtilities.invokeLater(() ->
        {
            PayloadbuilderEditorView view = new PayloadbuilderEditorView();
            PayloadbuilderEditorModel model = new PayloadbuilderEditorModel();
            new PayloadbuilderEditorController(config, view, model);

            view.setVisible(true);
        });
    }

    private static Config loadConfig()
    {
        if (!CONFIG_FILE.exists())
        {
            return new Config();
        }
        try
        {
            Config config = MAPPER.readValue(CONFIG_FILE, Config.class);
            config.initExtensions();
            return config;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error loading config from " + CONFIG_FILE.getAbsolutePath(), e);
        }
    }

    static void saveConfig(Config config)
    {
        try
        {
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(CONFIG_FILE, config);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error saving config to " + CONFIG_FILE.getAbsolutePath(), e);
        }
    }
}
