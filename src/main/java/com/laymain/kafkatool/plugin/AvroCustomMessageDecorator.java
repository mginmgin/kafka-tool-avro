package com.kafkatool;

import com.kafkatool.external.ICustomMessageDecorator;
import com.kafkatool.ui.MainFrame;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import javax.swing.*;
import java.awt.*;
import java.io.*;
// import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import java.util.concurrent.atomic.AtomicBoolean;

public class AvroCustomMessageDecorator implements ICustomMessageDecorator {
    private static final String DISPLAY_NAME = "AvroVsF";
    private static final String PROPERTIES_FILE = String.join(File.separator, System.getProperty("user.home"), ".com.laymain.kafkatool.plugin.avro.properties");
    private static final Properties SCHEMA_REGISTY_ENDPOINTS = loadProperties();
    private final AtomicBoolean configurationDialogOpened = new AtomicBoolean(false);
    private final AtomicBoolean menuBarInjectionDone = new AtomicBoolean(false);

    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }

    @Override
    public String decorate(String zookeeperHost, String brokerHost, String topic, long partitionId, long offset, byte[] bytes, Map<String, String> map) {
        injectMenuItem();
        Schema schema = null;
        try {
            schema = getSchemaFromFile(getSchemaRegistryEndpoint(zookeeperHost));
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            GenericRecord result = reader.read(null, decoder);
            return result.toString();
        } catch (Exception ex) {
            return "Zookeper: " + zookeeperHost + "Schema: " + Objects.requireNonNull(schema, "Empty Schema").toString() + '\n' + Arrays.toString(ex.getStackTrace());
        }
    }

/*    public static String readFileAsString(String fileName)throws Exception
    {
        String data;
        data = new String(Files.readAllBytes(Paths.get(fileName)));
        return data;
    }*/

    protected Schema getSchemaFromFile(final String path) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(new File(path));
    }

    private String getSchemaRegistryEndpoint(String zookeeperHost) {
        if (SCHEMA_REGISTY_ENDPOINTS.containsKey(zookeeperHost)) {
            return SCHEMA_REGISTY_ENDPOINTS.getProperty(zookeeperHost);
        }
        if (configurationDialogOpened.compareAndSet(false, true)) {
            // Double check
            if (SCHEMA_REGISTY_ENDPOINTS.containsKey(zookeeperHost)) {
                configurationDialogOpened.set(false);
                return SCHEMA_REGISTY_ENDPOINTS.getProperty(zookeeperHost);
            }
            SwingUtilities.invokeLater(() -> {
                if (!SCHEMA_REGISTY_ENDPOINTS.containsKey(zookeeperHost)) {
                    String endpoint = JOptionPane.showInputDialog(String.format("Enter schema registry endpoint for %s", zookeeperHost));
                    if (endpoint != null && !endpoint.isEmpty()) {
                        SCHEMA_REGISTY_ENDPOINTS.setProperty(zookeeperHost, endpoint);
                        saveProperties();
                    }
                }
                configurationDialogOpened.set(false);
            });
        }
        return null;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try {
            File propertiesFile = Paths.get(PROPERTIES_FILE).toFile();
            if (propertiesFile.exists()) {
                try (FileInputStream input = new FileInputStream(PROPERTIES_FILE)) {
                    properties.load(input);
                }
            } else {
                //noinspection ResultOfMethodCallIgnored
                propertiesFile.createNewFile(); //NOSONAR
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(MainFrame.getInstance(), e.toString(), "Cannot load avro plugin properties", JOptionPane.ERROR_MESSAGE);
        }
        return properties;
    }

    private static void saveProperties() {
        try (FileOutputStream output = new FileOutputStream(PROPERTIES_FILE)) {
            AvroCustomMessageDecorator.SCHEMA_REGISTY_ENDPOINTS.store(output, "Schema registry per cluster endpoints");
        } catch (Exception e) {
            JOptionPane.showMessageDialog(MainFrame.getInstance(), e.toString(), "Cannot save avro plugin properties", JOptionPane.ERROR_MESSAGE);
        }
    }

    private static final int MENUBAR_TOOLS_INDEX = 2;
    private static final String PLUGIN_MENU_ITEM_CAPTION = "Avro plugin settings...";
    private void injectMenuItem() {
        if (menuBarInjectionDone.compareAndSet(false, true)) {
            JMenu menu = MainFrame.getInstance().getJMenuBar().getMenu(MENUBAR_TOOLS_INDEX);
            if (menu != null) {
                menu.addSeparator();
                JMenuItem menuItem = new JMenuItem(PLUGIN_MENU_ITEM_CAPTION, PLUGIN_MENU_ITEM_CAPTION.charAt(0));
                menuItem.addActionListener(actionEvent -> editProperties(Paths.get(PROPERTIES_FILE).toFile()));
                menu.add(menuItem);
            }
        }
    }

    private static void editProperties(File propertyFile) {
        try {
            Desktop.getDesktop().edit(propertyFile);
        } catch (IOException e0) {
            try {
                Desktop.getDesktop().open(propertyFile);
            } catch (IOException e1) {
                final String message = "Cannot open configuration file in editor";
                JOptionPane.showMessageDialog(MainFrame.getInstance(), e1.toString(), message, JOptionPane.ERROR_MESSAGE);
            }
        }
    }
}
