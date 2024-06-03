import java.util.Properties;
import java.io.*;

public class ConfigManager {
    private static ConfigManager instance;
        private Properties configProps;
    
        private ConfigManager() {
            configProps = new Properties();
            try (InputStream input = new FileInputStream("config.properties")) {
                configProps.load(input);
            } catch (IOException e) {
                e.printStackTrace();
                // Handle error appropriately
            }
        }
        // Ensures that the singleton instance is created safely in a multithreaded environment
        public static synchronized ConfigManager getInstance() {
            if (instance == null) {
                instance = new ConfigManager();
            }
            return instance;
        }
    
        public String getProperty(String key) {
            return configProps.getProperty(key);
        }
    
}
