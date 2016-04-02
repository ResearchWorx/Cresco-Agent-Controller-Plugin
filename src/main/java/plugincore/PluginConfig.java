package plugincore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;

public class PluginConfig {

    private SubnodeConfiguration configObj;


    public PluginConfig(SubnodeConfiguration configObj) throws ConfigurationException {
        this.configObj = configObj;

    }

    public String getPluginName() {
        return configObj.getString("pluginname");
    }

    public String getRegion() {
        return configObj.getString("region");
    }

    public int getWatchDogTimer() {
        return configObj.getInt("watchdogtimer");
    }

    public Integer getIntParam(String param) {
        return Integer.parseInt(configObj.getString(param));
    }

    public String getStringParam(String param) {
        return configObj.getString(param);
    }

    public Map<String, String> buildPluginMap(String configparams) {

        Map<String, String> configMap = new HashMap<String, String>();
        try {
            String[] configLines = configparams.split(",");
            for (String config : configLines) {
                String[] configs = config.split("=");
                configMap.put(configs[0], configs[1]);
            }
        } catch (Exception ex) {
            System.out.println("Controller : PluginConfig : buildconfig ERROR : " + ex.toString());
        }
        return configMap;
    }

    public String getControllerIP() {
        try {
            return configObj.getString("controllerip");
        } catch (Exception ex) {
            System.out.println("Controller : PluginConfig : ERROR " + ex.toString());
            return null;
        }
    }

    public Map<String, String> getPluginConfig() {
        final Map<String, String> result = new TreeMap<String, String>();
        final Iterator it = configObj.getKeys();
        while (it.hasNext()) {
            final Object key = it.next();
            final String value = configObj.getString(key.toString());
            result.put(key.toString(), value);
        }
        return result;
    }

    public String getPluginConfigString() {
        //final Map<String,String> result=new TreeMap<String,String>();
        StringBuilder sb = new StringBuilder();
        final Iterator it = configObj.getKeys();
        while (it.hasNext()) {
            final Object key = it.next();
            final String value = configObj.getString(key.toString());
            //result.put(key.toString(),value);
            sb.append(key.toString() + "=" + value + ",");
        }
        return sb.toString().substring(0, sb.length() - 1);
        //return result;
    }

    public String getControllerPort() {
        try {
            return configObj.getString("controllerport");
        } catch (Exception ex) {
            System.out.println("Controller : PluginConfig : ERROR " + ex.toString());
            return null;
        }
    }
}