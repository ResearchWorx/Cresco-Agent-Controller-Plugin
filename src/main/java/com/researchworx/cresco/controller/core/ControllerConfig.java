package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.core.Config;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class ControllerConfig extends Config {
    //public ControllerConfig(String configFile) throws ConfigurationException {
    //    super(configFile);
    //}

    public ControllerConfig (SubnodeConfiguration config) {
        super(config);
    }

    public Map<String, String> buildPluginMap(String configparams) {

        Map<String, String> configMap = new HashMap<>();
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
            return this.getStringParam("controllerip");
        } catch (Exception ex) {
            System.out.println("Controller : PluginConfig : ERROR " + ex.toString());
            return null;
        }
    }

    public Map<String, String> getPluginConfigObject() {
        final Map<String, String> result = new TreeMap<>();
        final Iterator it = this.confObj.getKeys();
        while (it.hasNext()) {
            final Object key = it.next();
            final String value = getStringParam(key.toString());
            result.put(key.toString(), value);
        }
        return result;
    }

    public String getPluginConfigString() {
        //final Map<String,String> result=new TreeMap<String,String>();
        StringBuilder sb = new StringBuilder();
        final Iterator it = this.confObj.getKeys();
        while (it.hasNext()) {
            final Object key = it.next();
            final String value = getStringParam(key.toString());
            //result.put(key.toString(),value);
            sb.append(key.toString() + "=" + value + ",");
        }
        return sb.toString().substring(0, sb.length() - 1);
        //return result;
    }

    public String getControllerPort() {
        try {
            return getStringParam("controllerport");
        } catch (Exception ex) {
            System.out.println("Controller : PluginConfig : ERROR " + ex.toString());
            return null;
        }
    }
}
