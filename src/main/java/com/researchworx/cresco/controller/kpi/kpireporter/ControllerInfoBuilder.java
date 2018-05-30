package com.researchworx.cresco.controller.kpi.kpireporter;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.TimeUnit;

class ControllerInfoBuilder {
    private Gson gson;

    private MBeanServer server;

    private Launcher plugin;
    private CLogger logger;

    public ControllerInfoBuilder(Launcher plugin) {
        this.plugin = plugin;
        this.logger = new CLogger(ControllerInfoBuilder.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        gson = new Gson();
    }

    public String getControllerInfoMap() {

        String returnStr = null;
        try {

            Map<String,List<Map<String,String>>> info = new HashMap<>();
            info.put("controller",plugin.getMeasurementEngine().getMetricGroupList("controller"));
            returnStr = gson.toJson(info);
            //logger.info(returnStr);

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return returnStr;
    }

}
