package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.controller.graphdb.DBImport;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class Executor extends CExecutor {
    private Launcher mainPlugin;
    private CLogger logger;

    Executor(Launcher plugin) {
        super(plugin);
        this.mainPlugin = plugin;
        this.logger = new CLogger(Executor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
    }

    @Override
    public MsgEvent processConfig(MsgEvent msg) {
        logger.trace("Processing Config message");
        if (msg.getParam("configtype") == null || msg.getMsgBody() == null) return null;
        logger.debug("Config-type is properly set, as well as message body");
        switch (msg.getParam("configtype")) {
            case "comminit":
                logger.debug("comminit message type found");
                mainPlugin.commInit();
                msg.setParam("set_region", this.plugin.getRegion());
                msg.setParam("set_agent", this.plugin.getAgent());
                msg.setParam("is_regional_controller", Boolean.toString(this.mainPlugin.isRegionalController()));
                msg.setParam("is_global_controller", Boolean.toString(this.mainPlugin.isGlobalController()));
                msg.setParam("is_active", Boolean.toString(this.plugin.isActive()));
                logger.debug("Returning communication details to Cresco agent");
                return msg;
            //there has to be a better way to do this
            case "regionalimport":
                logger.info("regionalimport message type found");
                logger.info(msg.getParam("exportdata"));
                if(mainPlugin.getGDB().setRDBImport(msg.getParam("exportdata"))) {
                    logger.info("Database Imported.");
                }
                else {
                    logger.error("Database Import Failed!");
                }

            default:
                logger.debug("Unknown configtype found: {}", msg.getParam("configtype"));
                return null;
        }
    }


}
