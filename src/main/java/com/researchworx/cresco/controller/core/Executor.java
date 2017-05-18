package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;

class Executor extends CExecutor {
    private Launcher mainPlugin;
    private CLogger logger;
    private PrivilegedExecutor pe;

    Executor(Launcher plugin) {
        super(plugin);
        this.mainPlugin = plugin;
        this.pe = new PrivilegedExecutor(plugin);
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
                logger.debug("regionalimport message type found");
                logger.debug(msg.getParam("exportdata"));
                if(mainPlugin.getGDB().gdb.setDBImport(msg.getParam("exportdata"))) {
                    logger.debug("Database Imported.");
                }
                else {
                    logger.debug("Database Import Failed!");
                }
                //return msg;
            case "privileged":
                //put privileged auth here
                return pe.processConfig(msg);

            default:
                logger.debug("Unknown configtype found: {}", msg.getParam("configtype"));
                return null;
        }
    }


}
