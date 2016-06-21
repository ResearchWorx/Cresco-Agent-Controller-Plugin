package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;

class Executor extends CExecutor {
    private Launcher mainPlugin;
    private CLogger logger;

    Executor(Launcher plugin) {
        super(plugin);
        this.mainPlugin = plugin;
        this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
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
                msg.setParam("is_active", Boolean.toString(this.plugin.isActive()));
                logger.debug("Returning communication details to Cresco agent");
                return msg;
            default:
                logger.debug("Unknown configtype found: {}", msg.getParam("configtype"));
                return null;
        }
    }
}
