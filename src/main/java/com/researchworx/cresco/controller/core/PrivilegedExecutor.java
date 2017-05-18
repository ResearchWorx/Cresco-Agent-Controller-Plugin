package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;

class PrivilegedExecutor extends CExecutor {
    private Launcher mainPlugin;
    private CLogger logger;

    PrivilegedExecutor(Launcher plugin) {
        super(plugin);
        this.mainPlugin = plugin;
        this.logger = new CLogger(PrivilegedExecutor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
    }

    @Override
    public MsgEvent processConfig(MsgEvent msg) {
        logger.trace("Processing Config message");
        if (msg.getParam("privilegedtype") == null) return null;
        logger.debug("privileged-type is properly set");
        switch (msg.getParam("privilegedtype")) {

            case "regionalimport":

            default:
                logger.debug("Unknown privilegedtype found: {}", msg.getParam("privilegedtype"));
                return null;
        }
    }


}
