package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor extends CExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CExecutor.class);

    private Launcher mainPlugin;

    public Executor(Launcher plugin) {
        super(plugin);
        this.mainPlugin = plugin;
    }

    @Override
    public MsgEvent processConfig(MsgEvent msg) {
        if (msg.getParam("configtype") == null || msg.getMsgBody() == null) return null;

        switch (msg.getParam("configtype")) {
            case "comminit":
                mainPlugin.commInit();
                msg.setParam("set_region", this.plugin.getRegion());
                msg.setParam("set_agent", this.plugin.getAgent());
                msg.setParam("is_regional_controller", Boolean.toString(this.mainPlugin.isRegionalController()));
                msg.setParam("is_active", Boolean.toString(this.plugin.isActive()));
                return msg;
            default:
                return null;
        }
    }

    @Override
    public MsgEvent processExec(MsgEvent msg) {
        switch (msg.getParam("cmd")) {
            case "show_name":
                msg.setMsgBody(this.plugin.getName());
                return msg;
            case "show_version":
                msg.setMsgBody(this.plugin.getVersion());
                return msg;
            default:
                return null;
        }
    }
}
