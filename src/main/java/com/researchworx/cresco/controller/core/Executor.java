package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.controller.netdiscovery.DiscoveryStatic;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryType;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;

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
                return commInit(msg);
            case "enablenetdiscovery":
                return enableNetworkDiscovery(msg);
            case "disablenetdiscovery":
                return disableNetworkDiscovery(msg);
            case "staticnetworkdiscovery":
                return staticNetworkDiscovery(msg);
            default:
                logger.debug("Unknown configtype found: {}", msg.getParam("configtype"));
                return null;
        }
    }

    MsgEvent staticNetworkDiscovery(MsgEvent msg) {
        boolean isEnabled = false;
        try {
            if(msg.getParam("action_iplist") != null) {
                String ipliststr = msg.getParam("action_iplist");
                List<String> iplist = new ArrayList<>();
                if(ipliststr.contains(",")) {
                    String[] iplistar = ipliststr.split(",");
                    for(String ip : iplistar) {
                        iplist.add(ip);
                    }
                } else {
                    iplist.add(ipliststr);
                }

                String discoveryListString = mainPlugin.getPerfMonitorNet().getStaticNetworkDiscovery(iplist);
                msg.setCompressedParam("network_map", discoveryListString);

            } else {
                logger.error("staticNetworkDiscovery: no ip list");
            }

        } catch(Exception ex) {
            logger.error("staticNetworkDiscovery: " + ex.getMessage());
            msg.setParam("error", ex.getMessage());
        }
        msg.setParam("is_complete",String.valueOf(isEnabled));
        return msg;
    }

    MsgEvent enableNetworkDiscovery(MsgEvent msg) {
        boolean isEnabled = false;
        try {
            if(mainPlugin.startNetDiscoveryEngine()) {
                isEnabled = true;
            } else {
                msg.setParam("error", "Network Discovery Failed to Start");
            }
        } catch(Exception ex) {
            logger.error("enableNetworkDiscovery: " + ex.getMessage());
            msg.setParam("error", ex.getMessage());
        }
        msg.setParam("is_discoveryactive",String.valueOf(isEnabled));
        return msg;
    }

    MsgEvent disableNetworkDiscovery(MsgEvent msg) {
        boolean isDisabled = false;
        try {
            if(mainPlugin.stopNetDiscoveryEngine()) {
                isDisabled = true;
            }
            msg.setParam("error", "Network Discovery Failed to Stop");
        } catch(Exception ex) {
            logger.error("disableNetworkDiscovery: " + ex.getMessage());
            msg.setParam("error", ex.getMessage());
        }
        msg.setParam("is_discoveryactive",String.valueOf(isDisabled));
        return msg;
    }

    MsgEvent commInit(MsgEvent msg) {
        logger.debug("comminit message type found");
        mainPlugin.commInit();
        msg.setParam("set_region", this.plugin.getRegion());
        msg.setParam("set_agent", this.plugin.getAgent());
        msg.setParam("is_regional_controller", Boolean.toString(this.mainPlugin.isRegionalController()));
        msg.setParam("is_global_controller", Boolean.toString(this.mainPlugin.isGlobalController()));
        msg.setParam("is_active", Boolean.toString(this.plugin.isActive()));
        logger.debug("Returning communication details to Cresco agent");
        return msg;
    }
}
