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

    private long messageCount = 0;

    Executor(Launcher plugin) {
        super(plugin);
        this.mainPlugin = plugin;
        this.logger = new CLogger(Executor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
    }

    @Override
    public MsgEvent processExec(MsgEvent ce) {
        logger.trace("Processing Exec message");

            switch (ce.getParam("action")) {

                case "ping":
                    return pingReply(ce);

                case "noop":
                    noop();
                    break;

                default:
                    logger.error("Unknown configtype found {} for {}:", ce.getParam("action"), ce.getMsgType().toString());

            }

        return null;
    }

    @Override
    public MsgEvent processConfig(MsgEvent ce) {
        logger.trace("Processing Config message");

            switch (ce.getParam("action")) {
                case "comminit":
                    return commInit(ce);

                default:
                    logger.error("Unknown configtype found {} for {}:", ce.getParam("action"), ce.getMsgType().toString());

            }

        return null;
    }

    private MsgEvent pingReply(MsgEvent msg) {


        if(msg.getParam("reversecount") != null) {

            long starttime = System.currentTimeMillis();
            int count = 1;

            int samples = Integer.parseInt(msg.getParam("reversecount"));

            //RPCCall rpc = new RPCCall();

            while(count < samples) {
                MsgEvent me = new MsgEvent(MsgEvent.Type.EXEC, mainPlugin.getRegion(), mainPlugin.getAgent(), mainPlugin.getPluginID(), "external");
                me.setParam("action","ping");
                //me.setParam("action","noop");
                me.setParam("src_region", mainPlugin.getRegion());
                me.setParam("src_agent", mainPlugin.getAgent());
                me.setParam("src_plugin", mainPlugin.getPluginID());
                me.setParam("dst_region", me.getParam("src_region"));
                me.setParam("dst_agent", me.getParam("src_agent"));
                if(msg.getParam("src_plugin") != null) {
                    me.setParam("dst_plugin", msg.getParam("src_plugin"));
                }
                me.setParam("count",String.valueOf(count));
                //msgIn(me);
                //System.out.print(".");
                MsgEvent re = mainPlugin.getRPC().call(me);
                count++;
            }

            long endtime = System.currentTimeMillis();
            long elapsed = (endtime - starttime);
            float timemp = elapsed/samples;
            float mps = -1;
            try {
                mps = samples / ((endtime - starttime) / 1000);
            } catch(Exception ex) {
                //do nothing
            }
            msg.setParam("elapsedtime",String.valueOf(elapsed));
            msg.setParam("timepermessage",String.valueOf(timemp));
            msg.setParam("mps",String.valueOf(mps));
            msg.setParam("samples",String.valueOf(samples));
        }

        msg.setParam("action","pong");
        logger.debug("ping message type found");
        msg.setParam("remote_ts", String.valueOf(System.currentTimeMillis()));
        msg.setParam("type", "agent_controller");
        logger.debug("Returning communication details to Cresco agent");
        return msg;
    }

    private void noop() {

    }

        /*

        if (msg.getParam("configtype") == null || msg.getMsgBody() == null) return null;
        logger.debug("Config-type is properly set, as well as message body");
        switch (msg.getParam("action")) {
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
        */



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
