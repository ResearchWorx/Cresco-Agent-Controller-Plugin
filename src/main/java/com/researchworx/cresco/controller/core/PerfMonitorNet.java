package com.researchworx.cresco.controller.core;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryClientIPv4;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryClientIPv6;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryType;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

class PerfMonitorNet {

    private Launcher plugin;

    public Timer timer;
    private boolean running = false;
    private Gson gson;
    private CLogger logger;
    private boolean polling = false;

    private DiscoveryClientIPv4 ip4dc;
    private DiscoveryClientIPv6 ip6dc;


    PerfMonitorNet(Launcher plugin) {
        this.plugin = plugin;
        this.logger = new CLogger(PerfMonitorNet.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        gson = new Gson();
        plugin.isStarted = true;
    }

    PerfMonitorNet start() {
        if (this.running) return this;
        Long interval = plugin.getConfig().getLongParam("perftimer", 10000L);

        MsgEvent initial = new MsgEvent(MsgEvent.Type.INFO, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Performance Monitoring timer set to " + interval + " milliseconds.");
        initial.setParam("src_region", plugin.getRegion());
        initial.setParam("src_agent", plugin.getAgent());
        initial.setParam("src_plugin", plugin.getPluginID());
        initial.setParam("dst_region", plugin.getRegion());
        plugin.sendMsgEvent(initial);

        timer = new Timer();
        timer.scheduleAtFixedRate(new PerfMonitorTask(plugin), 5000, interval);
        return this;
    }

    PerfMonitorNet restart() {
        if (running) timer.cancel();
        running = false;
        return start();
    }

    void stop() {
        timer.cancel();
        running = false;
    }

    private List<MsgEvent> getNetworkDiscoveryList() {


        List<MsgEvent> discoveryList = null;
        polling = true;
        try {

            discoveryList = new ArrayList<>();
            if (plugin.isIPv6()) {
                if(ip6dc == null) {
                    ip6dc = new DiscoveryClientIPv6(plugin);
                }
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(ip6dc.getDiscoveryResponse(DiscoveryType.NETWORK, plugin.getConfig().getIntegerParam("discovery_ipv6_agent_timeout", 2000)));
                logger.debug("IPv6 Broker count = {} " + discoveryList.size());
            }
            if(ip4dc == null) {
                ip4dc = new DiscoveryClientIPv4(plugin);
            }
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(ip4dc.getDiscoveryResponse(DiscoveryType.NETWORK, plugin.getConfig().getIntegerParam("discovery_ipv4_agent_timeout", 2000)));
            logger.debug("Broker count = {} " + discoveryList.size());

            //for (MsgEvent me : discoveryList) {
            //    logger.debug(me.getParams().toString());
            //}
        }
        catch(Exception ex) {
            logger.error("getNetworkDiscoveryList() " + ex.getMessage());
        }
        polling = false;
        return discoveryList;
    }


    private class PerfMonitorTask extends TimerTask {
        private Launcher plugin;

        PerfMonitorTask(Launcher plugin) {
            this.plugin = plugin;
        }

        public void run() {

            if(!polling) {
                MsgEvent tick = new MsgEvent(MsgEvent.Type.KPI, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Performance Monitoring tick.");
                tick.setParam("src_region", plugin.getRegion());
                tick.setParam("src_agent", plugin.getAgent());
                tick.setParam("src_plugin", plugin.getPluginID());
                tick.setParam("dst_region", plugin.getRegion());
                tick.setParam("resource_id", plugin.getConfig().getStringParam("resource_id", "netdiscovery_resource"));
                tick.setParam("inode_id", plugin.getConfig().getStringParam("inode_id", "netdiscovery_inode"));

                //if(!plugin.isDiscoveryActive()) {
                    List<MsgEvent> discoveryList = getNetworkDiscoveryList();
                    String discoveryListString = null;
                    if (discoveryList != null) {
                        discoveryListString = gson.toJson(discoveryList);
                    }
                    tick.setCompressedParam("network_map", discoveryListString);

                    plugin.msgIn(tick);
                //}
            }
        }
    }
}
