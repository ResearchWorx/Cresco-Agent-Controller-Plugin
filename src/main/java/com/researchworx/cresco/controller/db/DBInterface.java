package com.researchworx.cresco.controller.db;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBInterface {

    private Launcher plugin;
    private CLogger logger;
    private DBEngine gde;
    public DBBaseFunctions gdb;
    public DBApplicationFunctions dba;

    public DBInterface(Launcher plugin) {
        this.logger = new CLogger(DBInterface.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.plugin = plugin;
        this.gde = new DBEngine(plugin);
        this.gdb = new DBBaseFunctions(plugin,gde);
        this.dba = new DBApplicationFunctions(plugin,gde);
    }

    public Map<String,String> paramStringToMap(String param) {
        Map<String,String> params = null;
        try
        {
            params = new HashMap<String,String>();
            String[] pstr = param.split(",");
            for(String str : pstr)
            {
                String[] pstrs = str.split("=");
                params.put(pstrs[0], pstrs[1]);
            }
        }
        catch(Exception ex)
        {
            logger.error("paramStringToMap " + ex.toString());
        }
        return params;
    }

    public Map<String,String> getResourceTotal() {
        Map<String,String> resourceTotal = null;
        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;
        long region_count = 0;
        long agent_count = 0;
        long plugin_count = 0;

        try
        {


            resourceTotal = new HashMap<>();
            List<String> sysInfoEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");
            for(String edgeID : sysInfoEdgeList) {

                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);
                cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                for (String fspair : edgeParams.get("fs-map").split(",")) {
                    String[] fskey = fspair.split(":");
                    diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                    diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                }
            }

            List<String> regionList = gdb.getNodeList(null,null,null);
            //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
            //Map<String,String> rMap = new HashMap<String,String>();
            if(regionList != null) {
                for (String region : regionList) {
                    region_count++;
                    logger.trace("Region : " + region);
                    List<String> agentList = gdb.getNodeList(region, null, null);
                    if (agentList != null) {
                        for (String agent : agentList) {
                            agent_count++;
                            logger.trace("Agent : " + agent);

                            List<String> pluginList = gdb.getNodeList(region, agent, null);
                            if (pluginList != null) {

                                boolean isRecorded = false;
                                for (String pluginId : pluginList) {
                                    logger.trace("Plugin : " + plugin);
                                    plugin_count++;
                                }
                            }
                        }
                    }
                }
            }

            //logger.trace("Regions :" + region_count);
            //logger.trace("Agents :" + agent_count);
            //logger.trace("Plugins : " + plugin_count);
            logger.trace("Total CPU core count : " + cpu_core_count);
            logger.trace("Total Memory Available : " + memoryAvailable);
            logger.trace("Total Memory Total : " + memoryTotal);
            logger.trace("Total Disk Available : " + diskAvailable);
            logger.trace("Total Disk Total : " + diskTotal);
            resourceTotal.put("regions",String.valueOf(region_count));
            resourceTotal.put("agents",String.valueOf(agent_count));
            resourceTotal.put("plugins",String.valueOf(plugin_count));
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));

        }
        catch(Exception ex)
        {
            logger.error("getResourceTotal() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return resourceTotal;
    }

    public Map<String,String> getResourceTotal2() {
        Map<String,String> resourceTotal = null;
        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;
        long region_count = 0;
        long agent_count = 0;
        long plugin_count = 0;

        try
        {
            /*
            logger.info("CODY START");
            List<String> sysInfoEdgeList = gdb.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");
            for(String edgeID : sysInfoEdgeList) {
                logger.info("ID = " + edgeID);
                //logger.info(gdb.getIsAssignedParam(String edge_id,String param_name)
                String region = gdb.getIsAssignedParam(edgeID,"region");
                String agent = gdb.getIsAssignedParam(edgeID,"agent");
                String pluginID = gdb.getIsAssignedParam(edgeID,"plugin");

                Map<String,String> edgeParams = gdb.getIsAssignedParams(edgeID);
                for (Map.Entry<String, String> entry : edgeParams.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    logger.info("key=" + key + " value=" + value);
                }

                logger.info(region + " " + agent + " " + pluginID);
            }

            logger.info("CODY END");
            */

            //public List<String> getIsAssignedEdgeIds(String resource_id, String inode_id)

            resourceTotal = new HashMap<>();
            List<String> regionList = gdb.getNodeList(null,null,null);
            //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
            //Map<String,String> rMap = new HashMap<String,String>();
            if(regionList != null) {
                for (String region : regionList) {
                    region_count++;
                    logger.trace("Region : " + region);
                    List<String> agentList = gdb.getNodeList(region, null, null);
                    if (agentList != null) {
                        for (String agent : agentList) {
                            agent_count++;
                            logger.trace("Agent : " + agent);

                            List<String> pluginList = gdb.getNodeList(region, agent, null);
                            if (pluginList != null) {

                                boolean isRecorded = false;
                                for (String pluginId : pluginList) {
                                    logger.trace("Plugin : " + plugin);
                                    plugin_count++;
                                    if (!isRecorded) {
                                        String pluginConfigparams = gdb.getNodeParam(region, agent, pluginId, "configparams");
                                        logger.trace("configParams : " + pluginConfigparams);
                                        if (pluginConfigparams != null) {
                                            Map<String, String> pMap = paramStringToMap(pluginConfigparams);
                                            if (pMap.get("pluginname").equals("cresco-sysinfo-plugin")) {

                                                String isAssignedEdgeId = dba.getResourceEdgeId("sysinfo_resource", "sysinfo_inode",region,agent,pluginId);
                                                Map<String,String> edgeParams = dba.getIsAssignedParams(isAssignedEdgeId);
                                                cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                                                memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                                                memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                                                for(String fspair : edgeParams.get("fs-map").split(",")) {
                                                    String[] fskey = fspair.split(":");
                                                    diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                                                    diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                                                }
                                                /*
                                                System.out.println("region=" + region + " agent=" + agent);
                                                String agent_path = region + "_" + agent;
                                                String agentConfigparams = gdb.getNodeParam(region, agent, null, "configparams");
                                                Map<String, String> aMap = paramStringToMap(agentConfigparams);
                                                //String resourceKey = aMap.get("platform") + "_" + aMap.get("environment") + "_" + aMap.get("location");

                                                for (Map.Entry<String, String> entry : pMap.entrySet()) {
                                                    String key = entry.getKey();
                                                    String value = entry.getValue();
                                                    System.out.println("\t" + key + ":" + value);
                                                }
                                                isRecorded = true;
                                                */
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            logger.trace("Regions :" + region_count);
            logger.trace("Agents :" + agent_count);
            logger.trace("Plugins : " + plugin_count);
            logger.trace("Total CPU core count : " + cpu_core_count);
            logger.trace("Total Memory Available : " + memoryAvailable);
            logger.trace("Total Memory Total : " + memoryTotal);
            logger.trace("Total Disk Available : " + diskAvailable);
            logger.trace("Total Disk Total : " + diskTotal);
            resourceTotal.put("regions",String.valueOf(region_count));
            resourceTotal.put("agents",String.valueOf(agent_count));
            resourceTotal.put("plugins",String.valueOf(plugin_count));
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));

        }
        catch(Exception ex)
        {
            logger.error("getResourceTotal() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return resourceTotal;
    }

    public Map<String,NodeStatusType> getNodeStatus(String region, String agent, String plugin) {

        Map<String,NodeStatusType> nodeStatusMap = null;
        try {
            nodeStatusMap = new HashMap<>();
            List<String> queryList = gdb.getNodeIds(region,agent,plugin,false);
            if((region != null) && (agent == null)) {
                //region queries should include plugins
                for(String agentNodeId : queryList) {
                    agent = gdb.getNodeParam(agentNodeId,"agent");
                    nodeStatusMap.putAll(getNodeStatus(region,agent,null));
                }
            }

            for(String nodeId : queryList) {
                Map<String,String> params = gdb.getNodeParams(nodeId);
                if((params.containsKey("watchdogtimer") && (params.containsKey("watchdog_ts"))  && (params.containsKey("is_active")))) {
                    long watchdog_ts = Long.parseLong(params.get("watchdog_ts"));
                    long watchdog_rate = Long.parseLong(params.get("watchdogtimer"));
                    boolean isActive = Boolean.parseBoolean(params.get("is_active"));
                    if(isActive) {
                        long watchdog_diff = System.currentTimeMillis() - watchdog_ts;
                        if (watchdog_diff > (watchdog_rate * 3)) {
                            //node is stale
                            logger.trace(nodeId + " is stale");
                            nodeStatusMap.put(nodeId, NodeStatusType.STALE);
                        } else {
                            nodeStatusMap.put(nodeId, NodeStatusType.ACTIVE);
                        }
                    }
                    else {
                        logger.trace(nodeId + " is lost");
                        nodeStatusMap.put(nodeId, NodeStatusType.LOST);
                    }
                }
                else {
                    //Plugins will trigger this problem, need to fix Cresco Library
                    //logger.error(nodeId + " could not find watchdog_ts or watchdog_rate");
                    nodeStatusMap.put(nodeId,NodeStatusType.ERROR);
                }
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return nodeStatusMap;
    }

    public Boolean addNode(MsgEvent de) {
        Boolean wasAdded = false;

        try {

            String region = de.getParam("src_region");
            String agent = de.getParam("src_agent");
            String plugin = de.getParam("src_plugin");

            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId != null) {
                logger.trace("region: " + region + " agent:" + agent + " plugin:" + plugin + " id:" + nodeId);
                logger.trace("params: " + gdb.getNodeParams(nodeId).toString());
                if(plugin != null) {
                    String nodeAgentId = gdb.getNodeId(region,agent,null);
                    logger.trace("region: " + region + " agent:" + agent + " plugin:" + " null id:" + nodeAgentId);
                    logger.trace("params: " + gdb.getNodeParams(nodeAgentId).toString());
                }
            }
            else {
                logger.debug("Adding Node: " + de.getParams().toString());
                gdb.addNode(region, agent,plugin);
                gdb.setNodeParams(region,agent,plugin, de.getParams());
                gdb.setNodeParam(region,agent,plugin, "watchdog_ts", String.valueOf(System.currentTimeMillis()));
                wasAdded = true;
            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : addNode ERROR : " + ex.toString());
        }
        return wasAdded;
    }

    public Boolean watchDogUpdate(MsgEvent de) {
        Boolean wasUpdated = false;

        try {

            String region = de.getParam("src_region");
            String agent = de.getParam("src_agent");
            String pluginId = de.getParam("src_plugin");

            String nodeId = gdb.getNodeId(region,agent,pluginId);

            if(nodeId != null) {
                logger.debug("Updating WatchDog Node: " + de.getParams().toString());
                //update watchdog_ts for local db
                gdb.setNodeParam(region,agent,pluginId, "watchdog_ts", String.valueOf(System.currentTimeMillis()));
                gdb.setNodeParam(region,agent,pluginId, "is_active", Boolean.TRUE.toString());

                String interval = de.getParam("watchdogtimer");
                if(interval == null) {
                    interval = "5000";
                }
                gdb.setNodeParam(region, agent, pluginId, "watchdogtimer", interval);

                wasUpdated = true;
            }
            else {

                //This must be fixed
                //logger.error("watchDogUpdate : Can't update missing node : " + de.getParams().toString());
                //Needs to be fixed in Cresco Library for watchdog discovery message modes (start,run,stop)
                if(gdb.getNodeId(region,agent,null) != null) {
                    //this is a plugin for which there is currently no discovery, add it for now
                    //send a note to the agent to send information on this plugin to the region controller

                    //for now we will request a plugin inventory from the hosting agent when we experence a new plugin.
                    MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG,plugin.getRegion(),null,null,"enabled");
                    le.setParam("src_region", plugin.getRegion());
                    le.setParam("dst_region", region);
                    le.setParam("dst_agent", agent);
                    //le.setParam("dst_plugin", pluginId);
                    le.setParam("configtype","plugininventory");
                    le.setParam("plugin",pluginId);
                    //le.setParam("is_active", Boolean.TRUE.toString());
                    this.plugin.msgIn(le);
                    /*
                    addNode(de);
                    nodeId = gdb.getNodeId(region,agent,plugin);
                    gdb.setNodeParam(region,agent,plugin, "watchdog_ts", String.valueOf(System.currentTimeMillis()));
                    String interval = de.getParam("watchdogtimer");
                    if(interval == null) {
                        interval = "5000";
                    }
                    gdb.setNodeParam(region, agent, plugin, "watchdogtimer", interval);
                    wasUpdated = true;
                    */
                }
            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : watchDogUpdate ERROR : " + ex.toString());
        }
        return wasUpdated;
    }

    public Boolean updateKPI(MsgEvent ce) {
        boolean updatedKPI = false;
        try {
            String region = null;
            String agent = null;
            String plugin = null;
            String resource_id = null;
            String inode_id = null;

            region = ce.getParam("src_region");
            agent = ce.getParam("src_agent");
            plugin = ce.getParam("src_plugin");
            resource_id = ce.getParam("resource_id");
            inode_id = ce.getParam("inode_id");

            //clean params for edge
				/*
				ce.removeParam("loop");
				ce.removeParam("isGlobal");
				ce.removeParam("src_agent");
				ce.removeParam("src_region");
				ce.removeParam("src_plugin");
				ce.removeParam("dst_agent");
				ce.removeParam("dst_region");
				ce.removeParam("dst_plugin");
				*/
            Map<String, String> params = ce.getParams();

            updatedKPI = dba.updateKPI(region, agent, plugin, resource_id, inode_id, params);

        }
        catch( Exception ex) {
            logger.error("updateKPI :" + ex.getMessage());
        }
        return updatedKPI;
    }


    public Boolean removeNode(MsgEvent de) {
        Boolean wasRemoved = false;

        try {

            String region = de.getParam("src_region");
            String agent = de.getParam("src_agent");
            String plugin = de.getParam("src_plugin");

            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId != null) {
                logger.debug("Removing Node: " + de.getParams().toString());
                gdb.removeNode(region, agent,plugin);
                wasRemoved = true;
            }
            else {
                logger.error("region: " + region + " agent:" + agent + " plugin:" + plugin + " does not exist!");
            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : removeNode ERROR : " + ex.toString());
        }
        return wasRemoved;
    }

    public Boolean removeNode(String region, String agent, String plugin) {
        Boolean wasRemoved = false;

        try {

            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId != null) {
                logger.debug("Removing Node: " + "region: " + region + " agent:" + agent + " plugin:" + plugin);
                gdb.removeNode(region, agent,plugin);
                wasRemoved = true;
            }
            else {
                logger.error("region: " + region + " agent:" + agent + " plugin:" + plugin + " does not exist!");
            }

        } catch (Exception ex) {
            logger.error("GraphDBUpdater : removeNode ERROR : " + ex.toString());
        }
        return wasRemoved;
    }

}
