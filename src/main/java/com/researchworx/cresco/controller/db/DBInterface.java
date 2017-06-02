package com.researchworx.cresco.controller.db;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.app.gNode;
import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import javax.xml.bind.DatatypeConverter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBInterface {

    private Launcher plugin;
    private CLogger logger;
    private DBEngine gde;
    public DBBaseFunctions gdb;
    public DBApplicationFunctions dba;
    private Gson gson;


    public DBInterface(Launcher plugin) {
        this.logger = new CLogger(DBInterface.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.plugin = plugin;
        this.gde = new DBEngine(plugin);
        this.gdb = new DBBaseFunctions(plugin,gde);
        this.dba = new DBApplicationFunctions(plugin,gde);
        this.gson = new Gson();
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

    public String getRegionList() {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();
            List<String> regionList = gdb.getNodeList(null,null,null);
            //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
            //Map<String,String> rMap = new HashMap<String,String>();
            if(regionList != null) {
                for (String region : regionList) {
                    Map<String,String> regionMap = new HashMap<>();
                    logger.trace("Region : " + region);
                    List<String> agentList = gdb.getNodeList(region, null, null);
                    regionMap.put("name",region);
                    regionMap.put("agents",String.valueOf(agentList.size()));
                    regionArray.add(regionMap);
                }
            }
            queryMap.put("regions",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));

        }
        catch(Exception ex)
        {
            logger.error("getRegionList() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public String getAgentList(String actionRegion) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> regionList;
            if(actionRegion != null) {
                regionList = new ArrayList<>();
                regionList.add(actionRegion);
            } else {
                regionList = gdb.getNodeList(null,null,null);
            }
            for(String region : regionList) {

                List<String> agentList = gdb.getNodeList(region, null, null);
                //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
                //Map<String,String> rMap = new HashMap<String,String>();
                if (agentList != null) {
                    for (String agent : agentList) {
                        Map<String, String> regionMap = new HashMap<>();
                        logger.trace("Agent : " + region);
                        List<String> pluginList = gdb.getNodeList(region, agent, null);
                        regionMap.put("name", agent);
                        regionMap.put("region", region);
                        regionMap.put("plugins", String.valueOf(pluginList.size()));
                        regionArray.add(regionMap);
                    }
                }
                queryMap.put("agents", regionArray);
            }
            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));

        }
        catch(Exception ex)
        {
            logger.error("getAgentList() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public String getPluginList(String actionRegion, String actionAgent) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> regionList;
            if(actionRegion != null) {
                regionList = new ArrayList<>();
                regionList.add(actionRegion);
            } else {
                regionList = gdb.getNodeList(null,null,null);
            }
            for(String region : regionList) {
                List<String> agentList;
                if(actionAgent != null) {
                    agentList = new ArrayList<>();
                    agentList.add(actionAgent);
                } else {
                    agentList = gdb.getNodeList(region,null,null);
                }
                //Map<String,Map<String,String>> ahm = new HashMap<String,Map<String,String>>();
                //Map<String,String> rMap = new HashMap<String,String>();
                if (agentList != null) {
                    for (String agent : agentList) {
                        logger.trace("Agent : " + region);
                        List<String> pluginList = gdb.getNodeList(region, agent, null);
                        for(String plugin : pluginList) {
                            Map<String, String> regionMap = new HashMap<>();
                            regionMap.put("name", plugin);
                            regionMap.put("region", region);
                            regionMap.put("agent", agent);
                            regionArray.add(regionMap);
                        }
                    }
                }
                queryMap.put("plugins", regionArray);
            }
            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));

        }
        catch(Exception ex)
        {
            logger.error("getAgentList() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    public String getPluginInfo(String actionRegion, String actionAgent, String actionPlugin) {
        String queryReturn = null;

        try
        {
            String nodeId = gdb.getNodeId(actionRegion, actionAgent,actionPlugin);
            Map<String,String> nodeParams = gdb.getNodeParams(nodeId);
            queryReturn = nodeParams.get("config");

        }
        catch(Exception ex)
        {
            logger.error("getPluginInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;
    }

    private String getGlobalResourceInfo() {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;


        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

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
            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("globalresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));

} catch(Exception ex) {
            logger.error("getGlobalResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    private String getRegionResourceInfo(String actionRegion) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;


        long cpu_core_count = 0;
        long memoryAvailable = 0;
        long memoryTotal = 0;
        long diskAvailable = 0;
        long diskTotal = 0;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> sysInfoEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");

            for(String edgeID : sysInfoEdgeList) {
                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);

                if(edgeParams.get("region").toLowerCase().equals(actionRegion.toLowerCase())) {
                    cpu_core_count += Long.parseLong(edgeParams.get("cpu-logical-count"));
                    memoryAvailable += Long.parseLong(edgeParams.get("memory-available"));
                    memoryTotal += Long.parseLong(edgeParams.get("memory-total"));
                    for (String fspair : edgeParams.get("fs-map").split(",")) {
                        String[] fskey = fspair.split(":");
                        diskAvailable += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-available"));
                        diskTotal += Long.parseLong(edgeParams.get("fs-" + fskey[0] + "-total"));
                    }
                }
            }
            Map<String,String> resourceTotal = new HashMap<>();
            resourceTotal.put("cpu_core_count",String.valueOf(cpu_core_count));
            resourceTotal.put("mem_available",String.valueOf(memoryAvailable));
            resourceTotal.put("mem_total",String.valueOf(memoryTotal));
            resourceTotal.put("disk_available",String.valueOf(diskAvailable));
            resourceTotal.put("disk_total",String.valueOf(diskTotal));
            regionArray.add(resourceTotal);
            queryMap.put("regionresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));

        } catch(Exception ex) {
            logger.error("getRegionResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    private String getAgentResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;

        Map<String,List<Map<String,String>>> queryMap;

        try
        {
            queryMap = new HashMap<>();
            List<Map<String,String>> regionArray = new ArrayList<>();

            List<String> sysInfoEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");

            for(String edgeID : sysInfoEdgeList) {
                Map<String, String> edgeParams = dba.getIsAssignedParams(edgeID);

                if((edgeParams.get("region").toLowerCase().equals(actionRegion.toLowerCase())) && (edgeParams.get("agent").toLowerCase().equals(actionAgent.toLowerCase()))) {

                    Map<String,String> resourceTotal = new HashMap<>(edgeParams);
                    resourceTotal.remove("dst_region");
                    resourceTotal.remove("src_plugin");
                    resourceTotal.remove("src_region");
                    resourceTotal.remove("src_agent");
                    resourceTotal.remove("plugin");
                    resourceTotal.remove("region");
                    resourceTotal.remove("agent");
                    resourceTotal.remove("inode_id");
                    resourceTotal.remove("resource_id");
                    resourceTotal.remove("routepath");

                    regionArray.add(resourceTotal);

                }
            }

            queryMap.put("agentresourceinfo",regionArray);

            queryReturn = DatatypeConverter.printBase64Binary(gdb.stringCompress((gson.toJson(queryMap))));

        } catch(Exception ex) {
            logger.error("getAgentResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public String getResourceInfo(String actionRegion, String actionAgent) {
        String queryReturn = null;
        try
        {
            if((actionRegion != null) && (actionAgent != null)) {
                queryReturn = getAgentResourceInfo(actionRegion,actionAgent);
            } else if (actionRegion != null) {
                queryReturn = getRegionResourceInfo(actionRegion);
            } else {
                queryReturn = getGlobalResourceInfo();
            }

        } catch(Exception ex) {
            logger.error("getResourceInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

    }

    public String getGPipeline(String actionPipelineId) {
        String queryReturn = null;
        try
        {
            gPayload gpay = plugin.getGDB().dba.getPipelineObj(actionPipelineId);
            int nodeSize = gpay.nodes.size();
            for(int i=0; i < nodeSize; i++) {
                gpay.nodes.get(i).params.clear();

            }
            String returnGetGpipeline = gson.toJson(gpay);
            //String returnGetGpipeline = plugin.getGDB().dba.getPipeline(actionPipelineId);
            queryReturn = DatatypeConverter.printBase64Binary(plugin.getGDB().gdb.stringCompress(returnGetGpipeline));

        } catch(Exception ex) {
            logger.error("getGPipeline() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return queryReturn;

    }

    public String getGPipelineExport(String actionPipelineId) {
        String queryReturn = null;
        try
        {
            String returnGetGpipeline = plugin.getGDB().dba.getPipeline(actionPipelineId);
            queryReturn = DatatypeConverter.printBase64Binary(plugin.getGDB().gdb.stringCompress(returnGetGpipeline));

        } catch(Exception ex) {
            logger.error("getGPipelineExport() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return queryReturn;

    }


    public String getPipelineInfo(String pipeline_action) {
        String queryReturn = null;
        try
        {
            Map<String,List<Map<String,String>>> queryMap;

                queryMap = new HashMap<>();
                List<Map<String,String>> pipelineArray = new ArrayList<>();
                List<String> pipelines = null;
                if(pipeline_action != null) {
                    pipelines = new ArrayList<>();
                    pipelines.add(pipeline_action);
                } else {
                    pipelines = plugin.getGDB().dba.getPipelineIdList();
                }

                for(String pipelineId :pipelines) {
                    Map<String,String> pipelineMap = plugin.getGDB().dba.getPipelineStatus(pipelineId);
                    if(!pipelineMap.isEmpty()) {
                        pipelineArray.add(pipelineMap);
                    }
                }
                queryMap.put("pipelines",pipelineArray);

                queryReturn = DatatypeConverter.printBase64Binary(plugin.getGDB().gdb.stringCompress((gson.toJson(queryMap))));

            } catch(Exception ex) {
            logger.error("getPipelineInfo() " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); //
        }

        return queryReturn;

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
                long watchdog_rate = 5000L;

                if(params.containsKey("watchdogtimer")) {
                    watchdog_rate = Long.parseLong(params.get("watchdogtimer"));

                }
                if((params.containsKey("watchdog_ts"))  && (params.containsKey("is_active"))) {
                    long watchdog_ts = Long.parseLong(params.get("watchdog_ts"));
                    //long watchdog_rate = Long.parseLong(params.get("watchdogtimer"));
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
            de.setParam("is_active",Boolean.TRUE.toString());

            String nodeId = gdb.getNodeId(region,agent,plugin);
            if(nodeId != null) {
                logger.trace("region: " + region + " agent:" + agent + " plugin:" + plugin + " id:" + nodeId);
                logger.trace("params: " + gdb.getNodeParams(nodeId).toString());
                if(plugin != null) {
                    String nodeAgentId = gdb.getNodeId(region,agent,null);
                    logger.trace("region: " + region + " agent:" + agent + " plugin:" + " null id:" + nodeAgentId);
                    logger.trace("params: " + gdb.getNodeParams(nodeAgentId).toString());
                    gdb.setNodeParams(region,agent,plugin, de.getParams());

                } else {
                    gdb.setNodeParams(region,agent,null, de.getParams());
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
                logger.error("nodeID does not exist for region:" + region + " agent:" + agent + " pluginId:" + pluginId);
                //logger.error(de.getMsgType().toString() + " " + de.getParams().toString());
                /*
                if(gdb.getNodeId(region,agent,null) != null) {

                    MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG,plugin.getRegion(),null,null,"enabled");
                    le.setMsgBody("Get Plugin Inventory From Agent");
                    le.setParam("src_region", plugin.getRegion());
                    le.setParam("dst_region", region);
                    le.setParam("dst_agent", agent);
                    le.setParam("configtype","plugininventory");
                    le.setParam("plugin",pluginId);
                    this.plugin.msgIn(le);
                }
                */

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
