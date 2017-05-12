package com.researchworx.cresco.controller.globalscheduler;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalhttp.RESTRequestFilter;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by vcbumg2 on 1/19/17.
 */
public class GuilderEngine {

    private AppSchedulerEngine appSchedulerEngine;
    private Launcher plugin;
    private CLogger logger;

    public GuilderEngine(Launcher plugin, AppSchedulerEngine appSchedulerEngine) {
        this.logger = new CLogger(GuilderEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
        this.plugin = plugin;
        this.appSchedulerEngine = appSchedulerEngine;

    }

    public double getResourceCost(String region, String agent) {
        double resourceCost = -1;
        try {
            resourceCost = 1;
        } catch (Exception ex) {

        }
        return resourceCost;
    }
//stuff
    public ResourceProvider getResourceProvider(String region, String agent) {
        ResourceProvider rp = null;
        try {
            List<ResourceProvider> resourceProviders = getResourceProviders();
            for(ResourceProvider crp : resourceProviders) {
                if((crp.getRegion().equals(region)) && crp.getAgent().equals(agent)) {
                    rp = crp;
                }
            }
        } catch (Exception ex) {
            logger.error("getResourceProvider " + ex.getMessage());
        }
        return rp;
    }

    public List<ResourceProvider> getResourceProviders() {
        return getResourceProviders(null);
    }
    public List<ResourceProvider> getResourceProviders(String location) {

        List<ResourceProvider> resourceProviders = null;

        try {
            resourceProviders = new ArrayList<>();
            List<String> resourceLocation = null;
            if(location != null) {
                resourceLocation = new ArrayList<>();
                for (String aNodeId : plugin.getGDB().gdb.getANodeFromIndex("location", location)) {
                    Map<String,String> nodeparams = plugin.getGDB().gdb.getNodeParams(aNodeId);
                    resourceLocation.add(nodeparams.get("region") + "_" + nodeparams.get("agent"));
                }
            }
            List<String> sysInfoEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");
            for(String edgeID : sysInfoEdgeList) {

                String region = plugin.getGDB().dba.getIsAssignedParam(edgeID,"region");
                String agent = plugin.getGDB().dba.getIsAssignedParam(edgeID,"agent");

                boolean addResource = true;

                String nodePath = region + "_" + agent;

                if(location != null) {
                    if(!resourceLocation.contains(nodePath)) {
                        addResource = false;
                    }
                }
                //make sure old resources are not assigned.
                if(!plugin.isReachableAgent(nodePath)) {
                    addResource = false;
                }


                if(addResource) {
                    Map<String, String> edgeParams = plugin.getGDB().dba.getIsAssignedParams(edgeID);
                    long sysUptime = Long.parseLong(edgeParams.get("sys-uptime"));
                    double cpuIdle = Double.parseDouble(edgeParams.get("cpu-idle-load"));
                    int cpuLogicalCount = Integer.parseInt(edgeParams.get("cpu-logical-count"));
                    int cpuCompositeBenchmark = Integer.parseInt(edgeParams.get("benchmark_cpu_composite"));
                    long memAvailable = Long.parseLong(edgeParams.get("memory-available"));
                    long memLimit = Long.parseLong(edgeParams.get("memory-total"));
                    resourceProviders.add(new ResourceProvider(region, agent, sysUptime, cpuIdle, cpuLogicalCount, cpuCompositeBenchmark, memAvailable, memLimit));
                }
            }

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return resourceProviders;

    }

    private void printResources() {


        List<String> sysInfoEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("sysinfo_resource", "sysinfo_inode");
        for(String edgeID : sysInfoEdgeList) {
            logger.info("ID = " + edgeID);
            //logger.info(gdb.getIsAssignedParam(String edge_id,String param_name)
            String region = plugin.getGDB().dba.getIsAssignedParam(edgeID,"region");
            String agent = plugin.getGDB().dba.getIsAssignedParam(edgeID,"agent");
            String pluginID = plugin.getGDB().dba.getIsAssignedParam(edgeID,"plugin");

            Map<String,String> edgeParams = plugin.getGDB().dba.getIsAssignedParams(edgeID);
            for (Map.Entry<String, String> entry : edgeParams.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                logger.info("key=" + key + " value=" + value);
            }

            logger.info(region + " " + agent + " " + pluginID);
        }

    }

    private ResourceProvider genResourceProvider() {
        Map<String,String> sm = null;
        ResourceProvider rp = null;
        try {

            Random rand = new Random();
            //int random_integer = rand.nextInt(upperbound-lowerbound) + lowerbound;
            double cpuIdle = ThreadLocalRandom.current().nextDouble(0,99.9);
            int cpuCount = ThreadLocalRandom.current().nextInt(1,80);

            //rp = new ResourceProvider(UUID.randomUUID().toString(),1000,cpuIdle,cpuCount,1000, 1000, 2000);
            /*
            sm = new HashMap<>();
            sm.put("benchmark_cpu_composite","1000");
            sm.put("cpu-logical-count","2");
            sm.put("cpu-idle-load","71.7");
            */
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return rp;
    }

}
