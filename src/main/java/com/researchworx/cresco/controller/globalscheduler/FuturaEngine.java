package com.researchworx.cresco.controller.globalscheduler;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by vcbumg2 on 1/19/17.
 */
public class FuturaEngine {

    private AppSchedulerEngine appSchedulerEngine;
    private Launcher plugin;
    private CLogger logger;

    public FuturaEngine(Launcher plugin, AppSchedulerEngine appSchedulerEngine) {
        this.logger = new CLogger(FuturaEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
        this.plugin = plugin;
        this.appSchedulerEngine = appSchedulerEngine;
    }

    public Map<Integer,List<String>> getClusters(int clusterK, int clusterType) {
        Map<Integer,List<String>> clusterMap = null;

        //clusterK
        //The number of clusters to generate

        //clusterTypes
        //0 = all ResourceMetrics
        //1 = Network ResourceMetrics
        //2 = CPU ResourceMetrics
        //3 = Mem ResourceMetrics
        //4 = Disk ResourceMetrics

        try {
            clusterMap = new HashMap<>();
            List<ResourceMetric> metrics = getResourceMetrics();
            List<ResourceMetricWrapper> clusterInput = new ArrayList<>(metrics.size());
            for (ResourceMetric rm : metrics) {
                clusterInput.add(new ResourceMetricWrapper(rm,clusterType));
            }

            // initialize a new clustering algorithm.
            // we use KMeans++ with 10 clusters and 10000 iterations maximum.
            // we did not specify a distance measure; the default (euclidean distance) is used.
            KMeansPlusPlusClusterer<ResourceMetricWrapper> clusterer = new KMeansPlusPlusClusterer<ResourceMetricWrapper>(clusterK, 10000);
            List<CentroidCluster<ResourceMetricWrapper>> clusterResults = clusterer.cluster(clusterInput);

            // output the clusters
            for (int i = 0; i < clusterResults.size(); i++) {

                clusterMap.put(i,new ArrayList<String>());

                for (ResourceMetricWrapper metricWrapper : clusterResults.get(i).getPoints()) {
                    clusterMap.get(i).add(metricWrapper.getResourceMetric().getRegion() + metricWrapper.getResourceMetric().getRegion());
                }
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return clusterMap;
    }


/*
    public ResourceMetric getResourceMetric(String pluginname) {
        ResourceMetric rm = null;
        try {
            Map<String, ResourceMetric> containerMetrics = getContainerMetricMap();
            if(containerMetrics.containsKey(pluginname)) {
                rm = containerMetrics.get(pluginname);
            }
            else {
                rm = new ResourceMetric(500);

            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return rm;
    }
    */
    /*
        public Map<String,ResourceMetric> getContainerMetricMap() {
            Map<String,ResourceMetric> containerMetricMap = null;
            try {
                containerMetricMap = new HashMap<>();
                for(ResourceMetric rm : getResourceMetrics()) {
                    if(containerMetricMap.containsKey(rm.getINodeId())) {
                        ResourceMetric rmq = containerMetricMap.get(rm.getINodeId());
                        rmq.addCpuAve(rm.getCpuAve());
                        rmq.addMemory(rm.getMemAve());
                        rmq.addDiskRead(rm.getDiskRead());
                        rmq.addDiskWrite(rm.getDiskWrite());
                        rmq.addNetworkRx(rm.getNetworkRx());
                        rmq.addNetworkTx(rm.getNetworkTx());
                    }
                    else {
                        containerMetricMap.put(rm.getINodeId(),rm);
                    }
                }

            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
            return containerMetricMap;
        }
    */

    private double workloadCalcUtil(ResourceMetric rm, ResourceProvider rp) {
        double workloadCost = -1;
        try{
            long cpuCompositeBenchmark = rp.getCpuCompositeBenchmark();
            long cpuLogicalCount = rp.getCpuLogicalCount();
            long compositeCpuCapacity = cpuCompositeBenchmark * cpuLogicalCount;
            double cpuAve = rm.getCpuAve();
            workloadCost = (double)compositeCpuCapacity * (cpuAve/100);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return workloadCost;
    }

    private ResourceMetric genResourceMetric() {
        ResourceMetric rm = null;
        try {
            String INodeId = UUID.randomUUID().toString();
            Random rand = new Random();
            //int random_integer = rand.nextInt(upperbound-lowerbound) + lowerbound;
            long runTime = ThreadLocalRandom.current().nextLong(0, 1000000);
            double cpuAve = ThreadLocalRandom.current().nextDouble(0,99.9);
            long memLimit = ThreadLocalRandom.current().nextLong(0, 1000000);
            long memMax = ThreadLocalRandom.current().nextLong(0, memLimit);
            long memAve = ThreadLocalRandom.current().nextLong(0, memMax);
            long memCurrent = ThreadLocalRandom.current().nextLong(0, memMax);
            long diskReadTotal = ThreadLocalRandom.current().nextLong(0, 1000000);
            long diskWriteTotal = ThreadLocalRandom.current().nextLong(0, 1000000);
            long networkRxTotal = ThreadLocalRandom.current().nextLong(0, 1000000);
            long networkTxTotal = ThreadLocalRandom.current().nextLong(0, 1000000);

            //public ResourceMetric(String INodeId, long runTime, long cpuTotal,
            // long memCurrent, long memAve, long memLimit, long memMax,
            // long diskReadTotal, long diskWriteTotal, long networkRxTotal,
            // long networkTxTotal) {
            rm = new ResourceMetric(runTime,cpuAve,memCurrent,memAve,memLimit,memMax,diskReadTotal,diskWriteTotal,networkRxTotal,networkTxTotal);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return rm;
    }

    public ResourceMetric getResourceMetricAve(String containerImage) {

        ResourceMetric resourceMetric = null;

        try {

            if(containerImage != null) {
                List<String> containerEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("container_resource", "container_inode");
                for (String edgeID : containerEdgeList) {

                    String region = plugin.getGDB().dba.getIsAssignedParam(edgeID, "region");
                    String agent = plugin.getGDB().dba.getIsAssignedParam(edgeID, "agent");
                    String pluginId = plugin.getGDB().dba.getIsAssignedParam(edgeID, "plugin");

                    logger.debug("LOOKING FOR RESOURCE PROVIDER ON Region: " + region + " Agent: " + agent);

                    ResourceProvider rp = appSchedulerEngine.ge.getResourceProvider(region,agent);

                    //only calculate metrics if record exist for provider
                    if(rp != null) {
                        /*
                        Map<String, String> edgeParams = plugin.getGDB().gdb.getIsAssignedParams(edgeID);
                        for (Map.Entry<String, String> entry : edgeParams.entrySet()) {
                            String key = entry.getKey();
                            String value = entry.getValue();
                            logger.info("key=" + key + " value=" + value);
                        }
                        */
                        logger.debug("LOOKING FOR CONTAINER IMAGE = " + containerImage);
                        String canidateContainerImage = plugin.getGDB().dba.getIsAssignedParam(edgeID, "container_image");

                        if (containerImage.equals(canidateContainerImage)) {
                            String resourceMetricJSON = plugin.getGDB().dba.getIsAssignedParam(edgeID, "resource_metric");
                            Gson gson = new Gson();
                            ResourceMetric rm = gson.fromJson(resourceMetricJSON, ResourceMetric.class);
                            if (resourceMetric == null) {
                                resourceMetric = rm;
                            } else {
                                resourceMetric.addCpuAve(rm.getCpuAve());
                                resourceMetric.addMemory(rm.getMemAve());
                                resourceMetric.addDiskRead(rm.getDiskRead());
                                resourceMetric.addDiskWrite(rm.getDiskWrite());
                                resourceMetric.addNetworkRx(rm.getNetworkRx());
                                resourceMetric.addNetworkTx(rm.getNetworkTx());
                            }
                            logger.debug("pre workload utilization: " + resourceMetric.getWorkloadUtil());
                            logger.debug("rp.getCpuCompositeBenchmark() : " + rp.getCpuCompositeBenchmark());
                            logger.debug("rp.getCpuLogicalCount() : " + rp.getCpuLogicalCount());
                            logger.debug("rm.getCpuAve() : " + rm.getCpuAve());
                            resourceMetric.addWorkloadCost( (rp.getCpuCompositeBenchmark() * rp.getCpuLogicalCount()) * (rm.getCpuAve()/100));
                            logger.debug("post workload utilization: " + resourceMetric.getWorkloadUtil());
                        }
                    }
                }
            }
            if (resourceMetric == null) {
                resourceMetric = new ResourceMetric(200);
                logger.debug("NO RESOURCE METRIC FOUND USING DEFAULT");
            }

        }
        catch(Exception ex) {
            logger.error("getResourceMetricAve " + ex.getMessage());
        }
        return resourceMetric;

    }

    public List<ResourceMetric> getResourceMetrics() {

        List<ResourceMetric> resourceProviders = null;
        try {
            resourceProviders = new ArrayList<>();

            List<String> containerEdgeList = plugin.getGDB().dba.getIsAssignedEdgeIds("container_resource", "container_inode");
            for(String edgeID : containerEdgeList) {

                String region = plugin.getGDB().dba.getIsAssignedParam(edgeID,"region");
                String agent = plugin.getGDB().dba.getIsAssignedParam(edgeID,"agent");
                String resourceMetricJSON = plugin.getGDB().dba.getIsAssignedParam(edgeID,"resource_metric");
                Gson gson = new Gson();
                ResourceMetric rm = gson.fromJson(resourceMetricJSON,ResourceMetric.class);

            }

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return resourceProviders;

    }


}
