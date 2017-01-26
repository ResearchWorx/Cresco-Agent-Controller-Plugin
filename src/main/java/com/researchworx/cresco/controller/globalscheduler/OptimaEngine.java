package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.app.gNode;

import java.util.*;

/**
 * Created by vcbumg2 on 1/19/17.
 */
public class OptimaEngine {

    AppSchedulerEngine appSchedulerEngine;

    public OptimaEngine(AppSchedulerEngine appSchedulerEngine) {
    this.appSchedulerEngine = appSchedulerEngine;

    }

    public Map<String,List<gNode>> scheduleAssignment(List<gNode> unAssignedNodes) {
        //base optimization on CPU, this must be expanded
        Map<String,List<gNode>> scheduleMap = null;
        List<gNode> assignedNodes = null;
        List<gNode> noResourceNodes = null;
        try {
            scheduleMap = new HashMap<>();
            assignedNodes = new ArrayList<>();
            noResourceNodes = new ArrayList<>();
            scheduleMap.put("assigned",assignedNodes);
            scheduleMap.put("unassigned",noResourceNodes);

            List<gNode> preAssignedNodes = new ArrayList<>();
            List<ResourceProvider> rps = appSchedulerEngine.ge.getResourceProviders();

            double minResourceUtil = -1;

            for(gNode gnode : unAssignedNodes) {
                String pluginname = gnode.params.get("pluginname");

                //Get past metric if it exist
                ResourceMetric rm = appSchedulerEngine.fe.getResourceMetricAve(pluginname);
                //add metric to gnode
                double resourceUtil = rm.getWorkloadUtil()/10;
                gnode.workloadUtil = resourceUtil;

                boolean foundResourceProvider = false;
                for(ResourceProvider rp : rps) {
                    double resourceAvalable = (rp.getCpuCompositeBenchmark() * rp.getCpuLogicalCount()) * (rp.getCpuIdle()/100);
                    if(gnode.workloadUtil <= resourceAvalable) {
                        foundResourceProvider = true;
                        if(minResourceUtil == -1) {
                            minResourceUtil = gnode.workloadUtil;
                        }
                        else {
                            if(minResourceUtil > gnode.workloadUtil) {
                                minResourceUtil = gnode.workloadUtil;
                            }
                        }
                    }
                }
                if(!foundResourceProvider) {
                    noResourceNodes.add(gnode);
                }
                else {
                    preAssignedNodes.add(gnode);
                }

            }

            if(noResourceNodes.size() == 0) {
                //no single resource too large to schedule, composite might still be

                List<Integer> providerCapacity = new ArrayList<>();
                for (ResourceProvider rp : rps) {
                    double resourceAvalable = (rp.getCpuCompositeBenchmark() * rp.getCpuLogicalCount()) * (rp.getCpuIdle() / 100);
                    providerCapacity.add((int) Math.round(resourceAvalable/minResourceUtil));
                }
                // number of warehouses
                int W = providerCapacity.size();
                // number of stores
                int S = unAssignedNodes.size();
                // capacity of each warehouse
                int[] K = convertIntegers(providerCapacity);

                int[][] P = new int[preAssignedNodes.size()][rps.size()];

                int bCost = -1;
                // stores first then warehouse
                for(gNode gnode : preAssignedNodes) {
                    //store row
                    double workloadUtil = gnode.workloadUtil;
                    for (ResourceProvider rp : rps) {
                        int resourceCost = (int)((rp.getCpuCompositeBenchmark() * rp.getCpuLogicalCount()) / appSchedulerEngine.ge.getResourceCost(rp.getRegion(), rp.getAgent()));
                        if(bCost == -1) {
                            bCost = resourceCost;
                        }
                        else {
                            if(bCost < resourceCost) {
                                bCost = resourceCost;
                            }
                        }
                        P[preAssignedNodes.indexOf(gnode)][rps.indexOf(rp)] = resourceCost;
                    }
                }

                ProviderOptimization po = new ProviderOptimization();
                Map<Integer, List<Integer>> opMap =  po.modelAndSolve(W,S,K,P,bCost);

                for (Map.Entry<Integer, List<Integer>> entry : opMap.entrySet())
                {
                    int providerIndex = entry.getKey();
                    //System.out.println("providerIndex: " + providerIndex);
                    //System.out.println("provider: " + rps.get(providerIndex).getINodeId());
                    for(int gnodeIndex : entry.getValue()) {
                        //System.out.println("workloadindex: " + gnodeIndex);
                        //System.out.println("workload: " + preAssignedNodes.get(gnodeIndex).node_name);
                        gNode gnode = preAssignedNodes.get(gnodeIndex);
                        gnode.params.put("location_region",rps.get(providerIndex).getRegion());
                        gnode.params.put("location_agent",rps.get(providerIndex).getAgent());
                        assignedNodes.add(gnode);
                    }
                    //System.out.println(entry.getKey() + "/" + entry.getValue());
                }


            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return scheduleMap;
    }

    public String getINodeIdRegion(String INodeId) {
        return "region-" + INodeId;
    }

    public String getINodeIdAgent(String INodeId) {
        return "agent-" + INodeId;
    }

    public  int[] convertIntegers(List<Integer> integers) {
        int[] ret = new int[integers.size()];
        Iterator<Integer> iterator = integers.iterator();

        for (int i = 0; i < ret.length; i++)
        {
            ret[i] = iterator.next().intValue();
        }
        return ret;
    }

    public List<gNode> getAssignments2(List<gNode> unAssignedNodes) {
        List<gNode> assignedNodes = null;
        try {

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return assignedNodes;
    }
}
