package com.researchworx.cresco.controller.globalscheduler;

import com.researchworx.cresco.controller.app.gNode;
import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class AppSchedulerEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
    private GlobalHealthWatcher ghw;
    private ExecutorService addPipelineExecutor;

    public AppSchedulerEngine(Launcher plugin, GlobalHealthWatcher ghw) {
		this.logger = new CLogger(AppSchedulerEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
		this.plugin = plugin;
		this.ghw = ghw;
        addPipelineExecutor = Executors.newFixedThreadPool(1);
	}

    public void run() {
        try
        {

            ghw.AppSchedulerActive = true;
            while (ghw.AppSchedulerActive)
            {
                try
                {
                    gPayload gpay = plugin.getAppScheduleQueue().poll();

                    if(gpay != null)
                    {
                        logger.debug("gPayload offered");

                        gPayload createdPipeline = plugin.getGDB().dba.createPipelineNodes(gpay);

                        if(createdPipeline.status_code.equals("3")) {
                            logger.debug("Created Pipeline Records: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);
                            //start creating real objects

                            int pipelineStatus = schedulePipeline(gpay.pipeline_id);

                            switch (pipelineStatus) {
                                //all metrics
                                case 1: plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"40","Failed to schedule pipeline resources.");
                                    break;
                                case 2: logger.error("Learn to schedule resources!");
                                case 4: plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline resources scheduled.");
                                    break;

                                default:
                                    logger.error("Pipeline Scheduling Failed: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);
                                    break;
                            }

                        }
                        else
                        {
                            logger.error("Pipeline Creation Failed: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);

                        }

                    }
                    else
                    {
                        Thread.sleep(1000);
                    }
                }
                catch(Exception ex)
                {
                    logger.error("AppSchedulerEngine gPayloadQueue Error: " + ex.toString());
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    ex.printStackTrace(pw);
                    logger.error(sw.toString()); // stack trace as a string
                }
            }
        }
        catch(Exception ex)
        {
            logger.error("AppSchedulerEngine Error: " + ex.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString()); // stack trace as a string
        }
    }

    public int schedulePipeline(String pipeline_id) {
        int scheduleStatus = 1;
        try {
            gPayload gpay = plugin.getGDB().dba.getPipelineObj(pipeline_id);
            logger.debug("checkPipeline started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

            List<String> badINodes = new ArrayList<String>();
            logger.debug("Checking Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
            for (gNode node : gpay.nodes) {
                //String vNode_id = node.node_id;
                //logger.info("Checking vNode_id:" + vNode_id);
                //String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
                String iNode_id = node.node_id;

                logger.debug("Checking iNode_id:" + iNode_id);
                plugin.getGDB().dba.addINodeResource(gpay.pipeline_id, iNode_id);
                /*
                if (iNode_id != null) {
                    plugin.getGDB().gdb.addINodeResource(gpay.pipeline_id, iNode_id);
                } else {
                    logger.error("iNode is null for vNode " + vNode_id);
                    return 1;
                }
                */

            }


            Map<String, List<gNode>> schedulemaps = buildNodeMaps(gpay.nodes);
            printScheduleStats(schedulemaps);

            if (schedulemaps.get("error").size() != 0) {
                System.out.println("Error Node assignments... dead dead deadsky!");
            } else if (schedulemaps.get("unassigned").size() != 0) {
                System.out.println("Unassigned Node assignments... dead dead deadsky!");
            }

            if ((schedulemaps.get("assigned").size() != 0) && (schedulemaps.get("unassigned").size() == 0) && (schedulemaps.get("error").size() == 0) && (schedulemaps.get("noresource").size() == 0)) {
                logger.debug("Scheduling is ready!");

                logger.debug("Submitting Resource Pipeline for Scheduling " + gpay.pipeline_id);
                addPipelineExecutor.execute(new PollAddPipeline(plugin,schedulemaps.get("assigned"), gpay.pipeline_id));
                logger.debug("Submitted Resource Pipeline for Scheduling");

                /*
                for(gNode gnode : schedulemaps.get("assigned")) {
                    logger.debug("gnode_id : " + gnode.node_id + " params : " + gnode.params);
                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
                    me.setParam("globalcmd", "addplugin");
                    me.setParam("inode_id", gnode.node_id);
                    me.setParam("resource_id", gpay.pipeline_id);
                    me.setParam("location_region",gnode.params.get("location_region"));
                    me.setParam("location_agent",gnode.params.get("location_agent"));
                    gnode.params.remove("location_region");
                    gnode.params.remove("location_agent");

                    StringBuilder configparms = new StringBuilder();
                    for (Map.Entry<String, String> entry : gnode.params.entrySet())
                    {
                        configparms.append(entry.getKey() + "=" + entry.getValue() + ",");
                        //System.out.println(entry.getKey() + "/" + entry.getValue());
                    }
                    if(configparms.length() > 0) {
                        configparms.deleteCharAt(configparms.length() -1);
                    }
                    me.setParam("configparams", configparms.toString());
                    logger.debug("Message [" + me.getParams().toString() + "]");
                    plugin.getResourceScheduleQueue().offer(me);
                }
                */

                return 4;
            } else {
                logger.error("SOMETHING IS BAD WRONG WITH SCHEDULING!");
            }


        }
        catch (Exception ex) {
            logger.error("schedulePipeline " + ex.getMessage());
        }
        return scheduleStatus;
    }



    public boolean nodeExist(String region, String agent) {
        boolean nodeExist = false;
        try {
            String nodeId = plugin.getGDB().gdb.getNodeId(region,agent,null);
            if(nodeId != null) {
                nodeExist = true;
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return nodeExist;
    }

    public boolean locationExist(String location) {
        boolean isLocation = false;
        //String getINodeId(String resource_id, String inode_id)
        List<String> aNodeLocations = plugin.getGDB().gdb.getANodeFromIndex("location",location);
        if(aNodeLocations.size() > 0) {
            isLocation = true;
        }
        return isLocation;
    }

    public void printScheduleStats(Map<String,List<gNode>> schedulemaps) {
        logger.info("Assigned Nodes : " + schedulemaps.get("assigned").size());
        logger.info("Unassigned Nodes : " + schedulemaps.get("unassigned").size());
        logger.info("Noresource Nodes : " + schedulemaps.get("noresource").size());
        logger.info("Error Nodes : " + schedulemaps.get("error").size());
    }

    private Map<String,List<gNode>> buildNodeMaps(List<gNode> nodes) {

        Map<String,List<gNode>> nodeResults = null;
        try {

            nodeResults = new HashMap<>();

            List<gNode> assignedNodes = new ArrayList<>();
            List<gNode> errorNodes = new ArrayList<>();
            List<gNode> unAssignedNodes = new ArrayList<>(nodes);

            //verify predicates
            for (gNode node : nodes) {
                logger.trace("node_id=" + node.node_id + " node_name=" + node.node_name + " type" + node.type + " params" + node.params.toString());
                if (node.params.containsKey("location_region") && node.params.containsKey("location_agent")) {
                    if (nodeExist(node.params.get("location_region"), node.params.get("location_agent"))) {
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    } else {
                        errorNodes.add(node);
                    }
                } else if(node.params.containsKey("location")) {
                    String nodeId = getLocationNodeId(node.params.get("location"));
                    if(nodeId != null) {
                        Map<String,String> nodeMap = plugin.getGDB().gdb.getNodeParams(nodeId);
                        node.params.put("location_region",nodeMap.get("region"));
                        node.params.put("location_agent",nodeMap.get("agent"));
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    }
                }
            }
            nodeResults.put("assigned",assignedNodes);
            nodeResults.put("unassigned", unAssignedNodes);
            nodeResults.put("error",errorNodes);
            nodeResults.put("noresource", new ArrayList<gNode>());

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeResults;
    }

    private String getLocationNodeId(String location) {
        String locationNodeId = null;
        try {
            List<String> aNodeList = plugin.getGDB().gdb.getANodeFromIndex("location", location);
            if(!aNodeList.isEmpty()) {
                locationNodeId = aNodeList.get(0);
            }
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return locationNodeId;
    }

}
