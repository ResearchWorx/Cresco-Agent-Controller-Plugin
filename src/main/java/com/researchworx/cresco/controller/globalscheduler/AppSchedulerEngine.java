package com.researchworx.cresco.controller.globalscheduler;

import com.researchworx.cresco.controller.app.gNode;
import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.*;


public class AppSchedulerEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
    private GlobalHealthWatcher ghw;
    public FuturaEngine fe;
    public GuilderEngine ge;
    public OptimaEngine oe;


    public AppSchedulerEngine(Launcher plugin, GlobalHealthWatcher ghw) {
		this.logger = new CLogger(AppSchedulerEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
		this.plugin = plugin;
		this.ghw = ghw;
        oe = new OptimaEngine(this);
        fe = new FuturaEngine(plugin,this);
        ge = new GuilderEngine(plugin,this);
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
                        //check the pipeline node

                        gPayload createdPipeline = plugin.getGDB().dba.createPipelineNodes(gpay);
                        if(createdPipeline.status_code.equals("3"))
                        {
                            logger.debug("Created Pipeline Records: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);
                            //start creating real objects

                            if(checkPipeline(gpay.pipeline_id))
                            {
                                plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline resources scheduled and active.");
                            }
                            else
                            {
                                plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"1","Failed to schedule pipeline resources.");
                            }

                        }
                        else
                        {
                            logger.debug("Pipeline Creation Failed: " + gpay.pipeline_name + " id=" + gpay.pipeline_id);

                        }
                        //process pipeline stuff
                    }
                    else
                    {
                        Thread.sleep(1000);
                    }
                }
                catch(Exception ex)
                {
                    logger.debug("ResourceSchedulerEngine gPayloadQueue Error: " + ex.toString());
                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("ResourceSchedulerEngine Error: " + ex.toString());
        }
    }

    public boolean checkPipeline(String pipeline_id) {

        gPayload gpay = plugin.getGDB().dba.getPipelineObj(pipeline_id);
        logger.debug("checkPipeline started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

        List<String> badINodes = new ArrayList<String>();
        logger.debug("Checking Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
        for(gNode node : gpay.nodes)
        {
            String vNode_id = node.node_id;
            String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
            logger.debug("Checking vNode_id:" + vNode_id + " iNode_id:" + iNode_id);

            //plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
            plugin.getGDB().gdb.addINodeResource(gpay.pipeline_id,iNode_id);
            //addINodeResource


            //Check resources

            /*
            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
            //me.setParam("src_region", "external");
            //me.setParam("src_agent", "external");
            //me.setParam("dst_region", "external");
            //me.setParam("dst_agent", "external");
            me.setParam("globalcmd", "addplugin");
            me.setParam("inode_id", iNode_id);
            me.setParam("resource_id", gpay.pipeline_id);
            me.setParam("configparams", plugin.getGDB().dba.getINodeParams(iNode_id));

            //schedule resource
            ghw.resourceScheduleQueue.offer(me);
            */


            /*
            //check if node is active
            if(Launcher.resource_list.containsKey(iNode_id))
            {
                if(!Launcher.resource_list.get(iNode_id))
                {
                    badINodes.add(iNode_id);
                    plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
                }
            }
            else
            {
                badINodes.add(iNode_id);
                plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
            }
            */
        }


        Map<String,List<gNode>> schedulemaps = buildNodeMaps(gpay);

        List<gNode> assignedNodes = schedulemaps.get("assigned");
        List<gNode> unAssignedNodes = schedulemaps.get("unassigned");
        List<gNode> badNodes = schedulemaps.get("error");

        System.out.println("Assigned Nodes : " + assignedNodes.size());
        System.out.println("unAssigned Nodes : " + unAssignedNodes.size());
        System.out.println("badNodes : " + badNodes.size());

        if (badNodes.size() != 0) {
            System.out.println("Bad Node assignments... dead dead deadsky!");
        } else if (unAssignedNodes.size() != 0) {
            System.out.println("We need to find some resources for these request.");

            Map<String,List<gNode>> scheduleAssignments = oe.scheduleAssignment(unAssignedNodes);

            if(scheduleAssignments.get("unassigned").size() != 0) {
                //nodify guilder to get resources
                double workloadResources = 0;
                for(gNode gnode : scheduleAssignments.get("noresources")) {
                    workloadResources += gnode.workloadUtil;
                }
                ge.addResourceProvider(workloadResources);
            }
            else if(scheduleAssignments.get("assigned").size() == unAssignedNodes.size()) {
                //rebuild payload
                gpay.nodes.clear();
                gpay.nodes.addAll(scheduleAssignments.get("assigned"));
                Map<String,List<gNode>> schedulemapsOpt = buildNodeMaps(gpay);

                List<gNode> assignedNodesOpt = schedulemapsOpt.get("assigned");
                List<gNode> unAssignedNodesOpt = schedulemapsOpt.get("unassigned");
                List<gNode> badNodesOpt = schedulemapsOpt.get("error");

                System.out.println("Assigned Nodes : " + assignedNodesOpt.size());
                System.out.println("unAssigned Nodes : " + unAssignedNodesOpt.size());
                System.out.println("badNodes : " + badNodesOpt.size());

                if((unAssignedNodesOpt.size() == 0) && (badNodesOpt.size() == 0)) {
                    System.out.println("WOOT");
                }

                ////gpay.nodes.addAll(assignedNodes);

            }


        } else if ((unAssignedNodes.size() == 0) && (badNodes.size() == 0)) {
            System.out.println("Woot! Schedule This thing!");
        } else {
            System.out.println("WTF are you doing here?");
        }

        /*
        //Rebuild nodes
        for(String iNode_id : badINodes)
        {
            logger.debug("iNode_id: " + iNode_id + " must be rescheduled.");
            if(!createResource(iNode_id))
            {
                plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"1","Pipeline check Failed.");
                return false;
            }

        }
        */
        plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline was activated.");

        return true;
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

    private Map<String,List<gNode>> buildNodeMaps(gPayload gpay) {

        Map<String,List<gNode>> nodeResults = null;
        try {

            nodeResults = new HashMap<>();

            List<gNode> assignedNodes = new ArrayList<>();
            List<gNode> unAssignedNodes = new ArrayList<>(gpay.nodes);
            List<gNode> errorNodes = new ArrayList<>();


            //verify predicates
            for (gNode node : gpay.nodes) {

                if (node.params.containsKey("location_region") && node.params.containsKey("location_agent")) {
                    if (nodeExist(node.params.get("location_region"), node.params.get("location_agent"))) {
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    } else {
                        errorNodes.add(node);
                    }
                } else if (node.params.containsKey("location")) {
                    if (locationExist(node.params.get("location"))) {
                        unAssignedNodes.remove(node);
                        assignedNodes.add(node);
                    } else {
                        errorNodes.add(node);
                    }
                }
            }
            nodeResults.put("assigned",assignedNodes);
            nodeResults.put("unassigned", unAssignedNodes);
            nodeResults.put("error",errorNodes);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return nodeResults;
    }


    public boolean checkPipeline2(String pipeline_id) {

        gPayload gpay = plugin.getGDB().dba.getPipelineObj(pipeline_id);
        logger.debug("checkPipeline started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

        List<String> badINodes = new ArrayList<String>();
        logger.debug("Checking Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
        for(gNode node : gpay.nodes)
        {
            String vNode_id = node.node_id;
            String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
            logger.debug("Checking vNode_id:" + vNode_id + " iNode_id:" + iNode_id);

            //plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
            plugin.getGDB().gdb.addINodeResource(gpay.pipeline_id,iNode_id);
            //addINodeResource

            //Check resources

            /*
            MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
            //me.setParam("src_region", "external");
            //me.setParam("src_agent", "external");
            //me.setParam("dst_region", "external");
            //me.setParam("dst_agent", "external");
            me.setParam("globalcmd", "addplugin");
            me.setParam("inode_id", iNode_id);
            me.setParam("resource_id", gpay.pipeline_id);
            me.setParam("configparams", plugin.getGDB().dba.getINodeParams(iNode_id));

            //schedule resource
            ghw.resourceScheduleQueue.offer(me);
            */


            /*
            //check if node is active
            if(Launcher.resource_list.containsKey(iNode_id))
            {
                if(!Launcher.resource_list.get(iNode_id))
                {
                    badINodes.add(iNode_id);
                    plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
                }
            }
            else
            {
                badINodes.add(iNode_id);
                plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
            }
            */
        }
        /*
        //Rebuild nodes
        for(String iNode_id : badINodes)
        {
            logger.debug("iNode_id: " + iNode_id + " must be rescheduled.");
            if(!createResource(iNode_id))
            {
                plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"1","Pipeline check Failed.");
                return false;
            }

        }
        */
        plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline was activated.");

        return true;
    }


    /*
    public boolean disablePipeline(String pipeline_id) {
		gPayload gpay = plugin.getGDB().dba.getPipelineObj(pipeline_id);
			logger.debug("Disable started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
			for(gNode node : gpay.nodes)
			{
				String vNode_id = node.node_id;
				String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
				logger.debug("Disable started for vNode_id:" + vNode_id + " iNode_id:" + iNode_id);

				//check if node is active
				if(Launcher.resource_list.containsKey(iNode_id)) {
					if(Launcher.resource_list.get(iNode_id))
					{
						Launcher.resource_list.put(iNode_id, false);
						plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Node was disabled.");
					}
				}
				else
				{
					
					plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Node was disabled.");
				}
				
			}
			plugin.getGDB().dba.setPipelineStatus(pipeline_id, "0", "Pipeline was disabled.");
			
			
		return true;
    }

	public boolean checkPipeline(String pipeline_id) {

		gPayload gpay = plugin.getGDB().dba.getPipelineObj(pipeline_id);
		logger.debug("Disable started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

    		List<String> badINodes = new ArrayList<String>();
			logger.debug("Checking Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
			for(gNode node : gpay.nodes)
			{
				String vNode_id = node.node_id;
				String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
				logger.debug("Checking vNode_id:" + vNode_id + " iNode_id:" + iNode_id);
				//check if node is active
				if(Launcher.resource_list.containsKey(iNode_id))
				{
					if(!Launcher.resource_list.get(iNode_id))
					{
						badINodes.add(iNode_id);
						plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
					}
				}
				else
				{
					badINodes.add(iNode_id);
					plugin.getGDB().dba.setINodeStatus(iNode_id, "0", "Status Reset on Startup");
				}

			}
			//Rebuild nodes
			for(String iNode_id : badINodes)
			{
				logger.debug("iNode_id: " + iNode_id + " must be rescheduled.");
				if(!createResource(iNode_id))
				{
					plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"1","Pipeline check Failed.");
					return false;
				}

			}
			plugin.getGDB().dba.setPipelineStatus(gpay.pipeline_id,"4","Pipeline was activated.");

			return true;
    }

    private void checkPipelines() {
    	List<gPayload> glist =  plugin.getGDB().dba.getPipelineList();
		for(gPayload gpay : glist)
		{
			checkPipeline(gpay.pipeline_id);
		}
    }

	private  boolean vNodeIsActive(String vNode_id) {
		//check node status
		//get iNode
		String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
		logger.debug("createResource: vNode:" + vNode_id);
		logger.debug("createResource: iNode:" + iNode_id);
		if(plugin.getGDB().dba.iNodeIsActive(iNode_id))
		{
			return true;
		}
		
		return false;
	}

	private boolean createResource(String iNode_id) {
		boolean isCreated = true;
		try
		{
			//iNode might now be running, check to make sure 
			if(Launcher.resource_list.containsKey(iNode_id))
			{
				if(Launcher.resource_list.get(iNode_id))
				{
					logger.debug("createResource pre-check: iNode_id: " + iNode_id + " is already scheduled.");
					return true;
				}
			}
			
			Stack<String> nodeStack = new Stack<String>();
			
			Map<String,String> manifest = null;
					 
		
			String nextNodeId = iNode_id;
			
			boolean isRoot = false;
			while(!isRoot)
			{
				manifest = plugin.getGDB().dba.getNodeManifest(nextNodeId);
				if(manifest != null)
				{
					logger.debug("Manifest Found for iNode: " + nextNodeId);
					nodeStack.push(nextNodeId);
					isRoot = true;
				}
				else
				{
					logger.debug("Unable to create manifest for iNode: " + nextNodeId + " push..");
					nodeStack.push(nextNodeId);
					nextNodeId = plugin.getGDB().dba.getUpstreamNode(nextNodeId);
					if(nextNodeId == null)
					{
						logger.debug("getNodeManifest: Error: null nextNodeID before AMQP Node");
						return false;
					}
					
				}
				
			}
			
			while(!nodeStack.isEmpty())
			{
				//createFromManifest()
				String popNode = nodeStack.pop();
				logger.debug("Unstacking manifest for iNode: " + popNode + " pop..");
				
				//iNode might now be running, check to make sure 
				if(Launcher.resource_list.containsKey(iNode_id))
				{
					if(!Launcher.resource_list.get(iNode_id))
					{
						logger.debug("createResource pre-create: iNode_id: " + iNode_id + " is not in the resource list - scheduling...");
						
						manifest = plugin.getGDB().dba.getNodeManifest(popNode);
						createFromManifest(manifest);
					}
				}
				else
				{
					logger.debug("createResource pre-create: iNode_id: " + iNode_id + " is marked as inactive - scheduling...");
					
					manifest = plugin.getGDB().dba.getNodeManifest(popNode);
					createFromManifest(manifest);
				}
			}
			
			
			
			
			
		}
		catch(Exception ex)
		{
			logger.debug("createResources: Error resource_id:" + iNode_id);	
			return false;
		}
		
		return isCreated;
	}

	private boolean createFromManifest(Map<String,String> manifest) {
		try
		{
			logger.debug("createFromManifest: " + manifest.get("node_type") + " " + manifest.get("amqp_server") + " " + manifest.get("amqp_login") + " " + manifest.get("amqp_password"));
			
			for (Map.Entry<String, String> entry : manifest.entrySet())
			{
				logger.debug(entry.getKey() + "/" + entry.getValue());
			}
			String iNode_id = manifest.get("node_id");
			
			Map<String,String> newparams = new HashMap<String,String>();
			newparams.put("amqp_server", manifest.get("amqp_server"));
			newparams.put("amqp_login", manifest.get("amqp_login"));
			newparams.put("amqp_password", manifest.get("amqp_password"));
			newparams.put("amqp_server", manifest.get("amqp_server"));
			
			//setINodeStatus(String iNode_id, String status_code, String status_desc)
			//addINodeParams(String iNode_id, Map<String,String> newparams)
			String node_type = manifest.get("node_type");
			logger.debug("Node Type: " + node_type);
			//AMQP
			if(node_type.equals("amqp"))
			{
				new Thread(new Amqp_check(iNode_id,manifest.get("amqp_server"),manifest.get("amqp_login"),manifest.get("amqp_password"),manifest.get("outExchange"))).start();
				while(!Launcher.resource_list.containsKey(iNode_id))
				{
					Thread.sleep(1000);
				}
				logger.debug("AMQP: resource_id:" + iNode_id + " Waiting...");
				
				while(!Launcher.resource_list.get(iNode_id))
				{
					Thread.sleep(1000);
				}
				plugin.getGDB().dba.addINodeParams(iNode_id, newparams);
				
			}
			//MEM
			else if(node_type.equals("membuffer"))
			{
				new Thread(new Membuffer(iNode_id,manifest.get("amqp_server"),manifest.get("amqp_login"),manifest.get("amqp_password"),manifest.get("outExchange"))).start();
				
				while(!Launcher.resource_list.containsKey(iNode_id))
				{
					Thread.sleep(1000);
				}
				logger.debug("Membuff: resource_id:" + iNode_id + " Waiting...");
				while(!Launcher.resource_list.get(iNode_id))
				{
					Thread.sleep(1000);
				}
				plugin.getGDB().dba.addINodeParams(iNode_id, newparams);
				
			}
			
			else if(node_type.equals("query"))
			{
				String outExchange = UUID.randomUUID().toString();
				    
				new Thread(new QueryNode(iNode_id,manifest.get("amqp_server"),manifest.get("amqp_login"),manifest.get("amqp_password"),manifest.get("outExchange"),outExchange,manifest.get("query_string"))).start();
				
				while(!Launcher.resource_list.containsKey(iNode_id))
				{
					Thread.sleep(1000);
				}
				logger.debug("QueryNode: resource_id:" + iNode_id + " Waiting...");
				while(!Launcher.resource_list.get(iNode_id))
				{
					Thread.sleep(1000);
				}
				newparams.put("inExchange", manifest.get("outExchange"));
				newparams.put("outExchange", outExchange);
				plugin.getGDB().dba.addINodeParams(iNode_id, newparams);
				
			}
			
			String statusString = "Resource Scheduled and Active.";
			logger.debug("iNode_id: " + iNode_id + " node_type: " + node_type + " is Active.");
			logger.debug("Setting iNode: " + iNode_id + " status=4");
			plugin.getGDB().dba.setINodeStatus(iNode_id, "4", statusString);
		}
		catch(Exception ex)
		{
			logger.debug("createFromManifest: Error: " + ex.toString());	
		}
		return false;
	}
    */
}
