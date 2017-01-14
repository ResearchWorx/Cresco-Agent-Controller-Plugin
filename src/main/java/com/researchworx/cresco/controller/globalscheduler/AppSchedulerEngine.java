package com.researchworx.cresco.controller.globalscheduler;

import app.gNode;
import app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.*;


public class AppSchedulerEngine implements Runnable {

	private Launcher plugin;
	private CLogger logger;
    private GlobalHealthWatcher ghw;

	public AppSchedulerEngine(Launcher plugin, GlobalHealthWatcher ghw) {
		this.logger = new CLogger(AppSchedulerEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
		this.plugin = plugin;
		this.ghw = ghw;
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
        logger.debug("Disable started for Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);

        List<String> badINodes = new ArrayList<String>();
        logger.debug("Checking Pipeline_id:" + gpay.pipeline_id + " Pipeline_name:" + gpay.pipeline_name);
        for(gNode node : gpay.nodes)
        {
            String vNode_id = node.node_id;
            String iNode_id = plugin.getGDB().dba.getINodefromVNode(vNode_id);
            logger.debug("Checking vNode_id:" + vNode_id + " iNode_id:" + iNode_id);
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
