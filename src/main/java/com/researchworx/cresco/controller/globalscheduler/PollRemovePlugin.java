package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;

public class PollRemovePlugin implements Runnable { 

	private String resource_id  = null;
	private String inode_id = null;
	private Launcher plugin;
	private GlobalHealthWatcher ghw;
	private GlobalCommandExec gexec;
	private CLogger logger;

	public PollRemovePlugin(Launcher plugin, String resource_id, String inode_id)
	{
		logger = new CLogger(PollRemovePlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);

		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.gexec = new GlobalCommandExec(plugin);
		
	}

	public void run() {
        try 
        {

        	String edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id, inode_id);
    		if(edge_id != null)
    		{
    			String pnode_node_id = plugin.getGDB().dba.getIsAssignedParam(edge_id, "out");
    			if(pnode_node_id != null)
				{
    				pnode_node_id = pnode_node_id.substring(pnode_node_id.indexOf("[") + 1, pnode_node_id.indexOf("]"));
    				
    				String region = plugin.getGDB().dba.getIsAssignedParam(edge_id, "region");
    				String agent = plugin.getGDB().dba.getIsAssignedParam(edge_id, "agent");
    				String pluginId = plugin.getGDB().dba.getIsAssignedParam(edge_id, "plugin");

    				logger.debug("starting to remove r: " + region + " a:" + agent + " p:" + pluginId);

    				String pnode_node_id_match = plugin.getGDB().gdb.getNodeId(region, agent, pluginId);

					if(pnode_node_id.equals(pnode_node_id_match))
    				{
    					//fire off remove command
        				MsgEvent me = removePlugin(region,agent,pluginId);
        				//gexec.cmdExec(me);
                        plugin.msgIn(me);
        				//ControllerEngine.commandExec.cmdExec(me);
        				//loop until remove is completed
        				
    					int count = 0;
	        			boolean isRemoved = false;
	        			while((!isRemoved) && (count < 30))
	        			{
	        				if(plugin.getGDB().gdb.getNodeId(region, agent, pluginId) == null)
	        				{
	        					isRemoved = true;
                                logger.debug("removed r: " + region + " a:" + agent + " p:" + pluginId);

                            }
	        				else
	        				{
	        					Thread.sleep(1000);
	        				}
	        			}
	        			if(isRemoved)
	        			{
	        				logger.debug("Deactivated iNode: " + inode_id);

	        			}
	        			else
	        			{
	        				logger.debug("ResourceSchedulerEngine : pollRemovePlugin : unable to verify iNode deactivation!");
	        			}
	        			
    				}
    				else {
                        logger.error("pnode_node_id mismatch : pnode_node_id " + pnode_node_id + " != " + pnode_node_id_match);
                    }
    				
    				
				}
				else {
                    logger.error("pnode_node_id=null");
                }
    			
    		}
    		else
    		{
    			logger.error("Edge_id=null");
    		}
    		logger.debug("Removing iNode: " + inode_id);

            //remove enodes
            List<String> eNodeList = plugin.getGDB().dba.getNodeIdFromEdge("inode", "in", "enode_id", false, "inode_id",inode_id);
            eNodeList.addAll(plugin.getGDB().dba.getNodeIdFromEdge("inode", "out", "enode_id",true, "inode_id",inode_id));
            for(String eNodeId : eNodeList) {
                logger.debug("enodes remove" + eNodeId);
                plugin.getGDB().dba.removeNode(plugin.getGDB().dba.getENodeNodeId(eNodeId));
            }

            //remove vnode
            plugin.getGDB().dba.removeNode(plugin.getGDB().dba.getvNodefromINode(inode_id));

            //remove inode
            plugin.getGDB().dba.removeINode(resource_id,inode_id);

			//remove resource_id if this is the last resource
			List<String> inodes = plugin.getGDB().dba.getresourceNodeList(resource_id,null);
			logger.error("in pipeline " + resource_id + " there are " + inodes.size() + " plugins left");
			for(String str : inodes) {
			    logger.error("inode left " + str);
            }

			if(inodes == null)
			{
				plugin.getGDB().dba.removeResourceNode(resource_id);
			}

			
        	/*
        	if(edge_id != null)
        	{
        		if((ControllerEngine.gdb.setINodeParam(resource_id,inode_id,"status_code","10")) &&
						(ControllerEngine.gdb.setINodeParam(resource_id,inode_id,"status_desc","iNode Active.")))
				{
						//recorded plugin activations
        				System.out.println("ResourceSchedulerEngine : pollAddPlugin : Activated inode_id=" + inode_id);
				}
        	}
        	else
        	{
        		System.out.println("ResourceSchedulerEngine : pollAddPlugin : unable to verify iNode activation!");
        	}
        	*/
        }
	   catch(Exception v) 
	   {
            logger.error(v.getMessage());
       }
    }  

	public MsgEvent removePlugin(String region, String agent, String pluginId)
	{

		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"remove plugin");
		me.setParam("src_region", plugin.getRegion());
		me.setParam("src_agent", plugin.getAgent());
        me.setParam("src_plugin", plugin.getPluginID());
        me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		me.setParam("configtype", "pluginremove");
		me.setParam("plugin", pluginId);
		return me;	
	}
	
}
