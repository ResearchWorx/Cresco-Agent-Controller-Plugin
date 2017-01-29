package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

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

        	String edge_id = plugin.getGDB().gdb.getResourceEdgeId(resource_id, inode_id);
    		if(edge_id != null)
    		{
    			String pnode_node_id = plugin.getGDB().gdb.getIsAssignedParam(edge_id, "out");
    			if(pnode_node_id != null)
				{
    				pnode_node_id = pnode_node_id.substring(pnode_node_id.indexOf("[") + 1, pnode_node_id.indexOf("]"));
    				
    				String region = plugin.getGDB().gdb.getIsAssignedParam(edge_id, "region");
    				String agent = plugin.getGDB().gdb.getIsAssignedParam(edge_id, "agent");
    				String pluginId = plugin.getGDB().gdb.getIsAssignedParam(edge_id, "plugin");

    				logger.debug("r: " + region + " a:" + agent + " p:" + pluginId);

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
    				
    				
				}
    			
    		}
    		else
    		{
    			logger.error("Edge_id=null");
    		}
    		logger.debug("Removing iNode: " + inode_id);
			plugin.getGDB().gdb.removeINode(resource_id,inode_id);
			
			//remove resource_id if this is the last resource
			List<String> inodes = plugin.getGDB().gdb.getresourceNodeList(resource_id,null);
			if(inodes == null)
			{
				plugin.getGDB().gdb.removeResourceNode(resource_id);
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
