package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.messaging.MsgEvent;

import java.util.List;

public class PollRemovePlugin implements Runnable { 

	private String resource_id  = null;
	private String inode_id = null;
	private Launcher plugin;
	private GlobalHealthWatcher ghw;
	private GlobalCommandExec gexec;

	public PollRemovePlugin(Launcher plugin, String resource_id, String inode_id)
	{
		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.gexec = new GlobalCommandExec(plugin);
		
	}

	public void run() {
        try 
        {

        	String edge_id = plugin.getGDB().getResourceEdgeId(resource_id, inode_id);
    		if(edge_id != null)
    		{
    			String pnode_node_id = plugin.getGDB().getIsAssignedParam(edge_id, "out");
    			if(pnode_node_id != null)
				{
    				pnode_node_id = pnode_node_id.substring(pnode_node_id.indexOf("[") + 1, pnode_node_id.indexOf("]"));
    				
    				String region = plugin.getGDB().getIsAssignedParam(edge_id, "region");
    				String agent = plugin.getGDB().getIsAssignedParam(edge_id, "agent");
    				String pluginId = plugin.getGDB().getIsAssignedParam(edge_id, "plugin");
    				
    				String pnode_node_id_match = plugin.getGDB().getNodeId(region, agent, pluginId);

					if(pnode_node_id.equals(pnode_node_id_match))
    				{
    					//fire off remove command
        				MsgEvent me = removePlugin(region,agent,pluginId);
        				gexec.cmdExec(me);
        				//ControllerEngine.commandExec.cmdExec(me);
        				//loop until remove is completed
        				
    					int count = 0;
	        			boolean isRemoved = false;
	        			while((!isRemoved) && (count < 30))
	        			{
	        				if(plugin.getGDB().getNodeId(region, agent, pluginId) == null)
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
	        				System.out.println("Deactivated iNode: " + inode_id);
							
	        			}
	        			else
	        			{
	        				System.out.println("SchedulerEngine : pollRemovePlugin : unable to verify iNode deactivation!");
	        			}
	        			
    				}
    				
    				
				}
    			
    		}
    		else
    		{
    			System.out.println("Edge_id=null");
    		}
    		System.out.println("Removing iNode: " + inode_id);
			plugin.getGDB().removeINode(resource_id,inode_id);
			
			//remove resource_id if this is the last resource
			List<String> inodes = plugin.getGDB().getresourceNodeList(resource_id,null);
			if(inodes == null)
			{
				plugin.getGDB().removeResourceNode(resource_id);
			}
			
        	/*
        	if(edge_id != null)
        	{
        		if((ControllerEngine.gdb.setINodeParam(resource_id,inode_id,"status_code","10")) &&
						(ControllerEngine.gdb.setINodeParam(resource_id,inode_id,"status_desc","iNode Active.")))
				{
						//recorded plugin activations
        				System.out.println("SchedulerEngine : pollAddPlugin : Activated inode_id=" + inode_id);
				}
        	}
        	else
        	{
        		System.out.println("SchedulerEngine : pollAddPlugin : unable to verify iNode activation!");
        	}
        	*/
        }
	   catch(Exception v) 
	   {
            System.out.println(v);
       }
    }  

	public MsgEvent removePlugin(String region, String agent, String plugin)
	{
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"remove plugin");
		me.setParam("src_region", region);
		me.setParam("src_agent", "external");
		me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		me.setParam("controllercmd", "regioncmd");
		me.setParam("configtype", "pluginremove");
		me.setParam("plugin", plugin);
		return me;	
	}
	
}
