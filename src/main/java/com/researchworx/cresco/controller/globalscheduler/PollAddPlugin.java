package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;

public class PollAddPlugin implements Runnable {

	private String resource_id  = null;
	private String inode_id = null;
	private String region = null;
	private String agent = null;
	private Launcher plugin;
	
	public PollAddPlugin(Launcher plugin, String resource_id, String inode_id, String region, String agent)
	{
		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.region = region;
		this.agent = agent;
		
	}
	 public void run() {
	        try 
	        {
	        	int count = 0;
	        	String edge_id = null;
	        	while((edge_id == null) && (count < 30))
	        	{
	        		edge_id = plugin.getGDB().getResourceEdgeId(resource_id, inode_id, region, agent);
	        		Thread.sleep(1000);
	        	}
	        	if(edge_id != null)
	        	{
	        		if((plugin.getGDB().setINodeParam(resource_id,inode_id,"status_code","10")) &&
							(plugin.getGDB().setINodeParam(resource_id,inode_id,"status_desc","iNode Active.")))
					{
							//recorded plugin activations
	        				System.out.println("SchedulerEngine : pollAddPlugin : Activated inode_id=" + inode_id);
					}
	        	}
	        	else
	        	{
	        		System.out.println("SchedulerEngine : pollAddPlugin : unable to verify iNode activation!");
	        	}
	        }
		   catch(Exception v) 
		   {
	            System.out.println(v);
	       }
	    }  
}
