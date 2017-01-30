package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

public class PollAddPlugin implements Runnable {

	private String resource_id  = null;
	private String inode_id = null;
	private String region = null;
	private String agent = null;
	private Launcher plugin;
	private CLogger logger;
	private MsgEvent me;

	public PollAddPlugin(Launcher plugin, String resource_id, String inode_id, String region, String agent, MsgEvent me)
	{
		this.logger = new CLogger(PollAddPlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.region = region;
		this.agent = agent;
		this.me = me;
		
	}
	 public void run() {
	        try 
	        {
	        	int count = 0;
	        	String edge_id = null;

	        	plugin.msgIn(me);
	        	//logger.error("return message! " + re.getParams().toString());

	        	while((edge_id == null) && (count < 30))
	        	{
	        		edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id, inode_id, region, agent);
                    logger.debug("PollAddPlugin resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " agent " + agent + " edgeid " + edge_id);

                    Thread.sleep(1000);
	        		count++;
	        	}
	        	if(edge_id != null)
	        	{
	        		if((plugin.getGDB().dba.setINodeParam(resource_id,inode_id,"status_code","10")) &&
							(plugin.getGDB().dba.setINodeParam(resource_id,inode_id,"status_desc","iNode Active.")))
					{
							//recorded plugin activations
	        				logger.debug("ResourceSchedulerEngine : pollAddPlugin : Activated inode_id=" + inode_id);
					}
	        	}
	        	else
	        	{
	        		logger.debug("ResourceSchedulerEngine : pollAddPlugin : unable to verify iNode activation!");
	        	}
	        }
		   catch(Exception v) 
		   {
	            logger.error(v.getMessage());
	       }
	    }  
}
