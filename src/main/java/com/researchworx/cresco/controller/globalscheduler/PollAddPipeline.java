package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.app.gNode;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PollAddPipeline implements Runnable {

	private Launcher plugin;
	private CLogger logger;
    List<gNode> pipelineNodes;
    String pipelineId;

	public PollAddPipeline(Launcher plugin, List<gNode> assignedNodes, String pipelineId)
	{
		this.logger = new CLogger(PollAddPipeline.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
		this.plugin = plugin;
		//this.assignedNodes = assignedNodes;
		this.pipelineId = pipelineId;
        pipelineNodes = new ArrayList<>(assignedNodes);
	}
	 public void run() {
	        try 
	        {
                logger.debug("PipelineId " + pipelineId + " Pipeline Starting");
                for(gNode gnode : pipelineNodes) {

                    logger.debug("gnode_id : " + gnode.node_id + " params : " + gnode.params);
                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
                    me.setParam("globalcmd", "addplugin");
                    me.setParam("inode_id", gnode.node_id);
                    me.setParam("resource_id", pipelineId);
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
                    plugin.getGDB().dba.setINodeParam(gnode.node_id,"status_code","4");
                    plugin.getGDB().dba.setINodeParam(gnode.node_id,"status_desc","iNode resources scheduled.");

                    plugin.getResourceScheduleQueue().add(me);

                }


                List<gNode> errorList = new ArrayList<>();
                boolean isScheduling = true;
                while(isScheduling)
	        	{
                    List<gNode> checkList = new ArrayList<>(pipelineNodes);

                    if(checkList.isEmpty()) {
	        	        isScheduling = false;
                    }

                    for(gNode gnode : checkList) {
                        int statusCode = Integer.parseInt(plugin.getGDB().dba.getINodeParam(gnode.node_id, "status_code"));
                        if (statusCode != 4) {
                            logger.debug("PollAddPipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id + " status_code :" + plugin.getGDB().dba.getINodeParam(gnode.node_id, "status_code"));
                            pipelineNodes.remove(gnode);
                            if(statusCode == 40) {
                                errorList.add(gnode);
                            }
                        }
                    }

                    Thread.sleep(500);

	        	}
                if(errorList.isEmpty()) {
                    plugin.getGDB().dba.setPipelineStatus(pipelineId, "10", "Pipeline Active");
                    logger.debug("PipelineId " + pipelineId + " Pipeline Active");
                } else {
                    plugin.getGDB().dba.setPipelineStatus(pipelineId, "50", "Pipeline Failed Resource Assignment");
                    logger.error("PipelineId " + pipelineId + " Failed Resource Assignment");
                }

	        }
		   catch(Exception ex)
		   {

               logger.error("PollAddPipeline : " + ex.getMessage());
	            //logger.error("PollAddPipeline : " + plugin.getStringFromError(ex));
	       }
	    }


}
