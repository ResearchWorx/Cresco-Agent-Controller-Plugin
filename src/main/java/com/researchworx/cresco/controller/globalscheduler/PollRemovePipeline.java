package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.app.gNode;
import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.List;

public class PollRemovePipeline implements Runnable {

	private Launcher plugin;
	private CLogger logger;
    private List<gNode> pipelineNodes;
    private String pipelineId;
    private gPayload gpay;


	public PollRemovePipeline(Launcher plugin, String pipelineId)
	{
		this.logger = new CLogger(PollRemovePipeline.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		//this.assignedNodes = assignedNodes;
		this.pipelineId = pipelineId;
	}
	 public void run() {
	        try {

	            logger.error("PollRemovePipeline : run() 0");

	            int pipelineStatus = plugin.getGDB().dba.getPipelineStatusCode(pipelineId);

                logger.error("PollRemovePipeline : run() 1");

                //logger.error("PIPELINE ID " + pipelineId + " SCHEDILER FOR REMOVAL!!! STATUS " + pipelineStatus);

                //if((pipelineStatus >= 10) && (pipelineStatus < 19)) {
                if((pipelineStatus >= 10) && (pipelineStatus < 19)) {
                    logger.error("PollRemovePipeline : run() 2");

                    plugin.getGDB().dba.setPipelineStatus(pipelineId, "9", "Pipeline Scheduled for Removal");
                    logger.error("PollRemovePipeline : run() 2.1");

					gpay = plugin.getGDB().dba.getPipelineObj(pipelineId);
                    //logger.error("pluginsid : " + pipelineId + " status_code " + plugin.getGDB().dba.getPipelineStatus(pipelineId) + " pipleinId payload:" + gpay.pipeline_id);
                    logger.error("PollRemovePipeline : run() 3");

                    if (pipelineId.equals(gpay.pipeline_id)) {
                        logger.error("PollRemovePipeline : run() 4");

						pipelineNodes = new ArrayList<>(gpay.nodes);
						for (gNode gnode : pipelineNodes) {
                            logger.error("PollRemovePipeline : run() 5");

						    int statusCode = plugin.getGDB().dba.getINodeStatus(gnode.node_id);
						    if((statusCode >= 10) && (statusCode < 19))  { //running somewhere
                                logger.error("PollRemovePipeline : run() 6");

                                MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");
                                me.setParam("globalcmd", "removeplugin");
                                me.setParam("inode_id", gnode.node_id);
                                me.setParam("resource_id", pipelineId);
                                //ghw.resourceScheduleQueue.add(me);
                                plugin.getGDB().dba.setINodeParam(gnode.node_id,"status_code","9");
                                plugin.getGDB().dba.setINodeParam(gnode.node_id,"status_desc","iNode Pipeline Scheduled for Removal");
                                logger.error("PollRemovePipeline : plugin.getResourceScheduleQueue().add(me);");

                                plugin.getResourceScheduleQueue().add(me);
                            }
                            else if(statusCode > 19) {
                                plugin.getGDB().dba.setINodeParam(gnode.node_id,"status_code","8");
                                plugin.getGDB().dba.setINodeParam(gnode.node_id,"status_desc","iNode Disabled");
                            }
						}
                    //start watch loop
                        logger.error("PollRemovePipeline : Start Listen loop");
                        List<gNode> errorList = new ArrayList<>();
                        boolean isScheduling = true;
                        while(isScheduling)
                        {
                            List<gNode> checkList = new ArrayList<>(pipelineNodes);

                            if(checkList.isEmpty()) {
                                isScheduling = false;
                            }

                            for(gNode gnode : checkList) {
                                int statusCode = plugin.getGDB().dba.getINodeStatus(gnode.node_id);
                                if (statusCode != 9) {
                                    if(statusCode == 8) {
                                        logger.debug("PollRemovePipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id + " status_code :" + plugin.getGDB().dba.getINodeParam(gnode.node_id, "status_code"));
                                        pipelineNodes.remove(gnode);
                                    }
                                    if(statusCode > 19) {
                                        errorList.add(gnode);
                                        pipelineNodes.remove(gnode);
                                        logger.error("PollRemovePipeline thread " + Thread.currentThread().getId() + " : " + gnode.node_id + " status_code :" + plugin.getGDB().dba.getINodeParam(gnode.node_id, "status_code"));

                                    }
                                }
                            }

                            Thread.sleep(500);

                        }
                        //end watch loop
                        if(errorList.isEmpty()) {
                            plugin.getGDB().dba.removePipeline(pipelineId);
                            logger.debug("pipelineid " + pipelineId + " removed!");
                        } else {
                            plugin.getGDB().dba.setPipelineStatus(pipelineId, "80", "Pipeline Failed Removal");
                            logger.error("PipelineID: " + pipelineId + " Removal Failed!");
                        }
                    }
				}

	        }
		   catch(Exception ex)
		   {
             logger.error("PollAddPipeline : " + ex.getMessage());
               plugin.getGDB().dba.setPipelineStatus(pipelineId, "90", "Pipeline Failed Removal");
               logger.error("PipelineID: " + pipelineId + " Removal Schedule Failed!");
	       }
	    }


}
