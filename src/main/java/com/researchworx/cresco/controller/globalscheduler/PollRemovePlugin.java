package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;


public class PollRemovePlugin implements Runnable { 

	private String resource_id  = null;
	private String inode_id = null;
	private Launcher plugin;
	private GlobalHealthWatcher ghw;
	private CLogger logger;

	public PollRemovePlugin(Launcher plugin, String resource_id, String inode_id)
	{
		logger = new CLogger(PollRemovePlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);

		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
	}

	public void run() {
        try {

            if(plugin.getGDB().dba.getINodeStatus(inode_id) > 8) {
            String edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id, inode_id);
            if (edge_id != null) {
                String pnode_node_id = plugin.getGDB().dba.getIsAssignedParam(edge_id, "out");
                if (pnode_node_id != null) {
                    pnode_node_id = pnode_node_id.substring(pnode_node_id.indexOf("[") + 1, pnode_node_id.indexOf("]"));

                    String region = plugin.getGDB().dba.getIsAssignedParam(edge_id, "region");
                    String agent = plugin.getGDB().dba.getIsAssignedParam(edge_id, "agent");
                    String pluginId = plugin.getGDB().dba.getIsAssignedParam(edge_id, "plugin");

                    logger.debug("starting to remove r: " + region + " a:" + agent + " p:" + pluginId);

                    String pnode_node_id_match = plugin.getGDB().gdb.getNodeId(region, agent, pluginId);

                    if (pnode_node_id.equals(pnode_node_id_match)) {
                        MsgEvent me = removePlugin(region, agent, pluginId);
                        plugin.msgIn(me);

                        int count = 0;
                        boolean isRemoved = false;
                        while ((!isRemoved) && (count < 60)) {
                            if (plugin.getGDB().gdb.getNodeId(region, agent, pluginId) == null) {
                                isRemoved = true;
                            }
                            Thread.sleep(1000);
                            count++;
                        }
                        if (isRemoved) {
                            plugin.getGDB().dba.setINodeParam(inode_id, "status_code", "8");
                            plugin.getGDB().dba.setINodeParam(inode_id,"status_desc","iNode Disabled");
                            logger.debug("removed r: " + region + " a:" + agent + " p:" + pluginId + " for inode:" + inode_id);
                        } else {
                            plugin.getGDB().dba.setINodeParam(inode_id, "status_code", "90");
                            plugin.getGDB().dba.setINodeParam(inode_id,"status_desc","iNode unable to verify iNode deactivation!");
                            logger.debug("pollRemovePlugin : unable to verify iNode deactivation! " + plugin.getGDB().dba.getINodeStatus(inode_id));

                        }

                    } else {
                        String errorString = "pnode_node_id mismatch : pnode_node_id " + pnode_node_id + " != " + pnode_node_id_match;
                        logger.error(errorString);
                        plugin.getGDB().dba.setINodeParam(inode_id, "status_code", "91");
                        plugin.getGDB().dba.setINodeParam(inode_id,"status_desc",errorString);
                    }

                } else {
                    plugin.getGDB().dba.setINodeParam(inode_id, "status_code", "92");
                    logger.error("plugin not found for resource_id: " + resource_id + " inode_id:" + inode_id + " setting inode status");
                }

                }
            }
        }
	   catch(Exception ex)
	   {
            logger.error("PollRemovePlugin : " + ex.getMessage());
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