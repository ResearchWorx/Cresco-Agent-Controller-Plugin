package com.researchworx.cresco.controller.regionalcontroller;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.messaging.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(AgentDiscovery.class);
    private Launcher plugin;
    private static RPC rpc;

    public AgentDiscovery(Launcher plugin) throws Exception {
        this.plugin = plugin;
        rpc = new RPC(plugin.getMsgOutQueue(), plugin.getRPCMap(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), null);
    }

    public void perfupdate(MsgEvent pe) {
        //send to controller if global WatchPerf
        //logger.debug("WatchPerf: " + pe.getParamsString());
        /*
       if(!PluginEngine.controllerChannel.updatePerf(pe))
	   {
			logger.debug("Controller : AgentDiscovery : Failed to updatePerf for WatchPerf on Controller");
	   }
	   */
    }

    public void discover(MsgEvent le) {
        try {

            String discoverString = le.getParam("src_region") + "-" + le.getParam("src_agent") + "-" + le.getParam("src_plugin");
            logger.debug("MsgType : " + le.getMsgType() + " Params: " + le.getParams());
            if (plugin.getDiscoveryMap().containsKey(discoverString)) {
                logger.info("Discovery underway for : discoverString=" + discoverString);
            } else {

                plugin.getDiscoveryMap().put(discoverString, System.currentTimeMillis());

                if ((le.getMsgType() == MsgEvent.Type.CONFIG) && (le.getMsgBody().equals("disabled"))) {
                    //if we see a agent enable command respond to it

                    logger.debug("Remove Node: " + le.getParams());
                    if (le.getParam("src_plugin") == null) //if plugin discover plugin info as well
                    {
                        plugin.getGDB().removeNode(le.getParam("src_region"), le.getParam("src_agent"), null);
                        logger.debug("AGENT REMOVED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    } else {
                        plugin.getGDB().removeNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"));
                        logger.debug("PLUGIN REMOVED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent") + " Plugin:" + le.getParam("src_plugin"));

                    }

                } else if ((le.getMsgType() == MsgEvent.Type.CONFIG) && (le.getMsgBody().equals("enabled"))) {
                    //if we see a agent enable command respond to it
                    logger.debug("CONFIG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    le.setMsgPlugin(null);
                    le.setMsgRegion(le.getParam("src_region"));
                    le.setMsgAgent(le.getParam("src_agent"));
                    le.removeParam("src_plugin");
                    le.setMsgBody("controllerenabled");
                    le.setParam("dst_region", le.getParam("src_region"));
                    le.setParam("dst_agent", le.getParam("src_agent"));
                    le.setSrc(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
                    //le.setDst(me.getParam("src_region"),me.getParam("src_agent"),me.getParam("src_plugin"));
                    plugin.sendMsgEvent(le);
                } else if (le.getMsgType() == MsgEvent.Type.WATCHDOG) {
                    logger.debug("WATCHDOG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    try {
                        if ((le.getParam("src_region") != null) && (le.getParam("src_agent") != null) && (le.getParam("src_plugin")) == null) { //agent
                            if (!plugin.getGDB().isNode(le.getParam("src_region"), le.getParam("src_agent"), null)) { //add if it does not exist
                                plugin.getGDB().addNode(le.getParam("src_region"), le.getParam("src_agent"), null);
                            }
                        } else if ((le.getParam("src_region") != null) && (le.getParam("src_agent") != null) && (le.getParam("src_plugin")) != null) { //plugin
                            if (!plugin.getGDB().isNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"))) { //add if it does not exist
                                plugin.getGDB().addNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"));
                            }
                        }
                    } catch (Exception ex) {
                        logger.debug("WATCHDOG : " + ex.getMessage());
                    }

                    /*
                    long watchRunTime = 0; //determine message runtime
                    long watchTimeStamp = 0; //determine message timestamp
                    try {
                        watchRunTime = Long.parseLong(le.getParam("runtime"));
                        watchTimeStamp = Long.parseLong(le.getParam("timestamp"));
                        le.setParam("timestamp", String.valueOf(System.currentTimeMillis()));
                    } catch (Exception ex) {
                        //PluginEngine.clog.getError("Problem with WATCHDOG parameters: " + ex.toString());
                        logger.info("Controller : AgentDiscovery : Problem with WATCHDOG parameters: " + ex.toString());
                    }

                    if (!PluginEngine.gdb.isNode(le.getParam("src_region"), le.getParam("src_agent"), null)) //add if it does not exist
                    {
                        MsgEvent de;
                        if (le.getParam("src_plugin") != null) {

                            String src_plugin = le.getParam("src_plugin");
                            le.removeParam("src_plugin");
                            le.setMsgPlugin(null);
                            de = refreshCmd(le); //build the discovery message discover host

                            le.setParam("src_plugin", src_plugin);
                            le.setMsgPlugin(src_plugin);
                        } else {
                            de = refreshCmd(le); //build the discovery message discover host

                        }
                        //Adding Node: region-grzv agent-ynyt null

                        if(PluginEngine.gdb.addNode(le.getParam("src_region"), le.getParam("src_agent"), null)) {
                            logger.info("WATCHDOG DISCOVERED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                        }
                        //PluginEngine.gdb.setNodeParams(le.getParam("src_region"), le.getParam("src_agent"), null, de.getParams());


                    } else //agent already exist
                    {
                        if (le.getParam("src_plugin") == null) //update agent
                        {
                            long oldRunTime = 0l;

                            if (oldRunTime > watchRunTime) //if plugin config has been reset refresh
                            {
                                logger.debug("AGENT CONFIGRUATION RESET");
                                MsgEvent newDep = refreshCmd(le);
                                PluginEngine.gdb.setNodeParams(le.getParam("src_region"), le.getParam("src_agent"), null, newDep.getParams());
                            } else {
                                PluginEngine.gdb.setNodeParam(le.getParam("src_region"), le.getParam("src_agent"), null, "runtime", le.getParam("runtime"));
                                PluginEngine.gdb.setNodeParam(le.getParam("src_region"), le.getParam("src_agent"), null, "timestamp", le.getParam("timestamp"));
                            }
                        }
                    }

                    if (le.getParam("src_plugin") != null) //if plugin discover plugin info as well
                    {
                        if (!PluginEngine.gdb.isNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"))) //agent does not exist
                        {

                            MsgEvent dep = refreshCmd(le);

                            if(PluginEngine.gdb.addNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"))) {
                                logger.info("WATCHDOG DISCOVERED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent") + " plugin:" + le.getParam("src_plugin"));
                            }
                            //PluginEngine.gdb.setNodeParams(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), dep.getParams());


                        } else //plugin exist so update
                        {
                            long oldRunTime = 0l;
                            if (oldRunTime > watchRunTime) //if plugin config has been reset refresh
                            {
                                logger.debug("PLUGIN CONFIGRUATION RESET");
                                //PluginEngine.gdb.setNodeParam(le.getMsgRegion(), le.getMsgAgent(),null,"runtime",)
                                MsgEvent newDep = refreshCmd(le);
                                PluginEngine.gdb.setNodeParams(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), newDep.getParams());
                            } else {
                                PluginEngine.gdb.setNodeParam(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), "runtime", le.getParam("runtime"));
                                PluginEngine.gdb.setNodeParam(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), "timestamp", le.getParam("timestamp"));
                            }
                        }
                    }
                    */
//end watch
                } else if (le.getMsgType() == MsgEvent.Type.KPI) {
                    logger.debug("KPI: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    //logger.info("MsgType=" + le.getMsgType() + " Params=" + le.getParams());
                    plugin.getGlobalControllerChannel().updatePerf(le);
                }
                else if (le.getMsgType() == MsgEvent.Type.INFO) {
                    logger.debug("INFO: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));

                }
                else {
                    logger.error("UNKNOWN DISCOVERY PATH! : MsgType=" + le.getMsgType() + " " +  le.getParams());
                }
                if (plugin.getDiscoveryMap().containsKey(discoverString)) {
                    plugin.getDiscoveryMap().remove(discoverString); //remove discovery block
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.debug("Controller : AgentDiscovery run() : " + ex.toString());

        }
    }

    public MsgEvent refreshCmd(MsgEvent me) {
        MsgEvent cr = null;
        boolean isLocal = false;
        try {

            if ((me.getParam("src_region") != null) && (me.getParam("src_agent") != null) && (me.getParam("src_plugin") != null)) {
                if ((me.getParam("src_region").equals(plugin.getRegion())) && (me.getParam("src_agent").equals(plugin.getAgent())) && (me.getParam("src_plugin").equals(plugin.getPluginID()))) {
                    isLocal = true; //set local messages local so we can call locally
                }
            }

            MsgEvent ce = null;

            if (me.getParam("src_plugin") != null) {
                ce = new MsgEvent(MsgEvent.Type.DISCOVER, me.getParam("src_region"), me.getParam("src_agent"), me.getParam("src_plugin"), "RequestDiscovery");
                ce.setSrc(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
                ce.setDst(me.getParam("src_region"), me.getParam("src_agent"), me.getParam("src_plugin"));
            } else {
                ce = new MsgEvent(MsgEvent.Type.DISCOVER, me.getParam("src_region"), me.getParam("src_agent"), null, "RequestDiscovery");
                ce.setSrc(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
                ce.setParam("dst_region", me.getParam("src_region"));
                ce.setParam("dst_agent", me.getParam("src_agent"));
            }

            /*
            if (isLocal) {
                cr = PluginEngine.commandExec.cmdExec(ce);
            } else {
                cr = rpcc.call(ce); //get reply from remote plugin/agent
            }
            */

            return cr;


        } catch (Exception ex) {
            logger.debug("Controller : AgentDiscovery refreshCmd : " + ex.toString());
            logger.debug("MsgType=" + me.getMsgType().toString());
            logger.debug("Region=" + me.getMsgRegion() + " Agent=" + me.getMsgAgent() + " plugin=" + me.getMsgPlugin());
            logger.debug("params=" + me.getParams());
            return null;
        }
    }

    /*class DiscoveryCleanUpTask extends TimerTask {
        public void run() {
  	  	        long timeStamp = System.currentTimeMillis();
  	  	    
  	    	    Iterator it = PluginEngine.agentStatus.entrySet().iterator();
  	    	    while (it.hasNext()) 
  	    	    {
  	    	        Map.Entry pairs = (Map.Entry)it.next();
  	    	        
  	    	        String region = pairs.getKey().toString();
  	    	        Map<String, AgentNode> regionMap = (Map<String, AgentNode>) pairs.getValue();
  	    	        //logger.debug("Cleanup Region:" + region);
	    	        
  	    	        Iterator it2 = regionMap.entrySet().iterator();
  	    	        while (it2.hasNext()) 
  	    	        {
    	    	        Map.Entry pairs2 = (Map.Entry)it2.next();
    	    	        
    	    	        String agent = pairs2.getKey().toString();
    	    	        AgentNode aNode = (AgentNode) pairs2.getValue();
      	    	        //logger.debug("Cleanup Agent:" + agent);
      	    	        MsgEvent de = aNode.getAgentDe();
      	    	        long deTimeStamp = Long.parseLong(de.getParam("timestamp"));
      	    	        deTimeStamp = deTimeStamp + PluginEngine.config.getWatchDogTimer() * 3;
      	    	        if(deTimeStamp < timeStamp)
      	    	        {
      	    	        	regionMap.remove(agent);
      	    	        	logger.debug("Removed Region:" + region + " Agent:" + agent);
      	    	        }
      	    	        else
      	    	        {
      	    	        	ArrayList<String> pluginList = aNode.getPlugins();
      	    	        	for(String plugin : pluginList)
      	    	        	{
      	    	        		MsgEvent dep = aNode.getPluginDe(plugin);
      	    	        		//logger.debug("Cleanup Agent:" + agent + " Plugin:" + plugin);
      	    	        		long depTimeStamp = Long.parseLong(dep.getParam("timestamp"));
      	      	    	        depTimeStamp = depTimeStamp + PluginEngine.config.getWatchDogTimer() * 3;
      	      	    	        if(depTimeStamp < timeStamp)
      	      	    	        {
      	      	    	        	aNode.removePlugin(plugin);
      	      	    	        	logger.debug("Removed Region:" + region + " Agent:" + agent + " plugin:" + plugin);
      	      	    	            //logger.debug("Removed Agent:" + agent + " plugin:" + plugin);
      	      	    	        }
      	    	        	}
      	    	        }
      	    	    }
  	    	        //if no agents exist remove region
  	    	        if(regionMap.size() == 0)
  	    	        {
  	    	        	PluginEngine.agentStatus.remove(region);
  	    	        }
  	    	    }

            //walk config and clean things up
        }
    }*/
}
