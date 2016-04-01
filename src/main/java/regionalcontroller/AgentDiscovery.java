package regionalcontroller;


import java.util.Timer;
import java.util.TimerTask;

import communication.RPCCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugincore.PluginEngine;
import shared.MsgEvent;
import shared.MsgEventType;


public class AgentDiscovery {

    private Timer cleanUpTimer;
    private static RPCCall rpcc;
    private static final Logger logger = LoggerFactory.getLogger(AgentDiscovery.class);

    public AgentDiscovery() throws Exception {

        rpcc = new RPCCall();

        //cleanUpTimer = new Timer();
        //cleanUpTimer.scheduleAtFixedRate(new DiscoveryCleanUpTask(), 500, PluginEngine.config.getWatchDogTimer() * 3);
    }

    public void perfupdate(MsgEvent pe) {
        //send to controller if global WatchPerf
        //System.out.println("WatchPerf: " + pe.getParamsString());
        /*
       if(!PluginEngine.controllerChannel.updatePerf(pe))
	   {
			System.out.println("Controller : AgentDiscovery : Failed to updatePerf for WatchPerf on Controller");
	   }
	   */
    }

    public void discover(MsgEvent le) {
        try {
            String discoverString = le.getParam("src_region") + "-" + le.getParam("src_agent") + "-" + le.getParam("src_plugin");

            if (PluginEngine.discoveryMap.containsKey(discoverString)) {
                System.out.println("Discovery underway for : discoverString=" + discoverString);
            } else {

                PluginEngine.discoveryMap.put(discoverString, System.currentTimeMillis());

                if ((le.getMsgType() == MsgEventType.CONFIG) && (le.getMsgBody().equals("disabled"))) {
                    //if we see a agent enable command respond to it
                    System.out.println("Remove Node: " + le.getParamsString());
                    if (le.getParam("src_plugin") == null) //if plugin discover plugin info as well
                    {
                        PluginEngine.gdb.removeNode(le.getParam("src_region"), le.getParam("src_agent"), null);
                        System.out.println("AGENT REMOVED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    } else {
                        PluginEngine.gdb.removeNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"));
                        System.out.println("PLUGIN REMOVED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent") + " Plugin:" + le.getParam("src_plugin"));

                    }
                    //le.setReturn(); //reverse
                    //PluginEngine.msgInQueue.offer(le);
             /*
			 le.setMsgPlugin(null);
			 le.setMsgRegion(le.getParam("src_region"));
			 le.setMsgAgent(le.getParam("src_agent"));
			 le.removeParam("src_plugin");
			 le.setMsgBody("controllerdisabled");
			 le.setParam("dst_region", le.getParam("src_region"));
			 le.setParam("dst_agent", le.getParam("src_agent"));
			 le.setSrc(PluginEngine.region, PluginEngine.agent, PluginEngine.plugin);
		     //le.setDst(me.getParam("src_region"),me.getParam("src_agent"),me.getParam("src_plugin"));
			 
			 PluginEngine.msgInQueue.offer(le);
			  */
                } else if ((le.getMsgType() == MsgEventType.CONFIG) && (le.getMsgBody().equals("enabled"))) {
                    //if we see a agent enable command respond to it
                    System.out.println("AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    le.setMsgPlugin(null);
                    le.setMsgRegion(le.getParam("src_region"));
                    le.setMsgAgent(le.getParam("src_agent"));
                    le.removeParam("src_plugin");
                    le.setMsgBody("controllerenabled");
                    le.setParam("dst_region", le.getParam("src_region"));
                    le.setParam("dst_agent", le.getParam("src_agent"));
                    le.setSrc(PluginEngine.region, PluginEngine.agent, PluginEngine.plugin);
                    //le.setDst(me.getParam("src_region"),me.getParam("src_agent"),me.getParam("src_plugin"));
                    PluginEngine.msgInQueue.offer(le);

                } else if (le.getMsgType() == MsgEventType.WATCHDOG) {
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
                                System.out.println("AGENT CONFIGRUATION RESET");
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
                                System.out.println("PLUGIN CONFIGRUATION RESET");
                                //PluginEngine.gdb.setNodeParam(le.getMsgRegion(), le.getMsgAgent(),null,"runtime",)
                                MsgEvent newDep = refreshCmd(le);
                                PluginEngine.gdb.setNodeParams(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), newDep.getParams());
                            } else {
                                PluginEngine.gdb.setNodeParam(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), "runtime", le.getParam("runtime"));
                                PluginEngine.gdb.setNodeParam(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), "timestamp", le.getParam("timestamp"));
                            }
                        }
                    }

                }

                if (PluginEngine.discoveryMap.containsKey(discoverString)) {
                    PluginEngine.discoveryMap.remove(discoverString); //remove discovery block
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Controller : AgentDiscovery run() : " + ex.toString());

        }
    }

    public static MsgEvent refreshCmd(MsgEvent me) {
        MsgEvent cr = null;
        boolean isLocal = false;
        try {

            if ((me.getParam("src_region") != null) && (me.getParam("src_agent") != null) && (me.getParam("src_plugin") != null)) {
                if ((me.getParam("src_region").equals(PluginEngine.region)) && (me.getParam("src_agent").equals(PluginEngine.agent)) && (me.getParam("src_plugin").equals(PluginEngine.plugin))) {
                    isLocal = true; //set local messages local so we can call locally
                }
            }

            MsgEvent ce = null;

            if (me.getParam("src_plugin") != null) {
                ce = new MsgEvent(MsgEventType.DISCOVER, me.getParam("src_region"), me.getParam("src_agent"), me.getParam("src_plugin"), "RequestDiscovery");
                ce.setSrc(PluginEngine.region, PluginEngine.agent, PluginEngine.plugin);
                ce.setDst(me.getParam("src_region"), me.getParam("src_agent"), me.getParam("src_plugin"));
            } else {
                ce = new MsgEvent(MsgEventType.DISCOVER, me.getParam("src_region"), me.getParam("src_agent"), null, "RequestDiscovery");
                ce.setSrc(PluginEngine.region, PluginEngine.agent, PluginEngine.plugin);
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
            System.out.println("Controller : AgentDiscovery refreshCmd : " + ex.toString());
            System.out.println("MsgType=" + me.getMsgType().toString());
            System.out.println("Region=" + me.getMsgRegion() + " Agent=" + me.getMsgAgent() + " plugin=" + me.getMsgPlugin());
            System.out.println("params=" + me.getParamsString());
            return null;
        }
    }

    class DiscoveryCleanUpTask extends TimerTask {
        public void run() {
  	    	/*
  	  	        long timeStamp = System.currentTimeMillis();
  	  	    
  	    	    Iterator it = PluginEngine.agentStatus.entrySet().iterator();
  	    	    while (it.hasNext()) 
  	    	    {
  	    	        Map.Entry pairs = (Map.Entry)it.next();
  	    	        
  	    	        String region = pairs.getKey().toString();
  	    	        Map<String, AgentNode> regionMap = (Map<String, AgentNode>) pairs.getValue();
  	    	        //System.out.println("Cleanup Region:" + region);
	    	        
  	    	        Iterator it2 = regionMap.entrySet().iterator();
  	    	        while (it2.hasNext()) 
  	    	        {
    	    	        Map.Entry pairs2 = (Map.Entry)it2.next();
    	    	        
    	    	        String agent = pairs2.getKey().toString();
    	    	        AgentNode aNode = (AgentNode) pairs2.getValue();
      	    	        //System.out.println("Cleanup Agent:" + agent);
      	    	        MsgEvent de = aNode.getAgentDe();
      	    	        long deTimeStamp = Long.parseLong(de.getParam("timestamp"));
      	    	        deTimeStamp = deTimeStamp + PluginEngine.config.getWatchDogTimer() * 3;
      	    	        if(deTimeStamp < timeStamp)
      	    	        {
      	    	        	regionMap.remove(agent);
      	    	        	System.out.println("Removed Region:" + region + " Agent:" + agent);
      	    	        }
      	    	        else
      	    	        {
      	    	        	ArrayList<String> pluginList = aNode.getPlugins();
      	    	        	for(String plugin : pluginList)
      	    	        	{
      	    	        		MsgEvent dep = aNode.getPluginDe(plugin);
      	    	        		//System.out.println("Cleanup Agent:" + agent + " Plugin:" + plugin);
      	    	        		long depTimeStamp = Long.parseLong(dep.getParam("timestamp"));
      	      	    	        depTimeStamp = depTimeStamp + PluginEngine.config.getWatchDogTimer() * 3;
      	      	    	        if(depTimeStamp < timeStamp)
      	      	    	        {
      	      	    	        	aNode.removePlugin(plugin);
      	      	    	        	System.out.println("Removed Region:" + region + " Agent:" + agent + " plugin:" + plugin);
      	      	    	            //System.out.println("Removed Agent:" + agent + " plugin:" + plugin);
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
  	    	*/

            //walk config and clean things up
        }
    }
}
