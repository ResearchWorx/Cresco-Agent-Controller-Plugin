package plugincore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.MsgEvent;

public class MsgRoute implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(MsgRoute.class);

    private MsgEvent rm;
	public MsgRoute(MsgEvent rm)
	{
		this.rm = rm;
	}
	public void run()
	{
     try{
		 if(!getTtl()) { //check ttl
			 return;
		 }

         int routePath = getRoutePath();

         MsgEvent re = null;
         switch (routePath) {
             case 1:  System.out.println("CONTROLLER ROUTE CASE 1");
                 break;
             case 40:  System.out.println("CONTROLLER ROUTE TO REGIONAL AGENT : 40 " + rm.getParams());
                 externalSend();
                 break;
             case 42:  System.out.println("CONTROLLER ROUTE TO COMMANDEXEC : 42 "  + rm.getParams());
                 String callId = "callId-" + PluginEngine.region + "_" + PluginEngine.agent; //calculate callID
                 if(rm.getParam(callId) != null) { //send message to RPC hash
                     PluginEngine.rpcMap.put(rm.getParam(callId), rm);
                 }
                 else {
                     re = PluginEngine.commandExec.cmdExec(rm);
                 }
                 break;
                 //case 42:  System.out.println("CONTROLLER ROUTE LOCAL PLUGIN TO LOCAL AGNET");
             //          PluginEngine.msgInQueue.offer(rm);
             //    break;
             default: System.out.println("CONTROLLER ROUTE CASE " + routePath + " " + rm.getParams());
                 break;
         }
         if(re != null)
         {
             re.setReturn(); //reverse to-from for return
             PluginEngine.msgInQueue.offer(re);
         }
                 //	AgentEngine.commandExec.cmdExec(me);

     }
     catch(Exception ex)
     {
         ex.printStackTrace();
    	 System.out.println("Agent : MsgRoute : Route Failed " + ex.toString());
     }

	}

    private void externalSend() {
        String targetAgent = null;
        try {

            if ((rm.getParam("dst_region") != null) && (rm.getParam("dst_agent") != null)) {
                //agent message
                targetAgent = rm.getParam("dst_region") + "_" + rm.getParam("dst_agent");

            }
            else if ((rm.getParam("dst_region") != null) && (rm.getParam("dst_agent") == null)) {
                //regional message
                targetAgent = rm.getParam("dst_region");
            }

            if (PluginEngine.isReachableAgent(targetAgent)) {
                PluginEngine.ap.sendMessage(rm);
                //System.out.println("SENT NOT CONTROLLER MESSAGE / REMOTE=: " + targetAgent + " " + " region=" + ce.getParam("dst_region") + " agent=" + ce.getParam("dst_agent") + " "  + ce.getParams());
            } else {
                logger.error("Unreachable External Agent : " + targetAgent);
            }
        }
        catch(Exception ex) {
            logger.error("External Send Error : " + targetAgent);
        }


    }
    private int getRoutePath() {
        int routePath = -1;
        try {
            //determine if local or controller
            String RXr = "0";
            String RXa = "0";
            String RXp = "0";
            String TXr = "0";
            String TXa = "0";
            String TXp = "0";

            if (rm.getParam("dst_region") != null) {
                if (rm.getParam("dst_region").equals(PluginEngine.region)) {
                    RXr = "1";
                    if (rm.getParam("dst_agent") != null) {
                        if (rm.getParam("dst_agent").equals(PluginEngine.agent)) {
                            RXa = "1";
                            if (rm.getParam("dst_plugin") != null) {
                                if (rm.getParam("dst_plugin").equals(PluginEngine.plugin)) {
                                    RXp = "1";
                                }
                            }
                        }
                    }
                }

            }

            if (rm.getParam("src_region") != null) {
                if (rm.getParam("src_region").equals(PluginEngine.region)) {
                    TXr = "1";
                    if (rm.getParam("src_agent") != null) {
                        if (rm.getParam("src_agent").equals(PluginEngine.agent)) {
                            TXa = "1";
                            if (rm.getParam("src_plugin") != null) {
                                if (rm.getParam("ssrc_plugin").equals(PluginEngine.plugin)) {
                                    TXp = "1";
                                }
                            }
                        }
                    }
                }

            }

            String routeString = RXr + TXr + RXa + TXa + RXp + TXp;
            routePath = Integer.parseInt(routeString, 2);
        }
        catch(Exception ex)
        {
            System.out.println("AgentEngine : MsgRoute : getRoutePath Error: " + ex.getMessage());
            ex.printStackTrace();
            routePath = -1;
        }
        return routePath;
    }
	private boolean getTtl() {
		boolean isValid = true;
		try {
			if(rm.getParam("ttl") != null) //loop detection
			{
				int ttlCount = Integer.valueOf(rm.getParam("ttl"));

				if(ttlCount > 10)
				{
					System.out.println("**Agent : MsgRoute : High Loop Count**");
					System.out.println("MsgType=" + rm.getMsgType().toString());
					System.out.println("Region=" + rm.getMsgRegion() + " Agent=" + rm.getMsgAgent() + " plugin=" + rm.getMsgPlugin());
					System.out.println("params=" + rm.getParamsString());
					isValid = false;
				}

				ttlCount++;
				rm.setParam("ttl", String.valueOf(ttlCount));
			}
			else
			{
				rm.setParam("ttl", "0");
			}

		}
		catch(Exception ex) {
			isValid = false;
		}
		return isValid;
	}
}
