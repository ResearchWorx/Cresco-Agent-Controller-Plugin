package plugincore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.MsgEvent;
import shared.MsgEventType;


public class CommandExec_kindaold {

	public CommandExec_kindaold()
	{
		//toats
	}
	private static final Logger logger = LoggerFactory.getLogger(CommandExec_kindaold.class);

	public MsgEvent cmdExec(MsgEvent ce) 
	{
		try
		{

			boolean isLocal = false;
			boolean isLocalPlugin = false;
			if ((ce.getParam("dst_region") != null) && (ce.getParam("dst_agent") != null)) {
				if ((ce.getParam("dst_region").equals(PluginEngine.region)) && (ce.getParam("dst_agent").equals(PluginEngine.agent))) {
					isLocal = true;
					if (ce.getParam("dst_plugin") != null) {
						if(ce.getParam("dst_plugin").equals(PluginEngine.plugin)) {
							isLocalPlugin = true;
						}
					}
				}
			}

			System.out.println("CONTROLLER: MESSAGE isLocal: " + isLocal + " isLocalPlugin: " + isLocalPlugin);

			if (isLocal) {
				if(isLocalPlugin) {

					/*
					String callId = ce.getParam("callId-" + PluginEngine.region + "-" + PluginEngine.agent + "-" + PluginEngine.plugin); //unique callId
					if (callId != null) //this is a callback put in RPC hashmap
					{
						//PluginEngine.rpcMap.put(callId, ce);
					}
					*/
					if (ce.getMsgType() == MsgEventType.CONFIG) //for init
					{
						if (ce.getMsgBody() != null) {
							if (ce.getMsgBody().equals("comminit")) {
								PluginEngine.commInit(); //initial init
								ce.setParam("set_region", PluginEngine.region);
								ce.setParam("set_agent", PluginEngine.agent);
								ce.setParam("is_regional_controller", Boolean.toString(PluginEngine.isRegionalController));
								ce.setParam("is_active", Boolean.toString(PluginEngine.isActive));

							}
							return ce;
						}
					}
					else if (ce.getMsgType() == MsgEventType.EXEC) {
						System.out.println("THIS IS WHERE IT SHOULD STOP!!!");
						if (ce.getParam("cmd").equals("show_name")) {
							ce.setMsgBody(PluginEngine.pluginName);
							return ce;
						} else if (ce.getParam("cmd").equals("show_version")) {
							ce.setMsgBody(PluginEngine.pluginVersion);
							return ce;
						}
					}
				}
				else { //local, but not for this plugin, sent to agent.
					//System.out.println("LOCAL AGENT MESSAGE Sending to Agent: " + ce.getParams());
					PluginEngine.msgInQueue.offer(ce);
				}
				return null; //default don't send anything back
			}
			else { //not local message send over broker

				String targetAgent = null;
				if ((ce.getParam("dst_region") != null) && (ce.getParam("dst_agent") != null)) {
					//agent message
					targetAgent = ce.getParam("dst_region") + "_" + ce.getParam("dst_agent");

				}
				else if ((ce.getParam("dst_region") != null) && (ce.getParam("dst_agent") == null)) {
					//regional message
					targetAgent = ce.getParam("dst_region");
				}

				if (PluginEngine.isReachableAgent(targetAgent)) {
					PluginEngine.ap.sendMessage(ce);
					//System.out.println("SENT NOT CONTROLLER MESSAGE / REMOTE=: " + targetAgent + " " + " region=" + ce.getParam("dst_region") + " agent=" + ce.getParam("dst_agent") + " "  + ce.getParams());
				} else {
					logger.error("Unreachable External Agent : " + targetAgent);
				}
				return null;
			}
		//end try
		}
		catch(Exception ex)
		 {
			/*
			 MsgEvent ee = PluginEngine.clog.getError("Agent : CommandExec : Error" + ex.toString());
			 System.out.println("MsgType=" + ce.getMsgType().toString());
			 System.out.println("Region=" + ce.getMsgRegion() + " Agent=" + ce.getMsgAgent() + " plugin=" + ce.getMsgPlugin());
			 System.out.println("params=" + ce.getParamsString()); 
			 return ee;
			 */
			System.out.println("MsgType=" + ce.getMsgType().toString());
			 return null;
		 }
	}
	
}
