package plugincore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.MsgEvent;
import shared.MsgEventType;



public class CommandExec {

	public CommandExec()
	{
		//toats
	}
	private static final Logger logger = LoggerFactory.getLogger(CommandExec.class);

	public MsgEvent cmdExec(MsgEvent ce) 
	{
		try
		{
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
						if (ce.getParam("cmd").equals("show_name")) {
							ce.setMsgBody(PluginEngine.pluginName);
							return ce;
						} else if (ce.getParam("cmd").equals("show_version")) {
							ce.setMsgBody(PluginEngine.pluginVersion);
							return ce;
						}
					}
			return null;
		}
		catch(Exception ex)
		 {
			 logger.error(ex.getMessage());
			 return null;
		 }
	}
	
}
