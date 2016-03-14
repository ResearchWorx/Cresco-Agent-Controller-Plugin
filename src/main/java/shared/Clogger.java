package shared;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;


public class Clogger {

	private String region;
	private String agent;
	private String plugin;
	
	private ConcurrentLinkedQueue<MsgEvent> logOutQueue;
	public Clogger(ConcurrentLinkedQueue<MsgEvent> msgOutQueue, String region, String agent, String plugin)
	{
		this.region = region;
		this.agent = agent;
		this.plugin = plugin;
		
		this.logOutQueue = msgOutQueue;
	}
	
	public void info(String logMessage) {
		MsgEvent me = new MsgEvent(MsgEventType.INFO,region,null,null,logMessage);
		me.setParam("src_region", region);
		if(agent != null) {
			me.setParam("src_agent", agent);
			if(plugin != null) {
				me.setParam("src_plugin", plugin);
			}
		}
		me.setParam("dst_region", region);
		logOutQueue.offer(me);
	}
	public void info(String logMessage, String ... params) {
		String msg = logMessage;
		for (String param : params) {
			int loc = msg.indexOf("{}");
			if (loc >= 0) {
				msg = msg.replaceFirst(Pattern.quote("{}"), param);
			}
		}
		MsgEvent me = new MsgEvent(MsgEventType.INFO,region,null,null,msg);
		me.setParam("src_region", region);
		if(agent != null) {
			me.setParam("src_agent", agent);
			if(plugin != null) {
				me.setParam("src_plugin", plugin);
			}
		}
		me.setParam("dst_region", region);
		logOutQueue.offer(me);
	}

	public void log(MsgEvent me) {
		logOutQueue.offer(me);
	}
	public MsgEvent getLog(String logMessage) {
		MsgEvent me = new MsgEvent(MsgEventType.INFO,region,null,null,logMessage);
		me.setParam("src_region", region);
		if(agent != null)
		{
			me.setParam("src_agent", agent);
			if(plugin != null)
			{
				me.setParam("src_plugin", plugin);
			}
		}
		me.setParam("dst_region", region);
		return me;
	}

	public void error(String ErrorMessage) {
		MsgEvent ee = new MsgEvent(MsgEventType.ERROR,region,null,null,ErrorMessage);
		ee.setParam("src_region", region);
		if(agent != null)
		{
			ee.setParam("src_agent", agent);
			if(plugin != null)
			{
				ee.setParam("src_plugin", plugin);
			}
		}
		ee.setParam("dst_region", region);
		logOutQueue.offer(ee);
	}
	public void error(String ErrorMessage, String ... params) {
		String msg = ErrorMessage;
		for (String param : params) {
			int loc = msg.indexOf("{}");
			if (loc >= 0) {
				msg = msg.replaceFirst(Pattern.quote("{}"), param);
			}
		}
		MsgEvent ee = new MsgEvent(MsgEventType.ERROR,region,null,null,msg);
		ee.setParam("src_region", region);
		if(agent != null)
		{
			ee.setParam("src_agent", agent);
			if(plugin != null)
			{
				ee.setParam("src_plugin", plugin);
			}
		}
		ee.setParam("dst_region", region);
		logOutQueue.offer(ee);
	}
	public MsgEvent getError(String ErrorMessage) {
		MsgEvent ee = new MsgEvent(MsgEventType.ERROR,region,null,null,ErrorMessage);
		ee.setParam("src_region", region);
		if(agent != null)
		{
			ee.setParam("src_agent", agent);
			if(plugin != null)
			{
				ee.setParam("src_plugin", plugin);
			}
		}
		ee.setParam("dst_region", region);
		return ee;
	}
	
}
