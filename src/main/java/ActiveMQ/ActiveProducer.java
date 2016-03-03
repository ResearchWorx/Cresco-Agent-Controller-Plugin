package ActiveMQ;

import java.util.Map;
import java.util.Timer;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveProducer
{

	public Map<String,ActiveProducerWorker> producerWorkers;
	
	private String URI;
	private Timer timer;
	  
	class ClearProducerTask extends TimerTask 
	{
		
	    public void run() 
	    {
	    	
	    	for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet())
	    	{
	    	    System.out.println("Cleanup: " + entry.getKey() + "/" + entry.getValue());
	    		ActiveProducerWorker apw = entry.getValue();
	    		if(apw.isActive)
		    	{
					//System.out.println("Marking ActiveProducerWork [" + entry.getKey() + "] inactive");
	    			apw.isActive = false;
	    			producerWorkers.put(entry.getKey(),apw);
		    	}
		    	else
		    	{
					//System.out.println("Shutting Down/Removing ActiveProducerWork [" + entry.getKey() + "]");
		    		if(apw.shutdown())
		    		{
		    			producerWorkers.remove(entry.getKey());
		    		}
		    		
		    	}
		    	
	    	}
	    	
	    }
	  }

	
public ActiveProducer(String URI) 
{
	
	try
	{
		producerWorkers = new ConcurrentHashMap<String,ActiveProducerWorker>();
		this.URI = URI;
		timer = new Timer();
		timer.scheduleAtFixedRate(new ClearProducerTask(), 5000, 5000);//start at 5000 end at 5000
	}
	catch(Exception ex)
	{
		ex.printStackTrace();
		System.out.println("ActiveProducer Init " + ex.toString());
	}
}

public boolean sendMessage(MsgEvent sm)
{
	boolean isSent = false;
	try
	{
		ActiveProducerWorker apw = null;
		/*String agentPath = sm.getMsgRegion() + "_" + sm.getMsgAgent();
		if(producerWorkers.containsKey(agentPath))
		{
			if(PluginEngine.isReachableAgent(agentPath))
			{
				apw = producerWorkers.get(agentPath);
			}
			else
			{
				System.out.println(agentPath + " is unreachable...");
			}
		}
		else
		{
			if (PluginEngine.isReachableAgent(agentPath))
			{
				System.out.println("Creating new ActiveProducerWorker [" + agentPath + "]");
				apw = new ActiveProducerWorker(agentPath, URI);
			}
			else
			{
				System.out.println(agentPath + " is unreachable...");
			}
	    	
		}*/
		String dstPath = sm.getParam("dst_region") + "_" + sm.getParam("dst_agent");
		if ((apw = producerWorkers.get(dstPath)) == null) {
			System.out.println("Creating new ActiveProducerWorker [" + dstPath + "]");
			apw = new ActiveProducerWorker(dstPath, URI);
			producerWorkers.put(dstPath, apw);
		}
		if(/*apw != null && */PluginEngine.isReachableAgent(dstPath))
		{
			apw.isActive = true;
			System.out.println("Dest: " + dstPath + " is reachable = true");
			apw.sendMessage(sm);
			isSent = true;
		}
		else
		{
			//System.out.println("apw is null");
			System.out.println("apw is inactive");
		}
		
	}
	catch(Exception ex)
	{
		System.out.println("ActiveProducer : sendMessage Error " + ex.toString());
	}
	
	return isSent;
	
}

	

}