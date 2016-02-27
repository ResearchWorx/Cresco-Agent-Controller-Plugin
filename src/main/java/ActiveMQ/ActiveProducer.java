package ActiveMQ;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveProducer
{

	public Map<String,ActiveProducerWorker> producerWorkers;
	
	private String URI;
	
	class ClearProducerTask extends TimerTask 
	{
		
	    public void run() 
	    {
	    	for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet())
	    	{
	    	    //System.out.println(entry.getKey() + "/" + entry.getValue());
	    		ActiveProducerWorker apw = entry.getValue();
	    		if(apw.isActive)
		    	{
	    			apw.isActive = false;
	    			producerWorkers.put(entry.getKey(),apw);
		    	}
		    	else
		    	{
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
	}
	catch(Exception ex)
	{
		System.out.println("ActiveProducer Init " + ex.toString());
	}
}

public boolean sendMessage(MsgEvent sm)
{
	boolean isSent = false;
	try
	{
		String agentPath = sm.getMsgRegion() + "_" + sm.getMsgAgent();
		ActiveProducerWorker apw = null;
		if(!producerWorkers.containsKey(agentPath))
		{
			if(PluginEngine.isReachableAgent(agentPath))
			{
				apw = producerWorkers.get(agentPath);
			}
		}
		else
		{
			apw = new ActiveProducerWorker(agentPath,URI);
	    	
		}
		if(apw != null)
		{
			apw.sendMessage(sm);
			isSent = true;
		}
		
		
	}
	catch(Exception ex)
	{
		System.out.println("ActiveProducer : sendMessage Error " + ex.toString());
	}
	
	return isSent;
	
}

	

}