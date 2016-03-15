package ActiveMQ;

import java.util.Map;
import java.util.Timer;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveProducer {

	public Map<String,ActiveProducerWorker> producerWorkers;
	
	private String URI;
	private Timer timer;

	private static final Logger logger = LoggerFactory.getLogger(ActiveProducer.class);

	class ClearProducerTask extends TimerTask 
	{
		
	    public void run() 
	    {
	    	logger.trace("Tick");
	    	for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet())
	    	{
	    	    //System.out.println("Cleanup: " + entry.getKey() + "/" + entry.getValue());
	    		ActiveProducerWorker apw = entry.getValue();
	    		if(apw.isActive)
		    	{
					//System.out.println("Marking ActiveProducerWork [" + entry.getKey() + "] inactive");
	    			apw.isActive = false;
	    			//producerWorkers.put(entry.getKey(),apw);
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
		producerWorkers = new ConcurrentHashMap<>();
		this.URI = URI;
		timer = new Timer();
		timer.scheduleAtFixedRate(new ClearProducerTask(), 5000, 5000);//start at 5000 end at 5000
		logger.debug("Producer initialized");
	}
	catch(Exception ex)
	{
		ex.printStackTrace();
		System.out.println("ActiveProducer Init " + ex.toString());
	}
}

	public void shutdown() {
		for (Entry<String, ActiveProducerWorker> entry : producerWorkers.entrySet())
		{
			entry.getValue().shutdown();
		}
		timer.cancel();
		logger.debug("Producer has shutdown");
	}

public boolean sendMessage(MsgEvent sm)
{
	boolean isSent = false;
	try
	{
		ActiveProducerWorker apw = null;
		String dstPath;
		if(PluginEngine.isBroker)
		{
			dstPath = sm.getParam("dst_region") + "_" + sm.getParam("dst_agent");
		}
		else
		{
			dstPath = sm.getParam("dst_region"); //send to broker for routing
		}
		if(producerWorkers.containsKey(dstPath))
		{
			if(PluginEngine.isReachableAgent(dstPath))
			{
				apw = producerWorkers.get(dstPath);
			}
			else
			{
				System.out.println(dstPath + " is unreachable...");
			}
		}
		else
		{
			if (PluginEngine.isReachableAgent(dstPath))
			{
				System.out.println("Creating new ActiveProducerWorker [" + dstPath + "]");
				apw = new ActiveProducerWorker(dstPath, URI);
				producerWorkers.put(dstPath, apw);
			}
			else
			{
				System.out.println(dstPath + " is unreachable...");
			}
	    	
		}
		if(apw != null)
		{
			apw.isActive = true;
			apw.sendMessage(sm);
			isSent = true;
		}
		else
		{
			System.out.println("apw is null");
		}
		
	}
	catch(Exception ex)
	{
		System.out.println("ActiveProducer : sendMessage Error " + ex.toString());
	}
	
	return isSent;
	
}

	

}