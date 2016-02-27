package ActiveMQ;

import plugincore.PluginEngine;
import shared.MsgEvent;



public class ActiveProducer implements Runnable
{
	
public ActiveProducer(String TXQueueName, String URI) 
{
	try
	{
		
	}
	catch(Exception ex)
	{
		System.out.println("ActiveProducer Init " + ex.toString());
	}
}

	
@Override
public void run() {
	PluginEngine.ProducerThreadActive = true;
    try {
    	
    	while(PluginEngine.ProducerThreadActive)
    	{
    		MsgEvent om = PluginEngine.outgoingMessages.poll();
    		if(om !=null)
    		{
    			System.out.println("outgoing message to " + om.getMsgRegion() + "_" + om.getMsgAgent());
    			/*
    			if(PluginEngine.isReachableAgent(des.getPhysicalName()))
				{
					//System.out.println("Dest: " + des.getPhysicalName() + "starting feed");
					//ActiveProducer ap = new ActiveProducer(des.getPhysicalName(),"tcp://[::1]:32010");
					//cm.put(des.getPhysicalName(), ap);
					//new Thread(ap).start();
					
				}
				*/
    		}
    				
    		Thread.sleep(1000);
    	}
    } 
    catch (Exception ex) {
        System.out.println("ActiveProducer : Run : " + ex.toString());
    }
}
}