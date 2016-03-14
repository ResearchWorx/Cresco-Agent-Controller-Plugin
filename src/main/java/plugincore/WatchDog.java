package plugincore;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import shared.MsgEvent;
import shared.MsgEventType;



public class WatchDog {
	public Timer timer;
	private long startTS;
	private Map<String,String> wdMap;
	private int wdTimer;
	
	public WatchDog() {
		  wdTimer = 1000;
		  startTS = System.currentTimeMillis();
		  timer = new Timer();
	      timer.scheduleAtFixedRate(new WatchDogTask(), 500, wdTimer);
	      wdMap = new HashMap<String,String>(); //for sending future WD messages
	      	  
	      MsgEvent le = new MsgEvent(MsgEventType.INFO,PluginEngine.region,null,null,"WatchDog timer set to " + wdTimer + " milliseconds");
	      le.setParam("src_region", PluginEngine.region);
		  le.setParam("src_agent", PluginEngine.agent);
		  le.setParam("src_plugin", PluginEngine.plugin);
		  le.setParam("dst_region", PluginEngine.region);
		  //PluginEngine.clog.log(le);
	      
	  }


	class WatchDogTask extends TimerTask {
	    public void run() {
	    	
	    	boolean isHealthy = true;
	    	try
	    	{
	    		if((!PluginEngine.ConsumerThreadActive) || !(PluginEngine.consumerAgentThread.isAlive()))
	    		{
	    			isHealthy = false;
	    		}
				
	    		if(PluginEngine.isBroker)
	    		{
	    			if(!(PluginEngine.DiscoveryActive) || !(PluginEngine.discoveryEngineThread.isAlive()))
	    			{
	    				isHealthy = false;
	    			}
	    		
	    			if(!(PluginEngine.ConsumerThreadRegionActive) || !(PluginEngine.consumerRegionThread.isAlive()))
		    		{
		    			isHealthy = false;
		    		}
	    			
	    			if(!(PluginEngine.ActiveBrokerManagerActive) || !(PluginEngine.activeBrokerManagerThread.isAlive()))
	    			{
	    				isHealthy = false;
	    			}
	    			
	    			if(PluginEngine.broker.isHealthy())
	    			{
	    				isHealthy = false;
	    			}
	    		}
	    		if(!isHealthy)
	    		{
	    			PluginEngine.shutdown();
	    		}
	    	}
	    	catch(Exception ex)
	    	{
	    		System.out.println("WathcDog : Error : " + ex.getMessage());
	    	}
	    	long runTime = System.currentTimeMillis() - startTS;
	    	 //wdMap.put("runtime", String.valueOf(runTime));
	    	 //wdMap.put("timestamp", String.valueOf(System.currentTimeMillis()));
	    	 MsgEvent le = new MsgEvent(MsgEventType.WATCHDOG,PluginEngine.region,null,null,"WatchDog timer set to " + wdTimer + " milliseconds");
	    	 le.setParam("src_region", PluginEngine.region);
			 le.setParam("src_agent", PluginEngine.agent);
			 le.setParam("src_plugin", PluginEngine.plugin);
			 le.setParam("dst_region", PluginEngine.region);
			 le.setParam("runtime", String.valueOf(runTime));
			 le.setParam("timestamp", String.valueOf(System.currentTimeMillis()));
			 //PluginEngine.clog.log(le);
	    }
	  }

}
