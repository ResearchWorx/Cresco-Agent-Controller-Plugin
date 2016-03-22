package plugincore;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shared.MsgEvent;
import shared.MsgEventType;



public class WatchDog {
	public Timer timer;
	private long startTS;
	//private Map<String,String> wdMap;
	private int wdTimer;
	private static final Logger logger = LoggerFactory.getLogger(WatchDog.class);

	public WatchDog() {
		logger.debug("Watchdog initialized");
		wdTimer = 1000;
		startTS = System.currentTimeMillis();
		timer = new Timer();
	    timer.scheduleAtFixedRate(new WatchDogTask(), 500, wdTimer);
	    //wdMap = new HashMap<>(); //for sending future WD messages
	      	  
	    MsgEvent le = new MsgEvent(MsgEventType.INFO,PluginEngine.region,null,null,"WatchDog timer set to " + wdTimer + " milliseconds");
	    le.setParam("src_region", PluginEngine.region);
		le.setParam("src_agent", PluginEngine.agent);
		le.setParam("src_plugin", PluginEngine.plugin);
		le.setParam("dst_region", PluginEngine.region);
		//PluginEngine.clog.log(le);
	}

	public void shutdown() {
		timer.cancel();
		logger.debug("WatchDog has shutdown");
	}

	class WatchDogTask extends TimerTask {
	    public void run() {
			logger.trace("Watchdog tick");
	    	boolean isHealthy = true;
	    	try {
	    		if((!PluginEngine.ConsumerThreadActive) || !(PluginEngine.consumerAgentThread.isAlive())) {
	    			isHealthy = false;
					logger.info("Agent Consumer shutdown detected");
	    		}
				
	    		if(PluginEngine.isRegionalController) {
	    			if(!PluginEngine.DiscoveryActive) {
	    				isHealthy = false;
						logger.info("Discovery shutdown detected");
	    				
	    			}
					if(!(PluginEngine.ConsumerThreadRegionActive) || !(PluginEngine.consumerRegionThread.isAlive())) {
		    			isHealthy = false;
						logger.info("Region Consumer shutdown detected");
		    		}
	    			if(!(PluginEngine.ActiveBrokerManagerActive) || !(PluginEngine.activeBrokerManagerThread.isAlive())) {
	    				isHealthy = false;
						logger.info("Active Broker Manager shutdown detected");
	    			}
	    			if(!PluginEngine.broker.isHealthy()) {
	    				isHealthy = false;
						logger.debug("Broker shutdown detected");
	    			}
	    		}
	    		if(!isHealthy) {
					logger.debug("System has become unhealthy, rebooting services");
	    			PluginEngine.restartOnShutdown = true;
	    			PluginEngine.shutdown();
	    		}
	    	} catch(Exception ex) {
				logger.error("Run {}", ex.getMessage());
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
