package ActiveMQ;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {

	  public Map<String,BrokerStatusType> addressMap;
	  public BrokerStatusType brokerStatus;
	  public String activeAddress;
	  public String agentPath;
	  public BrokerMonitor bm;
	  public boolean BrokerMonitorActive = false;
	  
	  public BrokeredAgent(String activeAddress, String agentPath)
	  {
		this.bm = new BrokerMonitor(agentPath);
		this.activeAddress = activeAddress;
		this.agentPath = agentPath;
		this.brokerStatus = BrokerStatusType.INIT;
		this.addressMap = new HashMap<String,BrokerStatusType>();
		this.addressMap.put(activeAddress, BrokerStatusType.INIT);
		
	  }
	  
	  public void setStarting()
	  {
		  brokerStatus = BrokerStatusType.STARTING;
		  addressMap.put(activeAddress, BrokerStatusType.STARTING);
		  if(BrokerMonitorActive)
		  {
			  bm.shutdown();
		  }
		  bm = new BrokerMonitor(agentPath);
		  new Thread(bm).start();
	  }
	  public void setActive()
	  {
		  brokerStatus = BrokerStatusType.ACTIVE;
		  addressMap.put(activeAddress, BrokerStatusType.ACTIVE);
	  }
	    	  
	}