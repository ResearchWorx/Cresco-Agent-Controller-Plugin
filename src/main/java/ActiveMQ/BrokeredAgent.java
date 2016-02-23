package ActiveMQ;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {

	  public Map<String,BrokerStatusType> addressMap;
	  public BrokerStatusType brokerStatus;
	  public String activeAddress;
	  public String agentPath;
	  
	  public BrokeredAgent(String activeAddress, String agentPath)
	  {
		this.activeAddress = activeAddress;
		this.agentPath = agentPath;
		this.brokerStatus = BrokerStatusType.STARTING;
		this.addressMap = new HashMap<String,BrokerStatusType>();
	  }
	  
	    	  
	}