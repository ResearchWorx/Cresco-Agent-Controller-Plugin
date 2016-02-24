package ActiveMQ;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {

	  public Map<String,BrokerStatusType> addressMap;
	  public BrokerStatusType brokerStatus;
	  public String activeAddress;
	  public String agentPath;
	  public BrokerMonitor bm;
	  
	  public BrokeredAgent(String activeAddress, String agentPath)
	  {
		this.bm = new BrokerMonitor(agentPath);
		this.activeAddress = activeAddress;
		this.agentPath = agentPath;
		this.brokerStatus = BrokerStatusType.INIT;
		this.addressMap = new HashMap<String,BrokerStatusType>();
		this.addressMap.put(activeAddress, BrokerStatusType.INIT);
		
	  }
	  public void setStop()
	  {
		  if(bm.MonitorActive)
		  {
			  bm.shutdown();
		  }
		  while(bm.MonitorActive)
		  {
			  try {
				  //System.out.println("STOPPING " + agentPath);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		  brokerStatus = BrokerStatusType.STOPPED;
	  }
	  public void setStarting()
	  {
		  brokerStatus = BrokerStatusType.STARTING;
		  addressMap.put(activeAddress, BrokerStatusType.STARTING);
		  if(bm.MonitorActive)
		  {
			  bm.shutdown();
		  }
		  bm = new BrokerMonitor(agentPath);
		  new Thread(bm).start();
		  while(!bm.MonitorActive)
		  {
			  try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
	  }
	  public void setActive()
	  {
		  brokerStatus = BrokerStatusType.ACTIVE;
		  addressMap.put(activeAddress, BrokerStatusType.ACTIVE);
	  }
	    	  
	}