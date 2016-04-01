package communication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class BrokeredAgent {
	public Map<String,BrokerStatusType> addressMap;
	public BrokerStatusType brokerStatus;
	public String activeAddress;
	public String agentPath;
	public BrokerMonitor bm;
	private static final Logger logger = LoggerFactory.getLogger(BrokeredAgent.class);

	public BrokeredAgent(String activeAddress, String agentPath) {
		logger.debug("Creating BrokerAgent : " + agentPath + " address: " + activeAddress);
		System.out.print("Name of Agent to message [q to quit]: ");
		this.bm = new BrokerMonitor(agentPath);
		this.activeAddress = activeAddress;
		this.agentPath = agentPath;
		this.brokerStatus = BrokerStatusType.INIT;
		this.addressMap = new HashMap<>();
		this.addressMap.put(activeAddress, BrokerStatusType.INIT);
	}

	public void setStop() {
		if(bm.MonitorActive) {
			bm.shutdown();
		}
		while(bm.MonitorActive) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("setStop {}", e.getMessage());
			}
		}
		brokerStatus = BrokerStatusType.STOPPED;
		logger.debug("BrokeredAgent : setStop : Broker STOP");
	}

	public void setStarting() {
		brokerStatus = BrokerStatusType.STARTING;
		addressMap.put(activeAddress, BrokerStatusType.STARTING);
		if(bm.MonitorActive) {
			bm.shutdown();
		}
		bm = new BrokerMonitor(agentPath);
		new Thread(bm).start();
		while(!bm.MonitorActive) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("setStarting {}", e.getMessage());
			}
		}
	}

	public void setActive() {
		brokerStatus = BrokerStatusType.ACTIVE;
		addressMap.put(activeAddress, BrokerStatusType.ACTIVE);
	}
}