package ActiveMQ;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;


public class ActiveBroker {

	public static BrokerService broker;
	
	private NetworkConnector bridge(BrokerService from, BrokerService to) throws Exception {
		   TransportConnector toConnector = to.getTransportConnectors().get(0);
		   NetworkConnector bridge = from.addNetworkConnector("static://" + toConnector.getPublishableConnectString());
		   //bridge.addStaticallyIncludedDestination(sendQ);
		   //bridge.addStaticallyIncludedDestination(replyQWildcard);
		   return bridge;
		}
	/*
	protected NetworkConnector bridgeBrokers(String localBrokerName,
            String remoteBrokerName,
            boolean dynamicOnly,
            int networkTTL) throws Exception {
NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName, dynamicOnly, networkTTL, true);
connector.setBridgeTempDestinations(true);
connector.setAdvisoryForFailedForward(true);
connector.setDuplex(useDuplex);
connector.setAlwaysSyncSend(true);
networkConnectors.add(connector);
return connector;
}
	*/
	public ActiveBroker(String brokerName)
	{
		try
		{
			if(portAvailable(1099))
			{
				broker = new BrokerService();
				broker.setPersistent(true);
				broker.setBrokerName(brokerName);
				//NetworkConnector connector = bridge
				//connector.
				// = new NetworkConnector();
				TransportConnector connector = new TransportConnector();
				connector.setUri(new URI("tcp://localhost:32010"));
				//connector.setDiscoveryUri(new URI("multicast://default?group=test"));
				broker.addConnector(connector);
				//broker.addNetworkConnector(new URI("multicast://default?group=test"));
				//NetworkConnector bridge = broker.addNetworkConnector(new URI("static://" + remoteIP + ":32010"));
				//bridge.setUserName(userName);
				broker.start();
			}
		}
		catch(Exception ex)
		{
			System.out.println("ActiveBroker Init : " + ex.toString());	
		}
		
	}	

	public static void AddNetworkConnector(String URI)
	{
		try
		{
			NetworkConnector bridge = broker.addNetworkConnector(new URI("static://" + URI + ":32010"));
			
			//TransportConnector connector = new TransportConnector();
			//connector.setUri(new URI(URI));
			//connector.setDiscoveryUri(new URI("multicast://default?group=test"));
			//broker.addConnector(connector);
			//broker.requestRestart();
			//broker.startAllConnectors();
			//broker.startTransportConnector(connector);
			bridge.start();
		}
		catch(Exception ex)
		{
			
		}
	}
	
	public static void AddTransportConnector(String URI)
	{
		try
		{
			TransportConnector connector = new TransportConnector();
			connector.setUri(new URI(URI));
			//connector.setDiscoveryUri(new URI("multicast://default?group=test"));
			broker.addConnector(connector);
			//broker.requestRestart();
			//broker.startAllConnectors();
			broker.startTransportConnector(connector);
		}
		catch(Exception ex)
		{
			
		}
	}
	
	
	public static boolean portAvailable(int port) 
	{
		if (port < 0 || port > 65535) 
		{
			throw new IllegalArgumentException("Invalid start port: " + port);
		}

		ServerSocket ss = null;
		DatagramSocket ds = null;
		try 
		{
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} 
		catch (IOException e) 
		{
		} 
		finally 
		{
			if (ds != null) 
			{
				ds.close();
			}

			if (ss != null) 
			{
				try 
				{
					ss.close();
				} 
				catch (IOException e) 
				{
					/* should not be thrown */
				}
			}
		}

		return false;
		}

	}



