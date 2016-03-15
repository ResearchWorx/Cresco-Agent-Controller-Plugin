package netdiscovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;

public class DiscoveryClientWorkerIPv6  {
	private DatagramSocket c;
	//private MulticastSocket c;
	private Gson gson;
	public Timer timer;
	public int discoveryTimeout;
	public String multiCastNetwork;
	public DiscoveryType disType;
	
	public static final Logger logger = LoggerFactory.getLogger(DiscoveryClientWorkerIPv6.class);

	public DiscoveryClientWorkerIPv6(DiscoveryType disType, int discoveryTimeout, String multiCastNetwork) {
		gson = new Gson();
		//timer = new Timer();
	    //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
		this.discoveryTimeout = discoveryTimeout;
		this.multiCastNetwork = multiCastNetwork;
		this.disType = disType;
	}
	
	public class IPv6Responder implements Runnable {
		DatagramSocket socket = null;
	    DatagramPacket packet = null;
	    String hostAddress = null;
	    Gson gson;

	    public IPv6Responder(DatagramSocket socket, DatagramPacket packet, String hostAddress) {
	        this.socket = socket;
	        this.packet = packet;
	        this.hostAddress = hostAddress;
	        gson = new Gson();
	    }

	    public void run() {
			//byte[] data = makeResponse(); // code not shown

			//We have a response
			//System.out.println(getClass().getName() + ">>> Broadcast response from server: " + packet.getAddress().getHostAddress());

			//Check if the message is correct
			//System.out.println(new String(receivePacket.getData()));

			String json = new String(packet.getData()).trim();
			//String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
			try {
				MsgEvent me = gson.fromJson(json, MsgEvent.class);
				if(me != null) {
					//System.out.println("RESPONCE: " + me.getParamsString());

					String remoteAddress = packet.getAddress().getHostAddress();
					if(remoteAddress.contains("%")) {
						String[] remoteScope = hostAddress.split("%");
						remoteAddress = remoteScope[0];
					}
					me.setParam("dst_ip", remoteAddress);
					me.setParam("dst_region", me.getParam("src_region"));
					me.setParam("dst_agent", me.getParam("src_agent"));
					PluginEngine.incomingCanidateBrokers.offer(me);
				}
			} catch(Exception ex) {
				logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
			}
	    }
	}
	
	class StopListnerTask extends TimerTask {
	    public void run() {
	    	try
	    	{
	    	//user timer to close socket
	    	c.close();
	    	timer.cancel();
	    	logger.debug("StopListnerTask {}", "Boom STOPPED");
    		
	    	}
	    	catch(Exception ex)
	    	{
	    		logger.error("stopped {}", timer.purge());
	    		
	    		logger.error("StopListnerTask {}", ex.getMessage());
	    		
	    	}
	    }
	}

	private synchronized void processIncoming(DatagramPacket packet) {
		synchronized (packet) {
			//byte[] data = makeResponse(); // code not shown

			//We have a response
			//System.out.println(getClass().getName() + ">>> Broadcast response from server: " + packet.getAddress().getHostAddress());

			//Check if the message is correct
			//System.out.println(new String(receivePacket.getData()));

			String json = new String(packet.getData()).trim();
			//String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
  			try {
				MsgEvent me = gson.fromJson(json, MsgEvent.class);
				if(me != null)
				{
					//System.out.println("RESPONCE: " + me.getParamsString());

					 String remoteAddress = packet.getAddress().getHostAddress();
					 if(remoteAddress.contains("%"))
					 {
						 String[] remoteScope = remoteAddress.split("%");
						 remoteAddress = remoteScope[0];
					 }
						me.setParam("dst_ip", remoteAddress);
						me.setParam("dst_region", me.getParam("src_region"));
						me.setParam("dst_agent", me.getParam("src_agent"));
						PluginEngine.incomingCanidateBrokers.offer(me);
				}
			} catch(Exception ex) {
				logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
			}
		}
	}


	public void discover() {
		// Find the server using UDP broadcast
		logger.debug("Start Discovery...");
		try {
			// Broadcast the message over all the network interfaces
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface networkInterface = interfaces.nextElement();

				//if (networkInterface.isLoopback() || !networkInterface.isUp()) {
				if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
				//if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
					continue; // Don't want to broadcast to the loopback interface
				}

				if(networkInterface.supportsMulticast()) {
					for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
						try {
							//if((interfaceAddress.getAddress() instanceof Inet6Address) && !interfaceAddress.getAddress().isLinkLocalAddress())
					  		InetAddress inAddr = interfaceAddress.getAddress();
					  		boolean isGlobal = !inAddr.isSiteLocalAddress() && !inAddr.isLinkLocalAddress();

					  		if((inAddr instanceof Inet6Address) && isGlobal) {
								//if(inAddr instanceof Inet6Address)

								//c = new MulticastSocket(null);
						 		c = new DatagramSocket(null);
						 		//c.setReuseAddress(true);
						  		//System.out.println("prebind1");
						 		String hostAddress = interfaceAddress.getAddress().getHostAddress();
						 		if(hostAddress.contains("%")) {
									String[] hostScope = hostAddress.split("%");
									hostAddress = hostScope[0];
						 		}
								SocketAddress sa = new InetSocketAddress(hostAddress,0);
						 		//System.out.println("prebind2");

								c.bind(sa);
								//System.out.println("prebind3");

								//start timer to clost discovery
								timer = new Timer();
								timer.schedule(new StopListnerTask(), discoveryTimeout);
								MsgEvent sme = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Discovery request.");
								sme.setParam("broadcast_ip",multiCastNetwork);
								sme.setParam("src_region",PluginEngine.region);
								sme.setParam("src_agent",PluginEngine.agent);
								if(disType == DiscoveryType.AGENT)
								{
									sme.setParam("discovery_type",DiscoveryType.AGENT.name());
								}
								else if(disType == DiscoveryType.REGION)
								{
									sme.setParam("discovery_type",DiscoveryType.REGION.name());
								}
								else
								{
									sme = null;
								}
								//me.setParam("clientip", packet.getAddress().getHostAddress());

								//convert java object to JSON format,
								// and returned as JSON formatted string
								if(sme != null)
								{
						 		String sendJson = gson.toJson(sme);

								byte[] sendData = sendJson.getBytes();

								DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName(multiCastNetwork), 32005);
								synchronized (c) {
									c.send(sendPacket);
								}
								//System.out.println(getClass().getName() + ">>> Request packet sent to: " + multiCastNetwork +  ": from : " + interfaceAddress.getAddress().getHostAddress());

								while(!c.isClosed()) {
									try {
										byte[] recvBuf = new byte[15000];
										DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
										synchronized (c) {
											c.receive(receivePacket);
											int cancelCount = timer.purge();
											logger.debug("discovery {}", "cacnel count : " + cancelCount);
											if(cancelCount == 0)
											{
												//timer.cancel(); //reset timer on new discovery packet
												timer.schedule(new StopListnerTask(), discoveryTimeout);
											}
										}
										synchronized (receivePacket) {
											processIncoming(receivePacket);
										}
										//new Thread(new IPv6Responder(c,receivePacket,hostAddress)).start();
									} catch(SocketException ex) {
										//eat message.. this should happen
										//System.out.println(ex.getMessage());
									} catch(Exception ex) {
										logger.error("discovery {}", ex.getMessage());
									}
								}
					  			//Close the port!
								}
								
					  		}
				  		} catch (IOException ie) {
							//eat exception we are closing port
							//System.out.println("DiscoveryClientWorkerIPv6 : getDiscoveryMap IO Error : " + ie.getMessage());
				  		} catch (Exception e) {
					  		logger.error("getDiscoveryMap {}", e.getMessage());
				  		}
					}
				}
				//Wait for a response

				//c.close();
				//System.out.println("CODY : Dicsicer Client Worker Engned!");
			}
		} catch (Exception ex) {
		  logger.error("while not closed: " + ex.getMessage());
		}
	}
}
