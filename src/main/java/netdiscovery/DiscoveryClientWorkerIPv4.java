package netdiscovery;

import java.io.IOException;
import java.net.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugincore.PluginEngine;

import com.google.gson.Gson;

import shared.MsgEvent;
import shared.MsgEventType;

class DiscoveryClientWorkerIPv4 {
	private static final Logger logger = LoggerFactory.getLogger(DiscoveryClientWorkerIPv4.class);

	private DatagramSocket c;
	private Gson gson;
	private Timer timer;
	public int discoveryTimeout;
	public String broadCastNetwork;
	public DiscoveryType disType;
	private boolean timerActive = false;
	private List<MsgEvent> discoveredList;

	DiscoveryClientWorkerIPv4(DiscoveryType disType, int discoveryTimeout, String broadCastNetwork) {
		gson = new Gson();
		//timer = new Timer();
	    //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
	    //timer.schedule(new StopListnerTask(), discoveryTimeout);
		this.discoveryTimeout = discoveryTimeout;
		this.broadCastNetwork = broadCastNetwork;
		this.disType = disType;
	}

	private class StopListenerTask extends TimerTask {
		public void run() {
			try {
				logger.trace("Closing Listener...");
				//user timer to close socket
				c.close();
				timer.cancel();
				timerActive = false;
			} catch(Exception ex) {
				logger.error("StopListenerTask {}", ex.getMessage());
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

					String remoteAddress = packet.getAddress().getHostAddress();
					if(remoteAddress.contains("%"))
					{
						String[] remoteScope = remoteAddress.split("%");
						remoteAddress = remoteScope[0];
					}
					logger.trace("Processing packet for {} {}_{}", remoteAddress, me.getParam("src_region"), me.getParam("src_agent"));
					me.setParam("dst_ip", remoteAddress);
					me.setParam("dst_region", me.getParam("src_region"));
					me.setParam("dst_agent", me.getParam("src_agent"));

					//PluginEngine.incomingCanidateBrokers.offer(me);
					discoveredList.add(me);
				}
			} catch(Exception ex) {
				logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
			}
		}
	}
	
	List<MsgEvent> discover() {
		// Find the server using UDP broadcast
		logger.info("Discovery (IPv4) started");
		DatagramSocket lastC;
		try {
			discoveredList = new ArrayList<>();
			//Open a random port to send the package
			//c = new DatagramSocket();
			//c.setBroadcast(true);

			//byte[] sendData = "DISCOVER_FUIFSERVER_REQUEST".getBytes();
			/*MsgEvent sme = new MsgEvent(MsgEventType.DISCOVER,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Discovery request.");
			sme.setParam("broadcast_ip","255.255.255.0");
			sme.setParam("src_region",PluginEngine.region);
			sme.setParam("src_agent",PluginEngine.agent);

			String sendJson = gson.toJson(sme);
			byte[] sendData = sendJson.getBytes();

			//Try the 255.255.255.255 first
			try {
				DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName("255.255.255.255"), 32005);
				c.send(sendPacket);
				//System.out.println(getClass().getName() + ">>> Request packet sent to: 255.255.255.255 (DEFAULT)");
			} catch (Exception e) {

			}*/

			// Broadcast the message over all the network interfaces
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface networkInterface = interfaces.nextElement();
				//if (networkInterface.isLoopback() || !networkInterface.isUp()) {
				if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
					//if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
					continue; // Don't want to broadcast to the loopback interface
				}
				logger.trace("Getting interfaceAddresses for interface {}", networkInterface.getDisplayName());
				for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
					try {

						if (!(interfaceAddress.getAddress() instanceof Inet4Address)) {
							continue;
						}

						logger.trace("Trying address {} for interface {}", interfaceAddress.getAddress().toString(), networkInterface.getDisplayName());

						InetAddress inAddr = interfaceAddress.getBroadcast();

						if (inAddr == null) {
							logger.trace("Not a broadcast");
							continue;
						}

						//if (!(inAddr instanceof Inet4Address)) { continue; }

						logger.trace("Creating DatagramSocket on {}", inAddr.toString());
						c = new DatagramSocket(null);
						logger.trace("Setting broadcast to true on {}", inAddr.toString());
						c.setBroadcast(true);

						/*String hostAddress = interfaceAddress.getAddress().getHostAddress();
						logger.trace("{}: Host address = {}", inAddr.toString(), hostAddress);

						SocketAddress sa = new InetSocketAddress(hostAddress, 0);

						c.bind(sa);*/

						timer = new Timer();
						timer.schedule(new StopListenerTask(), discoveryTimeout);
						timerActive = true;

						MsgEvent sme = new MsgEvent(MsgEventType.DISCOVER, PluginEngine.region, PluginEngine.agent, PluginEngine.plugin, "Discovery request.");
						sme.setParam("broadcast_ip", broadCastNetwork);
						sme.setParam("src_region", PluginEngine.region);
						sme.setParam("src_agent", PluginEngine.agent);
						if (disType == DiscoveryType.AGENT || disType == DiscoveryType.REGION || disType == DiscoveryType.GLOBAL) {
							logger.trace("Discovery Type = {}", disType.name());
							sme.setParam("discovery_type", disType.name());
						} else {
							logger.trace("Discovery type unknown");
							sme = null;
						}
						if (sme != null) {
							logger.trace("Building sendPacket on {}", inAddr.toString());
							String sendJson = gson.toJson(sme);
							byte[] sendData = sendJson.getBytes();
							DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName(broadCastNetwork), 32005);
							synchronized (c) {
								logger.trace("Sending sendPacket on {}", inAddr.toString());
								c.send(sendPacket);
								logger.trace("Sent sendPacket on {}", inAddr.toString());
							}
							while (!c.isClosed()) {
								logger.trace("Listening on {}", inAddr.toString());
								try {
									byte[] recvBuf = new byte[15000];
									DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
									synchronized (c) {
										logger.trace("Receiving packet on {}", inAddr.toString());
										c.receive(receivePacket);
										logger.trace("Received packet on {}", inAddr.toString());
										if (timerActive) {
											logger.trace("Activating timer on {}", inAddr.toString());
											timer.schedule(new StopListenerTask(), discoveryTimeout);
										}
									}
									synchronized (receivePacket) {
										processIncoming(receivePacket);
									}
								} catch (SocketException se) {
									// Eat the message, this is normal
								} catch (Exception e) {
									logger.error("discovery {}", e.getMessage());
								}
							}
						}
						/*InetAddress broadcast = interfaceAddress.getBroadcast();
						if (broadcast == null) {
							continue;
						}
						if (interfaceAddress.getAddress() instanceof Inet6Address) {
							continue;
						}
						// Send the broadcast package!
						try {
							DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, broadcast, 32005);
							c.send(sendPacket);
						} catch (Exception e) {

						}*/
					} catch (SocketException se) {
						logger.error("getDiscoveryMap : SocketException {}", se.getMessage());
					} catch (IOException ie) {
						// Eat the exception, closing the port
					} catch (Exception e) {
						logger.error("getDiscoveryMap {}", e.getMessage());
					}
					//System.out.println(getClass().getName() + ">>> Request packet sent to: " + broadcast.getHostAddress() + "; Interface: " + networkInterface.getDisplayName());
				}
			}

			//System.out.println(getClass().getName() + ">>> Done looping over all network interfaces. Now waiting for a reply!");

			// Wait for a response
			while(!c.isClosed()) {
				logger.trace("Listening...");
				try {
					byte[] recvBuf = new byte[15000];
					DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
					c.receive(receivePacket);
				} catch(SocketException ex) {
					//eat message.. this should happen
				} catch(Exception ex) {
					System.out.println("in loop 1" + ex.toString());
				}
			}
			//Close the port!
			// c.close();
			// System.out.println("CODY : Dicsicer Client Worker Engned!");
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("while not closed: {}", ex.getMessage());
			//System.out.println("while not closed: " + ex.toString());
		}
		return discoveredList;
	}

	/*public class IPv4Responder implements Runnable {

		DatagramSocket socket = null;
	    DatagramPacket packet = null;
	    String hostAddress = null;
	    Gson gson;


	    public IPv4Responder(DatagramSocket socket, DatagramPacket packet, String hostAddress) {
	        this.socket = socket;
	        this.packet = packet;
	        this.hostAddress = hostAddress;
	        gson = new Gson();
	    }

	    public void run()
	    {

	    	//We have a response
			  //System.out.println(getClass().getName() + ">>> Broadcast response from server: " + receivePacket.getAddress().getHostAddress());

			  //Check if the message is correct
			  //System.out.println(new String(receivePacket.getData()));

			  String json = new String(packet.getData()).trim();
			  //String response = "region=region0,agent=agent0,recaddr=" + packet.getAddress().getHostAddress();
		  		try
		  		{
		  			MsgEvent me = gson.fromJson(json, MsgEvent.class);
		  			if(me != null)
		  			{
		  				 //System.out.println("RESPONCE: " + me.getParamsString());

		  				 //if(!me.getParam("dst_ip").equals(receivePacket.getAddress().getHostAddress()))
		  				 //{
		  					//System.out.println("SAME HOST");
		  					//System.out.println(me.getParamsString() + receivePacket.getAddress().getHostAddress());
		  					//me.setParam("serverip", receivePacket.getAddress().getHostAddress());
		  					me.setParam("dst_ip", packet.getAddress().getHostAddress());
		  					me.setParam("dst_region", me.getParam("src_region"));
		        		    me.setParam("dst_agent", me.getParam("src_agent"));
		        		    PluginEngine.incomingCanidateBrokers.offer(me);
		  					//discoveryList.add(me);
		  					//System.out.println(getClass().getName() + ">>> Added server: " + receivePacket.getAddress().getHostAddress() + " to broker list");

		  			}
		  		}
		  		catch(Exception ex)
		  		{
		  			System.out.println("in loop 0" + ex.toString());
		  		}


	    }
	}*/
}
