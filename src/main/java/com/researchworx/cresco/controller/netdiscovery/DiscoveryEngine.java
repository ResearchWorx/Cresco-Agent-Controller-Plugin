package com.researchworx.cresco.controller.netdiscovery;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class DiscoveryEngine implements Runnable {
    private Launcher plugin;
    private static Map<NetworkInterface, MulticastSocket> workers = new ConcurrentHashMap<>();
    private DiscoveryCrypto discoveryCrypto;
    private Gson gson;


    private static final Logger logger = LoggerFactory.getLogger(DiscoveryEngine.class);

    public DiscoveryEngine(Launcher plugin) {
        logger.trace("Discovery Engine initialized");
        this.plugin = plugin;
        discoveryCrypto = new DiscoveryCrypto();
        gson = new Gson();

    }

    public static void shutdown() {
        for (Map.Entry<NetworkInterface, MulticastSocket> entry : workers.entrySet()) {
            entry.getValue().close();
        }
    }

    public void run() {
        logger.info("Discovery Engine started");
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                Thread thread = new Thread(new DiscoveryEngineWorker(networkInterface, plugin));
                thread.start();
            }
            this.plugin.setDiscoveryActive(true);
            logger.trace("Discovery Engine has shutdown");
        } catch (Exception ex) {
            logger.error("Run {}", ex.getMessage());
        }
    }

    class DiscoveryEngineWorker implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(DiscoveryEngineWorker.class);


        private NetworkInterface networkInterface;
        private MulticastSocket socket;
        private Launcher plugin;

        public DiscoveryEngineWorker(NetworkInterface networkInterface, Launcher plugin) {
            this.networkInterface = networkInterface;
            this.plugin = plugin;
        }

        public void shutdown() {
            socket.close();
        }

        public void run() {
            logger.debug("Creating worker [{}]", networkInterface.getDisplayName());
            try {
                if (!networkInterface.getDisplayName().startsWith("veth") && !networkInterface.isLoopback() && networkInterface.supportsMulticast() && !networkInterface.isPointToPoint() && !networkInterface.isVirtual()) {
                    logger.trace("Discovery Engine Worker [" + networkInterface.getDisplayName() + "] initialized");
                    //logger.trace("Init [{}]", networkInterface.getDisplayName());
                    SocketAddress sa;
                    if (plugin.isIPv6()) {
                        sa = new InetSocketAddress("[::]", 32005);
                    } else {
                        sa = new InetSocketAddress("0.0.0.0", 32005);
                    }
                    socket = new MulticastSocket(null);
                    socket.bind(sa);
                    workers.put(networkInterface, socket);
                    //ditry hack for possible race condition
                    while(!workers.containsKey(networkInterface)) {
                        Thread.sleep(1000);
                    }
                    logger.trace("Bound to interface [{}] address [::]", networkInterface.getDisplayName());

                    if (plugin.isIPv6()) {
                        //find to network and site multicast addresses
                        //SocketAddress saj = new InetSocketAddress(Inet6Address.getByName("ff05::1:c"),32005);
                        //socket.joinGroup(saj, networkInterface);
                        SocketAddress saj2 = new InetSocketAddress(Inet6Address.getByName("ff02::1:c"), 32005);
                        socket.joinGroup(saj2, networkInterface);
                    }

                    while (!workers.get(networkInterface).isClosed()) {
                        //System.out.println(getClass().getName() + ">>>Ready to receive broadcast packets!");

                        //Receive a packet
                        byte[] recvBuf = new byte[15000];
                        DatagramPacket recPacket = new DatagramPacket(recvBuf, recvBuf.length);

                        try {
                            synchronized (socket) {
                                socket.receive(recPacket); //rec broadcast packet, could be IPv6 or IPv4
                                logger.trace("Received Discovery packet!");
                            }

                            synchronized (socket) {
                                DatagramPacket sendPacket = sendPacket(recPacket);
                                if (sendPacket != null) {
                                    socket.send(sendPacket);
                                    logger.trace("Sent Discovery packet!");
                                    plugin.responds.incrementAndGet();
                                }
                                else {
                                    logger.trace("Discovery Return Packet = Null");
                                }

                            }
                        } catch (IOException e) {
                            logger.trace("Socket closed");
                        }
                    }
                    logger.trace("Discovery Engine Worker [" + networkInterface.getDisplayName() + "] has shutdown");
                }
            } catch (Exception ex) {
                logger.error("Run : Interface = {} : Error = {}", networkInterface.getDisplayName(), ex.getMessage());
                ex.printStackTrace();
            }
        }

        private String stripIPv6Address(InetAddress address) {
            String sAddress = null;
            try {
                sAddress = address.getHostAddress();
                if (sAddress.contains("%")) {
                    String[] aScope = sAddress.split("%");
                    sAddress = aScope[0];
                }
            } catch (Exception ex) {
                logger.error("stripIPv6Address {}", ex.getMessage());
            }
            return sAddress;
        }

        private String getSourceAddress(Map<String, String> intAddr, String remoteAddress) {
            String sAddress = null;
            try {
                for (Map.Entry<String, String> entry : intAddr.entrySet()) {
                    String cdirAddress = entry.getKey() + "/" + entry.getValue();
                    logger.trace("getSourceAddress : cdirAddress: " + cdirAddress + " remoteAddress: " + remoteAddress);
                    CIDRUtils cutil = new CIDRUtils(cdirAddress);
                    if (cutil.isInRange(remoteAddress)) {
                        sAddress = entry.getKey();
                        logger.trace("Found address in range : cdirAddress: " + cdirAddress + "== remoteAddress: " + remoteAddress);
                    }
                }
            } catch (Exception ex) {
                logger.error("getSourceAddress {}", ex.getMessage());
            }
            return sAddress;
        }

        private Map<String, String> getInterfaceAddresses() {
            Map<String, String> intAddr = null;
            try {
                intAddr = new HashMap<>();
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    Short aPrefix = interfaceAddress.getNetworkPrefixLength();
                    String hostAddress = stripIPv6Address(interfaceAddress.getAddress());
                    intAddr.put(hostAddress, aPrefix.toString());
                }
            } catch (Exception ex) {
                logger.error("stripIPv6Address {}", ex.getMessage());
            }
            return intAddr;
        }

        private synchronized DatagramPacket sendPacket(DatagramPacket packet) {
            synchronized (packet) {
                logger.trace("sendpacket 0");
                Map<String, String> intAddr = getInterfaceAddresses();
                InetAddress returnAddr = packet.getAddress();
                logger.trace("sendpacket returnAddr: " + returnAddr.getHostName());
                String remoteAddress = stripIPv6Address(returnAddr);
                logger.trace("sendpacket returnAddr-stripped: " + remoteAddress);

                boolean isGlobal = true;
                if (returnAddr instanceof Inet6Address)
                    isGlobal = !returnAddr.isSiteLocalAddress() && !returnAddr.isLinkLocalAddress();
                logger.trace("sendpacket 1");
                //logger.trace("Discovery packet from " + remoteAddress + " to " + sourceAddress);

                String sourceAddress = getSourceAddress(intAddr, remoteAddress); //determine send address


                //if (!(intAddr.containsKey(remoteAddress)) && (isGlobal) && (sourceAddress != null)) {
                if (!(intAddr.containsKey(remoteAddress)) && (isGlobal)) {
                        //Packet received
                    //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
                    //System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

                    //See if the packet holds the right command (message)
                    String message = new String(packet.getData()).trim();
                    logger.trace("sendpacket 2");

                    MsgEvent rme = null;

                    try {
                        //System.out.println(getClass().getName() + ">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
                        //check that the message can be marshaled into a MsgEvent
                        //System.out.println(getClass().getName() + "0.0 " + Thread.currentThread().getId());
                        try {
                            rme = gson.fromJson(message, MsgEvent.class);
                        } catch (Exception ex) {
                            logger.error(getClass().getName() + " failed to marshal discovery {}" + ex.getMessage());
                        }
                        //System.out.println(getClass().getName() + "0.1 " + Thread.currentThread().getId());
                        if (rme != null) {
                            //check for static discovery
                            //&& (sourceAddress != null)
                            logger.trace("Static Discovery Status = " + rme.getParam("discovery_static_agent"));
                            if ((sourceAddress != null) || (rme.getParam("discovery_static_agent") != null)) {
                            //if (sourceAddress != null) {

                                rme.setParam("src_ip", remoteAddress);
                                rme.setParam("src_port", String.valueOf(packet.getPort()));

                                MsgEvent me = null;
                                if (rme.getParam("discovery_type") != null) {
                                    if (rme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                                        logger.debug("{}", "agent discovery");
                                        me = getAgentMsg(rme); //generate payload
                                    } else if (rme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                                        logger.debug("{}", "regional discovery");
                                        me = getRegionMsg(rme);
                                    } else if (rme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                                        logger.debug("{}", "regional global");
                                    }
                                }


                                if (me != null) {

                                    String json = gson.toJson(me);
                                    logger.trace(me.getParams().toString());
                                    byte[] sendData = json.getBytes();
                                    //returnAddr = InetAddress.getByName(me.getParam("dst_ip"));
                                    int returnPort = Integer.parseInt(me.getParam("dst_port"));
                                    logger.debug("returnAddr: " + remoteAddress + " returnPort: " + returnPort);
                                    //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, returnAddr, returnPort);
                                    packet.setData(sendData);
                                    packet.setLength(sendData.length);
                                    packet.setAddress(returnAddr);
                                    packet.setPort(returnPort);
                                    logger.trace("sendpacket 3");

                                } else {
                                    packet = null; //make sure packet is null
                                    logger.trace("sendpacket 4e");

                                }
                            } else {
                                packet = null;
                            }
                        } else {
                            packet = null;
                        }
                    } catch (Exception ex) {
                        logger.error("{}", ex.getMessage());
                        packet = null;
                    }
                } else {
                    packet = null;
                }
                logger.trace("sendpacket 5");

                return packet;
            }
        }

        private MsgEvent getAgentMsg(MsgEvent rme) {
            MsgEvent me = null;
            try {

                //determine if we should respond to request
                //String validateMsgEvent(rme)
                 //       validatedAuthenication
                if (plugin.reachableAgents().size() < 25)  {

                    String validatedAuthenication = validateMsgEvent(rme); //create auth string

                    if ((rme.getParam("src_region") != null) && (validatedAuthenication != null)) {
                        if (rme.getParam("src_region").equals("init")) {
                            //System.out.println(getClass().getName() + "1 " + Thread.currentThread().getId());
                            me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                            me.setParam("dst_region", plugin.getRegion());
                            me.setParam("dst_agent", rme.getParam("src_agent"));
                            me.setParam("src_region", plugin.getRegion());
                            me.setParam("src_agent", plugin.getAgent());
                            me.setParam("dst_ip", rme.getParam("src_ip"));
                            me.setParam("dst_port", rme.getParam("src_port"));
                            me.setParam("agent_count", String.valueOf(plugin.reachableAgents().size()));
                            me.setParam("discovery_type", DiscoveryType.AGENT.name());
                            me.setParam("validated_authenication",validatedAuthenication);
                            logger.debug("getAgentMsg = " + me.getParams().toString());
                        }
                    } else {
                        logger.error("src_region=" + rme.getParam("src_region") + " validatedAuthenication=" + validatedAuthenication);
                        if ((rme.getParam("src_region").equals(plugin.getRegion())) && plugin.isRegionalController()) {
                            logger.error("{}", "!reconnect attempt!");
                        }
                    }
                }
                else {
                    logger.debug("Agent count too hight.. not responding to discovery");
                }

            } catch (Exception ex) {
                logger.error("{}", ex.getMessage());
            }
            return me;
        }

        private MsgEvent getRegionMsg(MsgEvent rme) {
            MsgEvent me = null;
            try {
                if (plugin.isRegionalController()) {

                    String validatedAuthenication = validateMsgEvent(rme); //create auth string

                    //System.out.println(getClass().getName() + "1 " + Thread.currentThread().getId());
                    me = new MsgEvent(MsgEvent.Type.DISCOVER, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "Broadcast discovery response.");
                    me.setParam("dst_region", plugin.getRegion());
                    me.setParam("dst_agent", rme.getParam("src_agent"));
                    me.setParam("src_region", plugin.getRegion());
                    me.setParam("src_agent", plugin.getAgent());
                    me.setParam("dst_ip", rme.getParam("src_ip"));
                    me.setParam("dst_port", rme.getParam("src_port"));
                    me.setParam("agent_count", String.valueOf(plugin.reachableAgents().size()));
                    me.setParam("discovery_type", DiscoveryType.REGION.name());
                    me.setParam("validated_authenication",validatedAuthenication);
                }

            } catch (Exception ex) {
                logger.error("{}", ex.getMessage());
            }
            return me;
        }

        private String validateMsgEvent(MsgEvent rme) {
            String validatedAuthenication = null;
            String groupName = null;
            try {
                String discoverySecret = null;
                if (rme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                    discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
                    groupName = "agent";
                } else if (rme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                    discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
                    groupName = "region";
                } else if (rme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                    discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
                    groupName = "global";
                }

                String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
                String discoveryValidator = rme.getParam("discovery_validator");
                String decryptedString = discoveryCrypto.decrypt(discoveryValidator,discoverySecret);
                if(decryptedString.equals(verifyMessage)) {
                    //plugin.brokerUserNameAgent
                    //isValidated = true;
                    //String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
                    //encryptedString = discoveryCrypto.encrypt(verifyMessage,discoverySecret);
                    validatedAuthenication = discoveryCrypto.encrypt(plugin.brokerUserNameAgent + "," + plugin.brokerPasswordAgent + "," + groupName,discoverySecret);
                }
            }
            catch(Exception ex) {
                logger.error(ex.getMessage());
            }

            return validatedAuthenication ;
        }

    }

}