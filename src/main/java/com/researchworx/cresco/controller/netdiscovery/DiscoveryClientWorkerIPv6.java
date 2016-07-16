package com.researchworx.cresco.controller.netdiscovery;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.IOException;
import java.net.*;
import java.util.*;

public class DiscoveryClientWorkerIPv6 {
    private Launcher plugin;
    private CLogger logger;
    private DatagramSocket c;
    //private MulticastSocket c;
    private Gson gson;
    public Timer timer;
    public int discoveryTimeout;
    public String multiCastNetwork;
    public DiscoveryType disType;
    private boolean timerActive = false;
    private List<MsgEvent> discoveredList;
    private DiscoveryCrypto discoveryCrypto;


    public DiscoveryClientWorkerIPv6(Launcher plugin, DiscoveryType disType, int discoveryTimeout, String multiCastNetwork) {
        this.logger = new CLogger(DiscoveryClientWorkerIPv6.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
        this.plugin = plugin;
        gson = new Gson();
        //timer = new Timer();
        //timer.scheduleAtFixedRate(new StopListnerTask(), 1000, discoveryTimeout);
        this.discoveryTimeout = discoveryTimeout;
        this.multiCastNetwork = multiCastNetwork;
        this.disType = disType;
        discoveryCrypto = new DiscoveryCrypto(plugin);
    }

    private class StopListnerTask extends TimerTask {
        public void run() {
            try {
                //user timer to close socket
                c.close();
                timer.cancel();
                timerActive = false;

            } catch (Exception ex) {
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
                if (me != null) {

                    String remoteAddress = packet.getAddress().getHostAddress();
                    if (remoteAddress.contains("%")) {
                        String[] remoteScope = remoteAddress.split("%");
                        remoteAddress = remoteScope[0];
                    }
                    me.setParam("dst_ip", remoteAddress);
                    me.setParam("dst_region", me.getParam("src_region"));
                    me.setParam("dst_agent", me.getParam("src_agent"));
                    me.setParam("validated_authenication",ValidatedAuthenication(me));

                    //PluginEngine.incomingCanidateBrokers.offer(me);
                    discoveredList.add(me);
                }
            } catch (Exception ex) {
                logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
            }
        }
    }


    List<MsgEvent> discover() {
        // Find the server using UDP broadcast
        logger.info("Discovery (IPv6) started");
        try {
            discoveredList = new ArrayList<>();
            // Broadcast the message over all the network interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();

                //if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                    //if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                    continue; // Don't want to broadcast to the loopback interface
                }

                if (networkInterface.supportsMulticast()) {
                    for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                        try {
                            //if((interfaceAddress.getAddress() instanceof Inet6Address) && !interfaceAddress.getAddress().isLinkLocalAddress())
                            InetAddress inAddr = interfaceAddress.getAddress();
                            boolean isGlobal = !inAddr.isSiteLocalAddress() && !inAddr.isLinkLocalAddress();

                            if ((inAddr instanceof Inet6Address) && isGlobal) {
                                //if(inAddr instanceof Inet6Address)

                                //c = new MulticastSocket(null);
                                c = new DatagramSocket(null);
                                //c.setReuseAddress(true);
                                //System.out.println("prebind1");
                                String hostAddress = interfaceAddress.getAddress().getHostAddress();
                                if (hostAddress.contains("%")) {
                                    String[] hostScope = hostAddress.split("%");
                                    hostAddress = hostScope[0];
                                }
                                SocketAddress sa = new InetSocketAddress(hostAddress, 0);
                                //System.out.println("prebind2");

                                c.bind(sa);
                                //System.out.println("prebind3");

                                //start timer to clost discovery
                                timer = new Timer();
                                timer.schedule(new StopListnerTask(), discoveryTimeout);
                                timerActive = true;
                                MsgEvent sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "Discovery request.");
                                sme.setParam("broadcast_ip", multiCastNetwork);
                                sme.setParam("src_region", this.plugin.getRegion());
                                sme.setParam("src_agent", this.plugin.getAgent());

                                //set crypto message for discovery

                                if (disType == DiscoveryType.AGENT) {
                                    sme.setParam("discovery_type", DiscoveryType.AGENT.name());
                                } else if (disType == DiscoveryType.REGION) {
                                    sme.setParam("discovery_type", DiscoveryType.REGION.name());
                                } else if (disType == DiscoveryType.GLOBAL) {
                                    sme.setParam("discovery_type", DiscoveryType.GLOBAL.name());
                                } else {
                                    sme = null;
                                }

                                sme.setParam("discovery_validator",generateValidateMessage(sme));

                                //me.setParam("clientip", packet.getAddress().getHostAddress());

                                //convert java object to JSON format,
                                // and returned as JSON formatted string
                                if (sme != null) {
                                    String sendJson = gson.toJson(sme);

                                    byte[] sendData = sendJson.getBytes();

                                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet6Address.getByName(multiCastNetwork), 32005);
                                    synchronized (c) {
                                        c.send(sendPacket);
                                    }
                                    //System.out.println(getClass().getName() + ">>> Request packet sent to: " + multiCastNetwork +  ": from : " + interfaceAddress.getAddress().getHostAddress());

                                    while (!c.isClosed()) {
                                        try {
                                            byte[] recvBuf = new byte[15000];
                                            DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
                                            synchronized (c) {
                                                c.receive(receivePacket);
                                                if (timerActive) {
                                                    //timer.cancel(); //reset timer on new discovery packet
                                                    timer.schedule(new StopListnerTask(), discoveryTimeout);
                                                }
                                            }
                                            synchronized (receivePacket) {
                                                processIncoming(receivePacket);
                                            }
                                            //new Thread(new IPv6Responder(c,receivePacket,hostAddress)).start();
                                        } catch (SocketException ex) {
                                            //eat message.. this should happen
                                            //System.out.println(ex.getMessage());
                                        } catch (Exception ex) {
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
        return discoveredList;
    }

    private String ValidatedAuthenication(MsgEvent rme) {
        String decryptedString = null;
        try {

            String discoverySecret = null;
            if (rme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
            } else if (rme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
            } else if (rme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
            }
            if(rme.getParam("validated_authenication") != null) {
                decryptedString = discoveryCrypto.decrypt(rme.getParam("validated_authenication"), discoverySecret);
            }
            else {
                logger.error("[validated_authenication] record not found!");
                logger.error(rme.getParams().toString());
            }

        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return decryptedString;
    }

    private String generateValidateMessage(MsgEvent sme) {
        String encryptedString = null;
        try {

            String discoverySecret = null;
            if (sme.getParam("discovery_type").equals(DiscoveryType.AGENT.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
            } else if (sme.getParam("discovery_type").equals(DiscoveryType.REGION.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
            } else if (sme.getParam("discovery_type").equals(DiscoveryType.GLOBAL.name())) {
                discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
            }

            String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
            encryptedString = discoveryCrypto.encrypt(verifyMessage,discoverySecret);

        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return encryptedString;
    }
}
