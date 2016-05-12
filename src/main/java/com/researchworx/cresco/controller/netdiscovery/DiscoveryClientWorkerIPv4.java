package com.researchworx.cresco.controller.netdiscovery;

import java.io.IOException;
import java.net.*;
import java.util.*;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

class DiscoveryClientWorkerIPv4 {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryClientWorkerIPv4.class);

    private Launcher plugin;
    private DatagramSocket c;
    private Gson gson;
    private Timer timer;
    private int discoveryTimeout;
    private String broadCastNetwork;
    private DiscoveryType disType;
    private boolean timerActive = false;
    private List<MsgEvent> discoveredList;
    private DiscoveryCrypto discoveryCrypto;

    DiscoveryClientWorkerIPv4(Launcher plugin, DiscoveryType disType, int discoveryTimeout, String broadCastNetwork) {
        this.plugin = plugin;
        gson = new Gson();
        this.discoveryTimeout = discoveryTimeout;
        this.broadCastNetwork = broadCastNetwork;
        this.disType = disType;
        discoveryCrypto = new DiscoveryCrypto();
    }

    private class StopListenerTask extends TimerTask {
        public void run() {
            try {
                logger.trace("Closing Listener...");
                //user timer to close socket
                c.close();
                timer.cancel();
                timerActive = false;
            } catch (Exception ex) {
                logger.error("StopListenerTask {}", ex.getMessage());
            }
        }
    }

    private synchronized void processIncoming(DatagramPacket packet) {
        synchronized (packet) {
            String json = new String(packet.getData()).trim();
            try {
                MsgEvent me = gson.fromJson(json, MsgEvent.class);
                if (me != null) {

                    String remoteAddress = packet.getAddress().getHostAddress();
                    if (remoteAddress.contains("%")) {
                        String[] remoteScope = remoteAddress.split("%");
                        remoteAddress = remoteScope[0];
                    }
                    logger.trace("Processing packet for {} {}_{}", remoteAddress, me.getParam("src_region"), me.getParam("src_agent"));
                    me.setParam("dst_ip", remoteAddress);
                    me.setParam("dst_region", me.getParam("src_region"));
                    me.setParam("dst_agent", me.getParam("src_agent"));

                    discoveredList.add(me);
                }
            } catch (Exception ex) {
                logger.error("DiscoveryClientWorker in loop {}", ex.getMessage());
            }
        }
    }

    List<MsgEvent> discover() {
        // Find the server using UDP broadcast
        logger.info("Discovery (IPv4) started");
        try {
            discoveredList = new ArrayList<>();

            // Broadcast the message over all the network interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
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

                        logger.trace("Creating DatagramSocket", inAddr.toString());
                        c = new DatagramSocket(null);
                        logger.trace("Setting broadcast to true on DatagramSocket", inAddr.toString());
                        c.setBroadcast(true);

                        timer = new Timer();
                        timer.schedule(new StopListenerTask(), discoveryTimeout);
                        timerActive = true;

                        MsgEvent sme = new MsgEvent(MsgEvent.Type.DISCOVER, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "Discovery request.");
                        sme.setParam("broadcast_ip", broadCastNetwork);
                        sme.setParam("src_region", this.plugin.getRegion());
                        sme.setParam("src_agent", this.plugin.getAgent());

                        //set crypto message for discovery
                        sme.setParam("discovery_secret",generateValidateMessage());


                        if (disType == DiscoveryType.AGENT || disType == DiscoveryType.REGION || disType == DiscoveryType.GLOBAL) {
                            logger.trace("Discovery Type = {}", disType.name());
                            sme.setParam("discovery_type", disType.name());
                        } else {
                            logger.trace("Discovery type unknown");
                            sme = null;
                        }
                        if (sme != null) {
                            logger.trace("Building sendPacket for {}", inAddr.toString());
                            String sendJson = gson.toJson(sme);
                            byte[] sendData = sendJson.getBytes();
                            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, Inet4Address.getByName(broadCastNetwork), 32005);
                            synchronized (c) {
                                c.send(sendPacket);
                                logger.trace("Sent sendPacket via {}", inAddr.toString());
                            }
                            while (!c.isClosed()) {
                                logger.trace("Listening", inAddr.toString());
                                try {
                                    byte[] recvBuf = new byte[15000];
                                    DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
                                    synchronized (c) {
                                        c.receive(receivePacket);
                                        logger.trace("Received packet");
                                        if (timerActive) {
                                            logger.trace("Restarting listening timer");
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
                    } catch (SocketException se) {
                        logger.error("getDiscoveryMap : SocketException {}", se.getMessage());
                    } catch (IOException ie) {
                        // Eat the exception, closing the port
                    } catch (Exception e) {
                        logger.error("getDiscoveryMap {}", e.getMessage());
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("while not closed: {}", ex.getMessage());
        }
        return discoveredList;
    }

    private String generateValidateMessage() {
        String encryptedString = null;
        try {
            String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
            String discoverySecret = plugin.getConfig().getStringParam("discovery_secret");
            encryptedString = discoveryCrypto.encrypt(verifyMessage,discoverySecret);

        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return encryptedString;
    }
}
