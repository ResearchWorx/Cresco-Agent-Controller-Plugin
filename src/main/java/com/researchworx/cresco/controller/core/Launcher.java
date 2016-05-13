package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.controller.communication.*;
import com.researchworx.cresco.controller.netdiscovery.*;
import com.researchworx.cresco.controller.regionalcontroller.AgentDiscovery;
import com.researchworx.cresco.controller.regionalcontroller.ControllerDB;
import com.researchworx.cresco.controller.regionalcontroller.GlobalControllerChannel;
import com.researchworx.cresco.controller.regionalcontroller.HealthWatcher;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Launcher extends CPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(CPlugin.class);

    private String agentpath;

    private ExecutorService msgInProcessQueue;

    //regional
    private ControllerDB gdb;

    private boolean clientDiscoveryActiveIPv4 = false;
    private boolean clientDiscoveryActiveIPv6 = false;
    private boolean DiscoveryActive = false;

    private boolean ActiveBrokerManagerActive = false;
    private boolean ActiveDestManagerActive = false;

    private boolean ConsumerThreadActive = false;
    private boolean ConsumerThreadRegionActive = false;

    public boolean isRestartOnShutdown() {
        return restartOnShutdown;
    }

    public void setRestartOnShutdown(boolean restartOnShutdown) {
        this.restartOnShutdown = restartOnShutdown;
    }

    private boolean restartOnShutdown = false;

    private Thread discoveryEngineThread;

    public Thread getActiveBrokerManagerThread() {
        return activeBrokerManagerThread;
    }

    public void setActiveBrokerManagerThread(Thread activeBrokerManagerThread) {
        this.activeBrokerManagerThread = activeBrokerManagerThread;
    }

    private Thread activeBrokerManagerThread;

    public Thread getConsumerRegionThread() {
        return consumerRegionThread;
    }

    public void setConsumerRegionThread(Thread consumerRegionThread) {
        this.consumerRegionThread = consumerRegionThread;
    }

    private Thread consumerRegionThread;

    public Thread getConsumerAgentThread() {
        return consumerAgentThread;
    }

    public void setConsumerAgentThread(Thread consumerAgentThread) {
        this.consumerAgentThread = consumerAgentThread;
    }

    private Thread consumerAgentThread;
    private Thread shutdownHook;

    private ActiveProducer ap;

    private String brokerAddressAgent;
    public String brokerUserNameAgent;
    public String brokerPasswordAgent;


    private boolean isRegionalController = false;
    private Map<String, Long> discoveryMap;
    private AgentDiscovery agentDiscover;

    private boolean hasGlobalController = false;
    private GlobalControllerChannel globalControllerChannel;

    private boolean isIPv6 = false;
    private boolean isActive = false;

    public AtomicInteger responds = new AtomicInteger(0);

    private ControllerConfig controllerConfig;

    private ConcurrentHashMap<String, BrokeredAgent> brokeredAgents;

    private ConcurrentLinkedQueue<MsgEvent> incomingCanidateBrokers;
    private ConcurrentLinkedQueue<MsgEvent> outgoingMessages;

    private DiscoveryClientIPv4 dcv4;
    private DiscoveryClientIPv6 dcv6;

    private ActiveBroker broker;

    private HealthWatcher healthWatcher;

    public Launcher() {
        this.msgInProcessQueue = Executors.newFixedThreadPool(4);
        this.rpcMap = new ConcurrentHashMap<>();
    }

    public void setExecutor() {
        this.setExec(new com.researchworx.cresco.controller.core.Executor(this));
    }

    public void start() {
        this.config = new ControllerConfig(this.config.getConfig());
    }

    @Override
    public void msgIn(MsgEvent msg) {
        this.msgInProcessQueue.submit(new MsgRoute(this, msg));
    }

    @Override
    public void cleanUp() {
        closeCommunications();
    }

    public void closeCommunications() {
        try {
            if (this.restartOnShutdown)
                LOG.info("Tearing down services");
            else
                LOG.info("Shutting down");
            this.DiscoveryActive = false;
            this.ConsumerThreadRegionActive = false;
            this.ConsumerThreadActive = false;
            this.ActiveBrokerManagerActive = false;
            if (this.discoveryEngineThread != null) {
                LOG.trace("Discovery Engine shutting down");
                DiscoveryEngine.shutdown();
                this.discoveryEngineThread.join();
                this.discoveryEngineThread = null;
                this.isActive = false;
            }
            if (this.watchDog != null) {
                this.watchDog.stop();
                this.watchDog = null;
            }
            if (this.healthWatcher != null) {
                this.healthWatcher.timer.cancel();
                this.healthWatcher = null;
            }
            if (this.consumerRegionThread != null) {
                LOG.trace("Region Consumer shutting down");
                this.consumerRegionThread.join();
                this.consumerRegionThread = null;
            }
            if (this.consumerAgentThread != null) {
                LOG.trace("Agent Consumer shutting down");
                this.consumerAgentThread.join();
                this.consumerAgentThread = null;
            }
            if (this.activeBrokerManagerThread != null) {
                LOG.trace("Active Broker Manager shutting down");
                this.activeBrokerManagerThread.join();
                this.activeBrokerManagerThread = null;
            }
            if (this.ap != null) {
                LOG.trace("Producer shutting down");
                this.ap.shutdown();
                this.ap = null;
            }
            if (this.broker != null) {
                LOG.trace("Broker shutting down");
                this.broker.stopBroker();
                this.broker = null;
            }
            if (this.restartOnShutdown) {
                MsgEvent ce = new MsgEvent(MsgEvent.Type.CONFIG, this.region, this.agent, null, "comminit");
                ce.setParam("configtype","comminit");
                ce.setParam("src_region", this.region);
                ce.setParam("src_agent", this.agent);
                ce.setParam("src_plugin", this.pluginID);
                ce.setParam("dst_region", this.region);
                ce.setParam("dst_agent", this.agent);
                commInit(); //reinit everything
                //notify agent of change
                ce.setParam("set_region", this.region);
                ce.setParam("set_agent", this.agent);
                ce.setParam("is_regional_controller", Boolean.toString(this.isRegionalController));
                ce.setParam("is_active", Boolean.toString(this.isActive));
                //PluginEngine.msgInQueue.offer(ce);
                this.sendMsgEvent(ce);
                this.restartOnShutdown = false;
            }
        } catch (Exception ex) {
            LOG.error("shutdown {}", ex.getMessage());
        }
    }

    public ControllerConfig getControllerConfig() {
        return this.controllerConfig;
    }

    public GlobalControllerChannel getGlobalControllerChannel() {
        return this.globalControllerChannel;
    }

    public void discover(MsgEvent msg) {
        this.agentDiscover.discover(msg);
    }

    public void sendAPMessage(MsgEvent msg) {
        if (this.ap == null) {
            System.out.println("AP is null");
            return;
        }
        this.ap.sendMessage(msg);
    }

    public void commInit() {
        LOG.info("Initializing services");
        setActive(true);
        try {
            this.brokeredAgents = new ConcurrentHashMap<>();
            this.incomingCanidateBrokers = new ConcurrentLinkedQueue<>();
            this.outgoingMessages = new ConcurrentLinkedQueue<>();
            this.brokerAddressAgent = null;
            this.isIPv6 = isIPv6();

            List<MsgEvent> discoveryList = new ArrayList<>();

            this.dcv4 = new DiscoveryClientIPv4(this);
            this.dcv6 = new DiscoveryClientIPv6(this);

            if (this.isIPv6) {
                LOG.debug("Broker Search (IPv6)...");
                discoveryList = this.dcv6.getDiscoveryResponse(DiscoveryType.AGENT, 2000);
                LOG.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            LOG.debug("Broker Search (IPv4)...");
            discoveryList.addAll(this.dcv4.getDiscoveryResponse(DiscoveryType.AGENT, 2000));
            LOG.debug("Broker count = {}" + discoveryList.size());

            if (discoveryList.isEmpty()) {
                //generate regional ident if not assigned
                //String oldRegion = region; //keep old region if assigned

                if ((this.region.equals("init")) && (this.agent.equals("init"))) {
                    region = "region-" + java.util.UUID.randomUUID().toString();
                    agent = "agent-" + java.util.UUID.randomUUID().toString();
                }
                LOG.debug("Generated regionid=" + this.region);
                this.agentpath = this.region + "_" + this.agent;
                LOG.debug("AgentPath=" + this.agentpath);
                //Start controller services

                //discovery engine
                this.discoveryEngineThread = new Thread(new DiscoveryEngine(this));
                this.discoveryEngineThread.start();
                while (!this.DiscoveryActive) {
                    Thread.sleep(1000);
                }
                //logger.debug("IPv6 DiscoveryEngine Started..");

                LOG.debug("Broker starting");
                brokerUserNameAgent = java.util.UUID.randomUUID().toString();
                brokerPasswordAgent = java.util.UUID.randomUUID().toString();
                this.broker = new ActiveBroker(this.agentpath,brokerUserNameAgent,brokerPasswordAgent);

                //broker manager
                LOG.debug("Starting Broker Manager");
                this.activeBrokerManagerThread = new Thread(new ActiveBrokerManager(this));
                this.activeBrokerManagerThread.start();
                /*synchronized (activeBrokerManagerThread) {
					activeBrokerManagerThread.wait();
				}*/
                while (!this.ActiveBrokerManagerActive) {
                    Thread.sleep(1000);
                }
                //logger.debug("ActiveBrokerManager Started..");

                if (this.isIPv6) { //set broker address for consumers and producers
                    this.brokerAddressAgent = "[::1]";
                } else {
                    this.brokerAddressAgent = "localhost";
                }

                //consumer region
                this.consumerRegionThread = new Thread(new ActiveRegionConsumer(this, this.region, "tcp://" + this.brokerAddressAgent + ":32010",brokerUserNameAgent,brokerPasswordAgent));
                this.consumerRegionThread.start();
                while (!this.ConsumerThreadRegionActive) {
                    Thread.sleep(1000);
                }
                //logger.debug("Region ConsumerThread Started..");

                this.gdb = new ControllerDB(this); //start graphdb service
                this.discoveryMap = new ConcurrentHashMap<>(); //discovery map
                LOG.debug("AgentDiscover Service Started");
                this.agentDiscover = new AgentDiscovery(this); //discovery service
                LOG.debug("ControllerDB Service Started");

                this.isRegionalController = true;
                //start regional init

                //start regional discovery
                discoveryList.clear();
                if (this.isIPv6)
                    discoveryList = this.dcv6.getDiscoveryResponse(DiscoveryType.REGION, 2000);
                discoveryList.addAll(this.dcv4.getDiscoveryResponse(DiscoveryType.REGION, 2000));
                if (!discoveryList.isEmpty()) {
                    for (MsgEvent ime : discoveryList) {
                        this.incomingCanidateBrokers.offer(ime);
                        logger.debug("Region Found: " + ime.getParams());

                    }
                }
                //global init
                if (/*PluginEngine.config.getStringParam("globalcontroller_host") != null*/this.config.getStringParam("globalcontroller_host") != null) {
                    LOG.info("Global Controller : Config Found Starting...");
                    this.globalControllerChannel = new GlobalControllerChannel(this);
                    this.hasGlobalController = this.globalControllerChannel.getController();
                    if (this.hasGlobalController) {
                        LOG.debug("Global Controller : Connected...");
                    } else {
                        LOG.debug("Global Controller : Unable to Contact!");
                    }
                }
            } else {
                //determine least loaded broker
                //need to use additional metrics to determine best fit broker
                String cbrokerAddress = null;
                String cbrokerValidatedAuthenication = null;

                String cRegion = null;
                int brokerCount = -1;
                for (MsgEvent bm : discoveryList) {

                    int tmpBrokerCount = Integer.parseInt(bm.getParam("agent_count"));
                    if (brokerCount < tmpBrokerCount) {
                        System.out.println("commInit {}" + bm.getParams().toString());
                        cbrokerAddress = bm.getParam("dst_ip");
                        cbrokerValidatedAuthenication = bm.getParam("validated_authenication");
                        cRegion = bm.getParam("src_region");
                    }
                }
                if ((cbrokerAddress != null) && (cbrokerValidatedAuthenication != null)) {

                    //set agent broker auth
                    String[] tmpAuth = cbrokerValidatedAuthenication.split(",");
                    this.brokerUserNameAgent = tmpAuth[0];
                    this.brokerPasswordAgent = tmpAuth[1];

                    //set broker ip
                    InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
                    if (remoteAddress instanceof Inet6Address) {
                        cbrokerAddress = "[" + cbrokerAddress + "]";
                    }
                    if ((this.region.equals("init")) && (this.agent.equals("init"))) {
                        //RandomString rs = new RandomString(4);
                        this.agent = "agent-" + java.util.UUID.randomUUID().toString();//rs.nextString();
                        //logger.warn("Agent region changed from :" + oldRegion + " to " + region);
                    }
                    this.brokerAddressAgent = cbrokerAddress;

                    this.region = cRegion;
                    LOG.info("Assigned regionid=" + this.region);
                    this.agentpath = this.region + "_" + this.agent;
                    LOG.debug("AgentPath=" + this.agentpath);


                }
                this.isRegionalController = false;
            }

            //consumer agent
            this.consumerAgentThread = new Thread(new ActiveAgentConsumer(this, this.agentpath, "tcp://" + this.brokerAddressAgent + ":32010",brokerUserNameAgent,brokerPasswordAgent));
            this.consumerAgentThread.start();
            while (!this.ConsumerThreadActive) {
                Thread.sleep(1000);
            }
            LOG.debug("Agent ConsumerThread Started..");

            this.ap = new ActiveProducer(this, "tcp://" + this.brokerAddressAgent + ":32010", brokerUserNameAgent, brokerPasswordAgent);

            //watchDogProcess = new plugincore.WatchDog();
            setWatchDog(new WatchDog(region, agent, pluginID, logger, config));
            startWatchDog();
            LOG.info("WatchDog started");
            this.healthWatcher = new HealthWatcher(this);
            LOG.info("HealthWatcher started");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("commInit " + e.getMessage());
        }
    }

    public boolean isLocal(String checkAddress) {
        boolean isLocal = false;
        if (checkAddress.contains("%")) {
            String[] checkScope = checkAddress.split("%");
            checkAddress = checkScope[0];
        }
        List<String> localAddressList = localAddresses();
        for (String localAddress : localAddressList) {
            if (localAddress.contains(checkAddress)) {
                isLocal = true;
            }
        }
        return isLocal;
    }

    public List<String> localAddresses() {
        List<String> localAddressList = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> inter = NetworkInterface.getNetworkInterfaces();
            while (inter.hasMoreElements()) {
                NetworkInterface networkInter = inter.nextElement();
                for (InterfaceAddress interfaceAddress : networkInter.getInterfaceAddresses()) {
                    String localAddress = interfaceAddress.getAddress().getHostAddress();
                    if (localAddress.contains("%")) {
                        String[] localScope = localAddress.split("%");
                        localAddress = localScope[0];
                    }
                    if (!localAddressList.contains(localAddress)) {
                        localAddressList.add(localAddress);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("localAddresses Error: {}", ex.getMessage());
        }
        return localAddressList;
    }

    public boolean isIPv6() {
        boolean isIPv6 = false;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
                    continue; // Don't want to broadcast to the loopback interface
                }
                if (networkInterface.supportsMulticast()) {
                    for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                        if ((interfaceAddress.getAddress() instanceof Inet6Address)) {
                            isIPv6 = true;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("isIPv6 Error: {}", ex.getMessage());
        }
        return isIPv6;
    }

    public List<String> reachableAgents() {
        List<String> rAgents = null;
        try {
            rAgents = new ArrayList<>();
            if (this.isRegionalController) {
                ActiveMQDestination[] er = this.broker.broker.getBroker().getDestinations();
                for (ActiveMQDestination des : er) {
                    if (des.isQueue()) {
                        rAgents.add(des.getPhysicalName());
                    }
                }
            } else {
                rAgents.add(this.region); //just return regional controller
            }
        } catch (Exception ex) {
            LOG.error("isReachableAgent Error: {}", ex.getMessage());
        }
        return rAgents;
    }

    public boolean isReachableAgent(String remoteAgentPath) {
        boolean isReachableAgent = false;
        if (this.isRegionalController) {
            try {
                ActiveMQDestination[] er = this.broker.broker.getBroker().getDestinations();
                for (ActiveMQDestination des : er) {
                    if (des.isQueue()) {
                        String testPath = des.getPhysicalName();
                        if (testPath.equals(remoteAgentPath)) {
                            isReachableAgent = true;
                        }
                    }
                }
            } catch (Exception ex) {
                LOG.error("isReachableAgent Error: {}", ex.getMessage());
            }
        } else {
            isReachableAgent = true; //send all messages to regional controller if not broker
        }
        return isReachableAgent;
    }

    public boolean isConsumerThreadActive() {
        return ConsumerThreadActive;
    }
    public void setConsumerThreadActive(boolean consumerThreadActive) {
        ConsumerThreadActive = consumerThreadActive;
    }

    public ConcurrentLinkedQueue<MsgEvent> getIncomingCanidateBrokers() {
        return incomingCanidateBrokers;
    }
    public void setIncomingCanidateBrokers(ConcurrentLinkedQueue<MsgEvent> incomingCanidateBrokers) {
        this.incomingCanidateBrokers = incomingCanidateBrokers;
    }

    public ConcurrentHashMap<String, BrokeredAgent> getBrokeredAgents() {
        return brokeredAgents;
    }
    public void setBrokeredAgents(ConcurrentHashMap<String, BrokeredAgent> brokeredAgents) {
        this.brokeredAgents = brokeredAgents;
    }

    public boolean isActiveBrokerManagerActive() {
        return ActiveBrokerManagerActive;
    }
    public void setActiveBrokerManagerActive(boolean activeBrokerManagerActive) {
        ActiveBrokerManagerActive = activeBrokerManagerActive;
    }

    public boolean isConsumerThreadRegionActive() {
        return ConsumerThreadRegionActive;
    }
    public void setConsumerThreadRegionActive(boolean consumerThreadRegionActive) {
        ConsumerThreadRegionActive = consumerThreadRegionActive;
    }

    public ActiveBroker getBroker() {
        return broker;
    }
    public void setBroker(ActiveBroker broker) {
        this.broker = broker;
    }

    public boolean isClientDiscoveryActiveIPv4() {
        return clientDiscoveryActiveIPv4;
    }
    public void setClientDiscoveryActiveIPv4(boolean clientDiscoveryActiveIPv4) {
        this.clientDiscoveryActiveIPv4 = clientDiscoveryActiveIPv4;
    }

    public boolean isClientDiscoveryActiveIPv6() {
        return clientDiscoveryActiveIPv6;
    }
    public void setClientDiscoveryActiveIPv6(boolean clientDiscoveryActiveIPv6) {
        this.clientDiscoveryActiveIPv6 = clientDiscoveryActiveIPv6;
    }

    public boolean isDiscoveryActive() {
        return DiscoveryActive;
    }
    public void setDiscoveryActive(boolean discoveryActive) {
        DiscoveryActive = discoveryActive;
    }

    public boolean isRegionalController() {
        return this.isRegionalController;
    }
    public void setRegionalController(boolean regionalController) {
        isRegionalController = regionalController;
    }

    public boolean hasGlobalController() {
        return this.hasGlobalController;
    }

    public Map<String, Long> getDiscoveryMap() {
        return discoveryMap;
    }
    public void setDiscoveryMap(Map<String, Long> discoveryMap) {
        this.discoveryMap = discoveryMap;
    }

    public ControllerDB getGDB() {
        return gdb;
    }
    public void setGDB(ControllerDB gdb) {
        this.gdb = gdb;
    }
    public void removeGDBNode(String region, String agent, String pluginID) {
        if (this.gdb != null)
            this.gdb.removeNode(region, agent, pluginID);
    }
}
