package com.researchworx.cresco.controller.core;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.controller.communication.*;
import com.researchworx.cresco.controller.netdiscovery.*;
import com.researchworx.cresco.controller.regionalcontroller.*;
import com.researchworx.cresco.controller.shell.AppShellFactory;
import com.researchworx.cresco.controller.shell.InAppPasswordAuthenticator;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;

import java.io.File;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@AutoService(CPlugin.class)
public class Launcher extends CPlugin {

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
    }

    @Override
    public void setExecutor() {
        setExec(new Executor(this));
    }

    public void start() {
        this.config = new ControllerConfig(config.getConfig());
    }

    @Override
    public void msgIn(MsgEvent msg) {
        logger.trace("msgIn : " + msg.getParams().toString());
        msgInProcessQueue.submit(new MsgRoute(this, msg));
    }

    @Override
    public void cleanUp() {
        //closeCommunications();
    }

    public void closeCommunications() {
        try {
            if (this.restartOnShutdown)
                logger.info("Tearing down services");
            else
                logger.info("Shutting down");
            this.DiscoveryActive = false;
            this.ConsumerThreadRegionActive = false;
            this.ConsumerThreadActive = false;
            this.ActiveBrokerManagerActive = false;
            if (this.discoveryEngineThread != null) {
                logger.trace("Discovery Engine shutting down");
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
                logger.trace("Region Consumer shutting down");
                this.consumerRegionThread.join();
                this.consumerRegionThread = null;
            }
            if (this.consumerAgentThread != null) {
                logger.trace("Agent Consumer shutting down");
                this.consumerAgentThread.join();
                this.consumerAgentThread = null;
            }
            if (this.activeBrokerManagerThread != null) {
                logger.trace("Active Broker Manager shutting down");
                this.activeBrokerManagerThread.join();
                this.activeBrokerManagerThread = null;
            }
            if (this.ap != null) {
                logger.trace("Producer shutting down");
                this.ap.shutdown();
                this.ap = null;
            }
            if (this.broker != null) {
                logger.trace("Broker shutting down");
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
            logger.error("shutdown {}", ex.getMessage());
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
            logger.debug("AP is null");
            return;
        }
        this.ap.sendMessage(msg);
    }

    public void commInit() {
        logger.info("Initializing services");
        setActive(true);
        try {

            if(getConfig().getBooleanParam("enable_sshd",false)) {
                SshServer sshd = SshServer.setUpDefaultServer();
                sshd.setPasswordAuthenticator(new InAppPasswordAuthenticator(this));
                sshd.setPort(config.getIntegerParam("sshd_port",5222));
                String keypairPath = config.getStringParam("sshd_rsa_key_path");
                if(keypairPath != null) {
                    try {
                        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(new File(keypairPath)));
                    }
                    catch (Exception ex) {
                        logger.error("Invalid RSA Key File = " +  keypairPath + " Message=" + ex.getMessage());
                        System.exit(0);
                    }
                }
                else {
                    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
                }



                AppShellFactory ssh_shell = new AppShellFactory(this);
                sshd.setShellFactory(ssh_shell);
                sshd.start();
                logger.info("Enabled SSH Shell");
            }

            this.brokeredAgents = new ConcurrentHashMap<>();
            this.incomingCanidateBrokers = new ConcurrentLinkedQueue<>();
            this.outgoingMessages = new ConcurrentLinkedQueue<>();
            this.brokerAddressAgent = null;
            this.isIPv6 = isIPv6();
            this.dcv4 = new DiscoveryClientIPv4(this);
            this.dcv6 = new DiscoveryClientIPv6(this);

            //List<MsgEvent> discoveryList = new ArrayList<>();
            List<MsgEvent> discoveryList = new ArrayList<>();

            if(getConfig().getStringParam("regional_controller_host") != null) {
                //do directed discovery
                while(discoveryList.size() == 0) {
                    logger.info("Static Broker Connection to Host : " + getConfig().getStringParam("regional_controller_host"));
                    DiscoveryStatic ds = new DiscoveryStatic(this);
                    discoveryList.addAll(ds.discover(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_static_agent_timeout",10000), getConfig().getStringParam("regional_controller_host")));
                    logger.debug("Static Broker count = {}" + discoveryList.size());
                    if(discoveryList.size() == 0) {
                        logger.info("Static Broker Connection to Host : " + getConfig().getStringParam("regional_controller_host") + " failed! - Restarting Discovery!");
                    }
                }

            } else {

                if (this.isIPv6) {
                    logger.debug("Broker Search (IPv6)...");
                    discoveryList.addAll(this.dcv6.getDiscoveryResponse(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_ipv6_agent_timeout", 2000)));
                    logger.debug("IPv6 Broker count = {}" + discoveryList.size());
                }
                logger.debug("Broker Search (IPv4)...");
                discoveryList.addAll(this.dcv4.getDiscoveryResponse(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_ipv4_agent_timeout", 2000)));
                logger.debug("Broker count = {}" + discoveryList.size());
            }
            if(getConfig().getStringParam("regional_controller_host") != null) {

                //determine least loaded broker
                //need to use additional metrics to determine best fit broker
                String cbrokerValidatedAuthenication = null;
                String cRegion = null;
                logger.trace("commInit {}" + discoveryList.get(0).getParams().toString());
                cbrokerValidatedAuthenication = discoveryList.get(0).getParam("validated_authenication");
                cRegion = discoveryList.get(0).getParam("src_region");

                if ((cbrokerValidatedAuthenication != null)) {

                    //set agent broker auth
                    String cbrokerAddress = getConfig().getStringParam("regional_controller_host");
                    logger.info("Using static configuration to connect to regional controller: " + cbrokerAddress);

                    String[] tmpAuth = cbrokerValidatedAuthenication.split(",");
                    this.brokerUserNameAgent = tmpAuth[0];
                    this.brokerPasswordAgent = tmpAuth[1];
                    logger.trace("regional_controller_host : brokerUserNameAgent=" + brokerUserNameAgent);
                    logger.trace("regional_controller_host : brokerPasswordAgent=" + brokerPasswordAgent);

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
                    logger.info("Assigned regionid=" + this.region);
                    this.agentpath = this.region + "_" + this.agent;
                    logger.debug("AgentPath=" + this.agentpath);

                }
                this.isRegionalController = false;

            } else if (discoveryList.isEmpty()) {
                //generate regional ident if not assigned
                //String oldRegion = region; //keep old region if assigned

                if ((this.region.equals("init")) && (this.agent.equals("init"))) {
                    region = "region-" + java.util.UUID.randomUUID().toString();
                    agent = "agent-" + java.util.UUID.randomUUID().toString();
                    logger.debug("Generated regionid=" + this.region);
                }
                this.agentpath = this.region + "_" + this.agent;
                logger.debug("AgentPath=" + this.agentpath);
                //Start controller services

                //discovery engine
                this.discoveryEngineThread = new Thread(new DiscoveryEngine(this));
                this.discoveryEngineThread.start();
                while (!this.DiscoveryActive) {
                    Thread.sleep(1000);
                }
                //logger.debug("IPv6 DiscoveryEngine Started..");

                logger.debug("Broker starting");
                //if((getConfig().getStringParam("regional_broker_username") != null) && (getConfig().getStringParam("regional_broker_password") != null)) {
                //    brokerUserNameAgent = getConfig().getStringParam("regional_broker_username");
                //    brokerPasswordAgent = getConfig().getStringParam("regional_broker_password");
                //}
                //else {
                    brokerUserNameAgent = java.util.UUID.randomUUID().toString();
                    brokerPasswordAgent = java.util.UUID.randomUUID().toString();
                //}
                this.broker = new ActiveBroker(this, this.agentpath,brokerUserNameAgent,brokerPasswordAgent);

                //broker manager
                logger.debug("Starting Broker Manager");
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
                logger.debug("AgentDiscover Service Started");
                this.agentDiscover = new AgentDiscovery(this); //discovery service
                logger.debug("ControllerDB Service Started");

                //Test new DB
                /*
                dbServer dbs = new dbServer();
                dbs.startServer();
                 */
                logger.info("Starting Graph DB");
                //GraphDBEngine_emb gdbe = new GraphDBEngine_emb();
                //gdbe.startServer();
                GraphDBEngine gdb = new GraphDBEngine();

                logger.info("Started Graph DB");
                //End Test new DV

                this.isRegionalController = true;
                //start regional init

                //start regional discovery
                discoveryList.clear();

                if (this.isIPv6)
                    discoveryList = this.dcv6.getDiscoveryResponse(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_ipv6_region_timeout", 2000));
                    discoveryList.addAll(this.dcv4.getDiscoveryResponse(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_ipv4_region_timeout", 2000)));
                if (!discoveryList.isEmpty()) {
                    for (MsgEvent ime : discoveryList) {
                        this.incomingCanidateBrokers.offer(ime);
                        logger.debug("Region Found: " + ime.getParams());

                    }
                }
                //global init
                if (/*PluginEngine.config.getStringParam("globalcontroller_host") != null*/this.config.getStringParam("globalcontroller_host") != null) {
                    logger.info("Global Controller : Config Found Starting...");
                    try {
                        this.globalControllerChannel = new GlobalControllerChannel(this);
                        this.hasGlobalController = this.globalControllerChannel.getController();
                        if (this.hasGlobalController) {
                            logger.debug("Global Controller : Connected...");
                        } else {
                            logger.debug("Global Controller : Unable to Contact!");
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }
            }
            else {
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
                    logger.info("Assigned regionid=" + this.region);
                    this.agentpath = this.region + "_" + this.agent;
                    logger.debug("AgentPath=" + this.agentpath);

                }
                this.isRegionalController = false;
            }

            this.logger = new CLogger(msgOutQueue, region, agent, pluginID);

            //consumer agent
            this.consumerAgentThread = new Thread(new ActiveAgentConsumer(this, this.agentpath, "tcp://" + this.brokerAddressAgent + ":32010",brokerUserNameAgent,brokerPasswordAgent));
            this.consumerAgentThread.start();
            while (!this.ConsumerThreadActive) {
                Thread.sleep(1000);
            }
            logger.debug("Agent ConsumerThread Started..");

            this.ap = new ActiveProducer(this, "tcp://" + this.brokerAddressAgent + ":32010", brokerUserNameAgent, brokerPasswordAgent);

            //watchDogProcess = new plugincore.WatchDog();
            //stopWatchDog();
            //setWatchDog(new WatchDog(region, agent, pluginID, logger, config));
            //startWatchDog();
            updateWatchDog();
            logger.info("WatchDog configuration updated");
            this.healthWatcher = new HealthWatcher(this);
            logger.info("HealthWatcher started");
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
            logger.error("localAddresses Error: {}", ex.getMessage());
        }
        return localAddressList;
    }

    public boolean isIPv6() {
        boolean isIPv6 = false;
        try {


            if (getConfig().getStringParam("isIPv6") != null) {
                isIPv6 = getConfig().getBooleanParam("isIPv6", false);
            }
            else {
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
        }
        } catch (Exception ex) {
            logger.error("isIPv6 Error: {}", ex.getMessage());
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
            logger.error("isReachableAgent Error: {}", ex.getMessage());
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
                logger.error("isReachableAgent Error: {}", ex.getMessage());
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
