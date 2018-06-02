package com.researchworx.cresco.controller.core;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.communication.*;
import com.researchworx.cresco.controller.db.DBInterface;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.controller.kpi.kpireporter.MeasurementEngine;
import com.researchworx.cresco.controller.kpi.kpireporter.PerfControllerMonitor;
import com.researchworx.cresco.controller.kpi.kpireporter.PerfMonitorNet;
import com.researchworx.cresco.controller.kpi.kpireporter.PerfSysMonitor;
import com.researchworx.cresco.controller.netdiscovery.*;
import com.researchworx.cresco.controller.regionalcontroller.RegionHealthWatcher;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.messaging.RPC;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.JMSException;
import java.io.PrintWriter;
import java.io.StringWriter;
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

@AutoService(CPlugin.class)
public class Launcher extends CPlugin {

    public ControllerState cstate;

    private boolean DBManagerActive = false;


    private String agentpath;

    //manager for all certificates
    private CertificateManager certificateManager;

    private ExecutorService msgInProcessQueue;

    //perf monitor for controller for networks
    private PerfMonitorNet perfMonitorNet;

    //regional
    private DBInterface gdb;

    private boolean clientDiscoveryActiveIPv4 = false;
    private boolean clientDiscoveryActiveIPv6 = false;
    private boolean DiscoveryActive = false;
    private boolean UDPDiscoveryActive = false;
    private boolean TCPDiscoveryActive = false;


    private boolean ActiveBrokerManagerActive = false;
    private boolean ActiveDestManagerActive = false;

    //for monitoring
    private MeasurementEngine measurementEngine;


    private boolean ConsumerThreadActive = false;

    public boolean isRestartOnShutdown() {
        return restartOnShutdown;
    }

    public void setRestartOnShutdown(boolean restartOnShutdown) {
        this.restartOnShutdown = restartOnShutdown;
    }

    private boolean restartOnShutdown = false;

    private Thread discoveryUDPEngineThread;
    private Thread discoveryTCPEngineThread;


    public PerfMonitorNet getPerfMonitorNet() {return perfMonitorNet; }

    public CertificateManager getCertificateManager() {
        return certificateManager;
    }


    public Thread getActiveBrokerManagerThread() {
        return activeBrokerManagerThread;
    }
    public void setActiveBrokerManagerThread(Thread activeBrokerManagerThread) {
        this.activeBrokerManagerThread = activeBrokerManagerThread;
    }

    private Thread activeBrokerManagerThread;

    public Thread getGlobalControllerManagerThread() {
        return globalControllerManagerThread;
    }

    public void setGlobalControllerManagerThread(Thread GlobalControllerManagerThread) {
        this.globalControllerManagerThread = globalControllerManagerThread;
    }

    private Thread globalControllerManagerThread;


    public Thread getConsumerAgentThread() {
        return consumerAgentThread;
    }

    public void setConsumerAgentThread(Thread consumerAgentThread) {
        this.consumerAgentThread = consumerAgentThread;
    }

    private Thread consumerAgentThread;
    private Thread shutdownHook;

    private ActiveProducer ap;

    private KPIProducer kpip;

    private String brokerAddressAgent;
    public String brokerUserNameAgent;
    public String brokerPasswordAgent;



    /*
    private boolean isRegionalController = false;
    private boolean isGlobalController = false;
    private String[] globalController;
    private String[] regionalController;
    */

    private boolean GlobalControllerManagerActive = false;

    private Map<String, Long> discoveryMap;

    private boolean isIPv6 = false;
    private boolean isActive = false;

    public AtomicInteger responds = new AtomicInteger(0);

    private ControllerConfig controllerConfig;

    private ConcurrentHashMap<String, BrokeredAgent> brokeredAgents;

    private BlockingQueue<MsgEvent> incomingCanidateBrokers;
    private BlockingQueue<MsgEvent> outgoingMessages;
    private BlockingQueue<MsgEvent> resourceScheduleQueue;
    private BlockingQueue<gPayload> appScheduleQueue;

    private ActiveBroker broker;
    private KPIBroker kpiBroker;


    private RegionHealthWatcher regionHealthWatcher;

    public boolean isStarted = false;

    public Launcher() {
        //this.msgInProcessQueue = Executors.newFixedThreadPool(4);
        this.msgInProcessQueue = Executors.newCachedThreadPool();
        //this.msgInProcessQueue = Executors.newSingleThreadExecutor();
    }

    @Override
    public void setExecutor() {
        setExec(new Executor(this));
    }

    public void start() {
        this.config = new ControllerConfig(config.getConfig());
        this.measurementEngine = new MeasurementEngine(this);

        System.setProperty("log.console.level", "SEVERE");

        this.setWatchDog(null);
        this.setRPC(null);

        cstate = new ControllerState(this);

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

            if (this.perfMonitorNet != null) {
                this.perfMonitorNet.stop();
                this.perfMonitorNet = null;
            }

            if(!stopNetDiscoveryEngine()) {
                logger.error("Failed to stop Network Discovery Engine");
            }

            /*
            if (this.watchDog != null) {
                this.watchDog.stop();
                //this might be wrong
                //this.watchDog = null;
            }
            */

            this.GlobalControllerManagerActive = false;
            if (this.globalControllerManagerThread!= null) {
                logger.trace("Global HealthWatcher shutting down");
                this.regionHealthWatcher.communicationsHealthTimer.cancel();
                this.regionHealthWatcher.regionalUpdateTimer.cancel();
                this.globalControllerManagerThread.join();
                this.globalControllerManagerThread = null;
                logger.info("Global HealthWatcher shutting down");

            }

            if (this.regionHealthWatcher != null) {
                logger.trace("Region HealthWatcher shutting down");
                //in case its not canceled as part of global
                this.regionHealthWatcher.communicationsHealthTimer.cancel();
                this.regionHealthWatcher.regionalUpdateTimer.cancel();
                this.regionHealthWatcher = null;
                logger.info("Region HealthWatcher shutting down");

            }
            this.ActiveBrokerManagerActive = false;

            //this.getIncomingCanidateBrokers().offer(null);
            if (this.activeBrokerManagerThread != null) {
                logger.trace("Active Broker Manager shutting down");
                this.activeBrokerManagerThread.interrupt();
                this.activeBrokerManagerThread.join();
                this.activeBrokerManagerThread = null;
                logger.info("Active Broker Manager shutting down");
            }
            if (this.ap != null) {
                logger.trace("Producer shutting down");
                this.ap.shutdown();
                this.ap = null;
                logger.info("Producer shutting down");
            }

            this.ConsumerThreadActive = false;
            if (this.consumerAgentThread != null) {
                logger.trace("Agent Consumer shutting down");
                this.consumerAgentThread.interrupt();
                this.consumerAgentThread.join();
                this.consumerAgentThread = null;
                logger.info("Agent Consumer shutting down");
            }

            if (this.broker != null) {
                logger.trace("Broker shutting down");
                this.broker.stopBroker();
                this.broker = null;
                logger.info("Broker shutting down");

            }

            //disable
            this.DBManagerActive = false;
            logger.info("DB shutting down");


            if (this.restartOnShutdown) {
                MsgEvent ce = new MsgEvent(MsgEvent.Type.CONFIG, this.region, this.agent, null, "comminit");
                ce.setParam("action","comminit");
                ce.setParam("src_region", this.region);
                ce.setParam("src_agent", this.agent);
                ce.setParam("src_plugin", this.pluginID);
                ce.setParam("dst_region", this.region);
                ce.setParam("dst_agent", this.agent);
                while(!commInit()); //reinit everything
                //notify agent of change
                ce.setParam("set_region", this.region);
                ce.setParam("set_agent", this.agent);
                ce.setParam("is_regional_controller", Boolean.toString(this.cstate.isRegionalController()));
                ce.setParam("is_active", Boolean.toString(this.isActive));
                //PluginEngine.msgInQueue.add(ce);
                this.sendMsgEvent(ce);
                this.restartOnShutdown = false;
            }

        } catch (Exception ex) {
            logger.error("shutdown {}", ex.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }
        System.out.println("SHUTDOWN COMPLETE!!!");
        logger.info("complete comm shutting down");

    }

    public ControllerConfig getControllerConfig() {
        return this.controllerConfig;
    }

    public void sendAPMessage(MsgEvent msg) {
        if ((this.ap == null) && (!region.equals("init"))) {
            logger.error("AP is null");
            logger.error("Message: " + msg.getParams());
            return;
        }
        else if(this.ap == null) {
            logger.trace("AP is null");
            return;
        }
        this.ap.sendMessage(msg);
    }

    private  List<MsgEvent> initAgentDiscovery() {
        //continue regional discovery until regional controller is found
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
                discoveryList = new ArrayList<>();
                    if (this.isIPv6) {
                        DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                        logger.debug("Broker Search (IPv6)...");
                        discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_ipv6_agent_timeout", 2000)));
                        logger.debug("IPv6 Broker count = {}" + discoveryList.size());
                    }
                    DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
                    logger.debug("Broker Search (IPv4)...");
                    discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_ipv4_agent_timeout", 2000)));
                    logger.debug("Broker count = {}" + discoveryList.size());
                    if(discoveryList.isEmpty()) {
                        discoveryList = null;
                    }
        } catch (Exception ex) {
            logger.error("initAgentDiscovery() Error " + ex.getMessage());
        }

        return discoveryList;
    }

    private Boolean initAgent(List<MsgEvent> discoveryList) {
        //connect to a specific regional controller
        boolean isInit = false;
        try {
            if(getConfig().getStringParam("regional_controller_host") != null) {
                this.cstate.setAgentInit("initAgent() Static Regional Host: " + getConfig().getStringParam("regional_controller_host"));
                while(!isInit) {

                    String tmpRegion = discoveryList.get(0).getParam("dst_region");
                    this.agent = getConfig().getStringParam("agentname", "agent-" + java.util.UUID.randomUUID().toString());
                    this.agentpath = tmpRegion + "_" + this.agent;
                    certificateManager = new CertificateManager(this, agentpath);

                    TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
                    List<MsgEvent> certDiscovery = ds.discover(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), getConfig().getStringParam("regional_controller_host"), true);

                    String cbrokerAddress = certDiscovery.get(0).getParam("dst_ip");
                    String cbrokerValidatedAuthenication = certDiscovery.get(0).getParam("validated_authenication");
                    String cRegion = certDiscovery.get(0).getParam("dst_region");
                    String cAgent = certDiscovery.get(0).getParam("dst_agent");

                    if ((cbrokerAddress != null) && (cbrokerValidatedAuthenication != null)) {

                        if((tmpRegion.equals(cRegion)) && (getConfig().getStringParam("regional_controller_host").equals(cbrokerAddress))) {

                            this.region = certDiscovery.get(0).getParam("dst_region");

                            String[]tmpAuth = cbrokerValidatedAuthenication.split(",");
                            this.brokerUserNameAgent = tmpAuth[0];
                            this.brokerPasswordAgent = tmpAuth[1];

                            //set broker ip
                            InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
                            if (remoteAddress instanceof Inet6Address) {
                                cbrokerAddress = "[" + cbrokerAddress + "]";
                            }

                            this.brokerAddressAgent = cbrokerAddress;

                            //TODO SET AGENT INFORMATOIN HERE
                            this.cstate.setAgentSuccess(cRegion,cAgent,"initAgent() Static Regional Host: " + getConfig().getStringParam("regional_controller_host") + " connected.");
                            isInit = true;
                            logger.info("Broker IP: " + cbrokerAddress);
                            logger.info("Region: " + this.region);
                            logger.info("Agent: " + this.agent);

                        }
                    }
                }
            }
            //do discovery
            else {
                this.cstate.setAgentInit("initAgent() : Dynamic Discovery");

                while(!isInit || discoveryList.isEmpty()) {

                    //determine least loaded broker
                    //need to use additional metrics to determine best fit broker
                    String pcbrokerAddress = null;
                    String pcbrokerValidatedAuthenication = null;

                    String pcRegion = null;
                    //String cAgent = null;

                    int brokerCount = -1;
                    for (MsgEvent bm : discoveryList) {

                        int tmpBrokerCount = Integer.parseInt(bm.getParam("agent_count"));
                        if (brokerCount < tmpBrokerCount) {
                            logger.trace("commInit {}" + bm.getParams().toString());
                            pcbrokerAddress = bm.getParam("dst_ip");
                            pcbrokerValidatedAuthenication = bm.getParam("validated_authenication");
                            pcRegion = bm.getParam("dst_region");
                            //cAgent = bm.getParam("dst_agent");
                        }
                    }

                    if ((pcbrokerAddress != null) && (pcbrokerValidatedAuthenication != null)) {

                        this.agent = getConfig().getStringParam("agentname", "agent-" + java.util.UUID.randomUUID().toString());
                        this.region = pcRegion;
                        this.agentpath = pcRegion + "_" + this.agent;
                        certificateManager = new CertificateManager(this, agentpath);

                        TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
                        List<MsgEvent> certDiscovery = ds.discover(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), pcbrokerAddress, true);

                        String cbrokerAddress = certDiscovery.get(0).getParam("dst_ip");
                        String cbrokerValidatedAuthenication = certDiscovery.get(0).getParam("validated_authenication");
                        String cRegion = certDiscovery.get(0).getParam("dst_region");
                        String cAgent = certDiscovery.get(0).getParam("dst_agent");


                    if ((cbrokerAddress != null) && (cbrokerValidatedAuthenication != null)) {


                        //UDPDiscoveryStatic ds = new UDPDiscoveryStatic(this);
                        //discoveryList.addAll(ds.discover(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), getConfig().getStringParam("regional_controller_host")));

                        //List<MsgEvent> certDiscovery =

                        //set agent broker auth
                        String[] tmpAuth = cbrokerValidatedAuthenication.split(",");
                        this.brokerUserNameAgent = tmpAuth[0];
                        this.brokerPasswordAgent = tmpAuth[1];

                        //set broker ip
                        InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
                        if (remoteAddress instanceof Inet6Address) {
                            cbrokerAddress = "[" + cbrokerAddress + "]";
                        }
                        //if ((this.region.equals("init")) && (this.agent.equals("init"))) {
                        //RandomString rs = new RandomString(4);
                        this.agent = getConfig().getStringParam("agentname", "agent-" + java.util.UUID.randomUUID().toString());
                        //this.agent = "agent-" + java.util.UUID.randomUUID().toString();//rs.nextString();
                        //logger.warn("Agent region changed from :" + oldRegion + " to " + region);
                        //}

                        this.brokerAddressAgent = cbrokerAddress;

                        this.region = cRegion;

                        logger.info("Assigned regionid=" + this.region);
                        this.agentpath = this.region + "_" + this.agent;
                        logger.debug("AgentPath=" + this.agentpath);
                        this.cstate.setAgentSuccess(cRegion, cAgent, "initAgent() Dynamic Regional Host: " + cbrokerAddress + " connected.");
                        isInit = true;
                    }
                }
                if (this.getConfig().getBooleanParam("enable_clientnetdiscovery", true)) {
                    //discovery engine
                    if (!startNetDiscoveryEngine()) {
                        logger.error("Start Network Discovery Engine Failed!");
                    }
                }
                //hold no loop
                    if(!isInit || discoveryList.isEmpty()) {

                        logger.error("isInit Status: " + isInit + " discoveryList.isEmpty() == true");
                        Thread.sleep(1000);
                    }

            }
            }
        } catch (Exception ex) {
            logger.error("initAgent() Error " + ex.getMessage());
        }

        return isInit;
    }

    private List<MsgEvent> initAgentStatic() {
        //connect to a specific regional controller
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            logger.info("Static Agent Connection to Regional Controller : " + getConfig().getStringParam("regional_controller_host"));

            //UDPDiscoveryStatic ds = new UDPDiscoveryStatic(this);
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);

            discoveryList.addAll(ds.discover(DiscoveryType.AGENT, getConfig().getIntegerParam("discovery_static_agent_timeout",10000), getConfig().getStringParam("regional_controller_host")));
            logger.debug("Static Agent Connection count = {}" + discoveryList.size());
            if(discoveryList.size() == 0) {
                logger.info("Static Agent Connection to Regional Controller : " + getConfig().getStringParam("regional_controller_host") + " failed! - Restarting Discovery!");
            }
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initAgentStatic() Error " + ex.getMessage());
        }

        return discoveryList;
    }

    private  List<MsgEvent> initRegionDiscovery() {
        //continue regional discovery until regional controller is found
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            if (this.isIPv6) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_ipv6_region_timeout", 2000)));
                logger.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_ipv4_region_timeout", 2000)));
            logger.debug("Broker count = {}" + discoveryList.size());
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initRegionDiscovery() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private  Boolean initRegionToRegion() {
        //continue regional discovery until regional controller is found
        boolean isInit = false;
        try {
            List<MsgEvent> discoveryList = new ArrayList<>();
            if (this.isIPv6) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_ipv6_region_timeout", 2000)));
                logger.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_ipv4_region_timeout", 2000)));
            logger.debug("Broker count = {}" + discoveryList.size());


            if (!discoveryList.isEmpty()) {

                    for (MsgEvent ime : discoveryList) {
                        this.incomingCanidateBrokers.add(ime);
                        logger.debug("Regional Controller Found: " + ime.getParams());
                }
            }

        } catch (Exception ex) {
            logger.error("initRegionToRegion() Error " + ex.getMessage());
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
        return isInit;
    }

    private  Boolean initIOChannels() {
        boolean isInit = false;
        try {
            boolean consumerAgentConnected = false; //loop to catch expections on JMX connect of consumer
            int consumerAgentConnectCount = 0;
            while(!consumerAgentConnected && (consumerAgentConnectCount < 10)) {
                try {
                    //consumer agent
                    int discoveryPort = getConfig().getIntegerParam("discovery_port",32010);
                    this.consumerAgentThread = new Thread(new ActiveAgentConsumer(this, this.agentpath, "ssl://" + this.brokerAddressAgent + ":" + discoveryPort, brokerUserNameAgent, brokerPasswordAgent));
                    this.consumerAgentThread.start();
                    while (!this.ConsumerThreadActive) {
                        Thread.sleep(1000);
                    }
                    consumerAgentConnected = true;
                    logger.debug("Agent ConsumerThread Started..");
                } catch (JMSException jmx) {
                    logger.error("Agent ConsumerThread JMX " + jmx.getMessage());
                    StringWriter errors = new StringWriter();
                    jmx.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
                catch (Exception ex) {
                    logger.error("Agent ConsumerThread " + ex.getMessage());
                    StringWriter errors = new StringWriter();
                    ex.printStackTrace(new PrintWriter(errors));
                    logger.error(errors.toString());
                }
                consumerAgentConnectCount++;
            }
            int discoveryPort = getConfig().getIntegerParam("discovery_port",32010);
            this.ap = new ActiveProducer(this, "ssl://" + this.brokerAddressAgent + ":" + discoveryPort, brokerUserNameAgent, brokerPasswordAgent);

            logger.debug("Agent ProducerThread Started..");
            isInit = true;
        } catch (Exception ex) {
            logger.error("initIOChannels() Error " + ex.getMessage());
        }
        return isInit;
    }

    private List<MsgEvent> initRegionStatic() {
        //connect to a specific regional controller
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            logger.info("Static Region Connection to Regional Controller : " + getConfig().getStringParam("regional_controller_host"));
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
            discoveryList.addAll(ds.discover(DiscoveryType.REGION, getConfig().getIntegerParam("discovery_static_agent_timeout",10000), getConfig().getStringParam("regional_controller_host")));
            logger.debug("Static Agent Connection count = {}" + discoveryList.size());
            if(discoveryList.size() == 0) {
                logger.info("Static Region Connection to Regional Controller : " + getConfig().getStringParam("regional_controller_host") + " failed! - Restarting Discovery!");
            }
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initRegionStatic() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private  List<MsgEvent> initGlobalDiscovery() {
        //continue regional discovery until regional controller is found
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            if (this.isIPv6) {
                DiscoveryClientIPv6 dc = new DiscoveryClientIPv6(this);
                logger.debug("Broker Search (IPv6)...");
                discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.GLOBAL, getConfig().getIntegerParam("discovery_ipv6_global_timeout", 2000)));
                logger.debug("IPv6 Broker count = {}" + discoveryList.size());
            }
            DiscoveryClientIPv4 dc = new DiscoveryClientIPv4(this);
            logger.debug("Broker Search (IPv4)...");
            discoveryList.addAll(dc.getDiscoveryResponse(DiscoveryType.GLOBAL, getConfig().getIntegerParam("discovery_ipv4_global_timeout", 2000)));
            logger.debug("Broker count = {}" + discoveryList.size());
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initGlobalDiscovery() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private List<MsgEvent> initGlobalStatic() {
        //connect to a specific regional controller
        List<MsgEvent> discoveryList = null;
        boolean isInit = false;
        try {
            discoveryList = new ArrayList<>();
            logger.info("Static Region Connection to Global Controller : " + getConfig().getStringParam("global_controller_host"));
            TCPDiscoveryStatic ds = new TCPDiscoveryStatic(this);
            discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, getConfig().getIntegerParam("discovery_static_global_timeout",10000), getConfig().getStringParam("global_controller_host")));
            logger.debug("Static Agent Connection count = {}" + discoveryList.size());
            if(discoveryList.size() == 0) {
                logger.info("Static Region Connection to Global Controller : " + getConfig().getStringParam("global_controller_host") + " failed! - Restarting Discovery!");
            }
            if(discoveryList.isEmpty()) {
                discoveryList = null;
            }
        } catch (Exception ex) {
            logger.error("initGlobalStatic() Error " + ex.getMessage());
        }
        return discoveryList;
    }

    private Boolean initGlobal() {
        //don't discover anything
        boolean isInit = false;
        try {

            String kpiPort = config.getStringParam("kpiport","32011");
            String kpiProtocol = config.getStringParam("kpiprotocol","tcp");
            //init KPIBroker
            this.kpiBroker = new KPIBroker(this, kpiProtocol, kpiPort,this.agentpath + "_KPI",brokerUserNameAgent,brokerPasswordAgent);
            //init KPIProducer
            this.kpip = new KPIProducer(this, "KPI", kpiProtocol + "://" + this.brokerAddressAgent + ":" + kpiPort, "bname", "bpass");

            if(cstate.isRegionalController()) {

                //do global discovery here
                this.globalControllerManagerThread = new Thread(new GlobalHealthWatcher(this));
                this.globalControllerManagerThread.start();

                while (!this.GlobalControllerManagerActive) {
                    Thread.sleep(1000);
                    logger.trace("Wait loop for Global Controller");
                }
                isInit = true;
                measurementEngine.initGlobalMetrics();
            } else {
                logger.error("initGlobal Error : Must be Regional Controller First!");
            }

        } catch (Exception ex) {
            logger.error("initGlobal() Error " + ex.getMessage());
        }
        return isInit;
    }

    private Boolean initRegion() {
        boolean isInit = false;
        try {
            region = getConfig().getStringParam("regionname", "region-" + java.util.UUID.randomUUID().toString());
            agent = getConfig().getStringParam("agentname", "agent-" + java.util.UUID.randomUUID().toString());
            logger.debug("Generated regionid=" + this.region);

            this.cstate.setRegionInit("initRegion() Region:" + region + " agent:" + agent);

            this.agentpath = this.region + "_" + this.agent;
            certificateManager = new CertificateManager(this,agentpath);

            logger.debug("AgentPath=" + this.agentpath);
            //Start controller services

            //logger.debug("IPv6 UDPDiscoveryEngine Started..");

            logger.debug("Broker starting");
            if((getConfig().getStringParam("broker_username") != null) && (getConfig().getStringParam("broker_password") != null)) {
                brokerUserNameAgent = getConfig().getStringParam("broker_username");
                brokerPasswordAgent = getConfig().getStringParam("broker_password");
            }
            else {
                brokerUserNameAgent = java.util.UUID.randomUUID().toString();
                brokerPasswordAgent = java.util.UUID.randomUUID().toString();
            }
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
            logger.debug("ActiveBrokerManager Started..");

            if (this.isIPv6) { //set broker address for consumers and producers
                this.brokerAddressAgent = "[::1]";
            } else {
                this.brokerAddressAgent = "localhost";
            }

            //removed region consumer, no longer needed things to go agents

            this.gdb = new DBInterface(this); //start com.researchworx.cresco.controller.db service
            logger.debug("RegionalControllerDB Service Started");
            //started by DBInterface
            while (!this.DBManagerActive) {
                Thread.sleep(1000);
            }

            this.discoveryMap = new ConcurrentHashMap<>(); //discovery map

            //TODO Does this still need to be done, this was causing a delay?
            /*
            //enable this regional controller in the DB
            MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, getRegion(), getAgent(), getPluginID(), "enabled");
            le.setParam("src_region", getRegion());
            le.setParam("dst_region", getRegion());
            le.setParam("action", "enable");
            le.setParam("watchdogtimer", String.valueOf(getConfig().getLongParam("watchdogtimer", 5000L)));
            le.setParam("source", "initRegion()");
            getGDB().addNode(le);
            */

            logger.info("Discovery Engine ");

            //discovery engine
            if(!startNetDiscoveryEngine()) {
                logger.error("Start Network Discovery Engine Failed!");
            }

            cstate.setRegionGlobalInit("initRegion() : Success");
            isInit = true;
            measurementEngine.initRegionalMetrics();

        } catch (Exception ex) {
            logger.error("initRegion() Error " + ex.getMessage());
            this.cstate.setRegionFailed("initRegion() Error " + ex.getMessage());
        }

        return isInit;
    }

    public Boolean commInit() {

        boolean isRegionalController = false;
        boolean isGlobalController = false;

        boolean isCommInit = true;
        if(getWatchDog() != null) {
            watchDog.stop();
            logger.info("WatchDog Disabled");
        }

        logger.info("Initializing services");
        setActive(true);

        try {

            this.brokeredAgents = new ConcurrentHashMap<>();
            this.incomingCanidateBrokers = new LinkedBlockingQueue<>();
            this.outgoingMessages = new LinkedBlockingQueue<>();
            this.brokerAddressAgent = null;
            this.isIPv6 = isIPv6();


            DiscoveryClientIPv4 dcv4 = new DiscoveryClientIPv4(this);
            DiscoveryClientIPv6 dcv6 = new DiscoveryClientIPv6(this);

            List<MsgEvent> discoveryList = null;


            if(getConfig().getBooleanParam("is_agent",false)) {
                if(getConfig().getStringParam("regional_controller_host") == null) {
                        discoveryList = initAgentDiscovery();
                        while(discoveryList == null) {
                            discoveryList = initAgentDiscovery();
                        }
                        isRegionalController = false;
                        isGlobalController = false;
                } else {
                    //agent with static region
                        discoveryList = initAgentStatic();
                        while(discoveryList == null) {
                            discoveryList = initAgentStatic();
                            Thread.sleep(1000);
                        }
                        isRegionalController = false;
                        isGlobalController = false;
                }
            } else if(getConfig().getBooleanParam("is_region",false)) {
                    isRegionalController = true;
                    isGlobalController = false;

            } else if(getConfig().getBooleanParam("is_global",false)) {
                //by pass all discovery
                    isRegionalController = true;
                    isGlobalController = true;
            } else {
                //allow promotion of agent to region if agent connection fails
                discoveryList = initAgentDiscovery();
                if(discoveryList != null) {
                    isRegionalController = false;
                    isGlobalController = false;
                } else {
                    discoveryList = initGlobalDiscovery();
                    if(discoveryList != null) {
                        isRegionalController = true;
                        isGlobalController = false;
                    } else {
                        isRegionalController = true;
                        isGlobalController = true;
                    }
                }
            }

            //if a regional controller setup a broker and attach consumer and producer
            if(isRegionalController) {

                if(initRegion()) {
                    //connect to other regions
                    if(getConfig().getBooleanParam("regional_discovery",false)) {
                        initRegionToRegion();
                    }
                } else {
                    logger.error("Unable to init Region!");
                    return false;
                }
            } else { //not a region, try and connect to one.
                    if(!initAgent(discoveryList)) {
                        logger.error("Unable to init agent!");
                        return false;
                    }
            }

            this.logger = new CLogger(msgOutQueue, region, agent, pluginID, CLogger.Level.Info);

            //setup producer and consumers
            if(!initIOChannels()) {
                logger.error("initIOChannels Failed");
                return false;
            }

            //set values
            this.setRegion(this.region);
            this.setAgent(this.agent);
            this.setPluginID(this.pluginID);

            //setRPC to reflect new values
            this.setRPC(new RPC(this.msgOutQueue, this.region, this.agent, this.pluginID, this.logger));

            //set new watchdog to reflect discovered values
            this.setWatchDog(new WatchDog(this.region, this.agent, this.pluginID, this.logger, this.config));
            getWatchDog().start();
            logger.info("WatchDog Started");

            //Setup Regional Watcher
            this.regionHealthWatcher = new RegionHealthWatcher(this);

            //Setup Global is Needed
            if(isRegionalController){
                initGlobal();
            }

            //populate controller-specific metrics
            measurementEngine.initControllerMetrics();

            PerfControllerMonitor perfControllerMonitor = new PerfControllerMonitor(this);
            perfControllerMonitor.start();
            logger.info("Performance Controller monitoring initialized");


            PerfSysMonitor perfSysMonitor = new PerfSysMonitor(this);
            perfSysMonitor.start();
            logger.info("Performance System monitoring initialized");


            if(perfMonitorNet == null) {
              perfMonitorNet = new PerfMonitorNet(this);
            }

            perfMonitorNet.start();
            logger.info("Performance Network monitoring initialized");

            /*
            logger.info("Starting Network Discovery Engine...");
            if(!startNetDiscoveryEngine()) {
                logger.error("Start Network Discovery Engine Failed!");
            }
            */


        } catch (Exception e) {
            e.printStackTrace();
            logger.trace("commInit " + e.getMessage());
        }
        return isCommInit;
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
            if (this.cstate.isRegionalController()) {
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
        if (this.cstate.isRegionalController()) {
            try {
                ActiveMQDestination[] er = this.broker.broker.getBroker().getDestinations();
                for (ActiveMQDestination des : er) {
                    //for(String despaths : des.getDestinationPaths()) {
                    //    logger.info("isReachable destPaths: " + despaths);
                    //}
                    if (des.isQueue()) {
                        String testPath = des.getPhysicalName();

                        logger.trace("isReachable isQueue: physical = " + testPath + " qualified = " + des.getQualifiedName());
                        if (testPath.equals(remoteAgentPath)) {
                            isReachableAgent = true;
                        }
                    }
                }

                er = this.broker.broker.getRegionBroker().getDestinations();
                for (ActiveMQDestination des : er) {
                    //for(String despaths : des.getDestinationPaths()) {
                    //    logger.info("isReachable destPaths: " + despaths);
                    //}

                    if (des.isQueue()) {
                        String testPath = des.getPhysicalName();
                        logger.trace("Regional isReachable isQueue: physical = " + testPath + " qualified = " + des.getQualifiedName());
                        if (testPath.equals(remoteAgentPath)) {
                            isReachableAgent = true;
                        }
                    }
                }
                /*
                Map<String,BrokeredAgent> brokerAgentMap = this.getBrokeredAgents();
                for (Map.Entry<String, BrokeredAgent> entry : brokerAgentMap.entrySet()) {
                    String agentPath = entry.getKey();
                    BrokeredAgent bAgent = entry.getValue();

                    logger.info("isReachable : agentName: " + agentPath + " agentPath:" + bAgent.agentPath + " " + bAgent.activeAddress + " " + bAgent.brokerStatus.toString());
                    if((remoteAgentPath.equals(agentPath)) && (bAgent.brokerStatus == BrokerStatusType.ACTIVE))
                    {
                        isReachableAgent = true;
                    }
                }
                */
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

    public boolean stopNetDiscoveryEngine() {
        boolean isStopped = false;
        try {
            if (this.discoveryUDPEngineThread != null) {
                logger.trace("UDP Discovery Engine shutting down");
                UDPDiscoveryEngine.shutdown();
                this.discoveryUDPEngineThread.join();
                this.discoveryUDPEngineThread = null;
                this.DiscoveryActive = false;
            }
            if (this.discoveryTCPEngineThread != null) {
                logger.trace("TCP Discovery Engine shutting down");
                TCPDiscoveryEngine.shutdown();
                this.discoveryTCPEngineThread.join();
                this.discoveryTCPEngineThread = null;
                this.DiscoveryActive = false;
            }
            isStopped = true;
        } catch(Exception ex) {
            logger.error("stopNetDiscoveryEngine: " + ex.getMessage());
        }
        return isStopped;
    }

    public boolean startNetDiscoveryEngine() {
        boolean isStarted = false;
        try {
            if(!this.DiscoveryActive) {
                //discovery engine
                this.discoveryUDPEngineThread = new Thread(new UDPDiscoveryEngine(this));
                this.discoveryUDPEngineThread.start();

                this.discoveryTCPEngineThread = new Thread(new TCPDiscoveryEngine(this));
                this.discoveryTCPEngineThread.start();


                while (!this.UDPDiscoveryActive && !this.TCPDiscoveryActive) {
                    Thread.sleep(1000);
                }
                setDiscoveryActive(true);
            }
            isStarted = true;
        } catch(Exception ex) {
            logger.error("startNetDiscoveryEngine: " + ex.getMessage());
        }
        return isStarted;
    }

    public BlockingQueue<MsgEvent> getResourceScheduleQueue() {
        return resourceScheduleQueue;
    }
    public void setResourceScheduleQueue(BlockingQueue<MsgEvent> appScheduleQueue) {
        this.resourceScheduleQueue = appScheduleQueue;
    }

    public BlockingQueue<gPayload> getAppScheduleQueue() {
        return appScheduleQueue;
    }
    public void setAppScheduleQueue(BlockingQueue<gPayload> appScheduleQueue) {
        this.appScheduleQueue = appScheduleQueue;
    }

    public BlockingQueue<MsgEvent> getIncomingCanidateBrokers() {
        return incomingCanidateBrokers;
    }
    public void setIncomingCanidateBrokers(BlockingQueue<MsgEvent> incomingCanidateBrokers) {
        this.incomingCanidateBrokers = incomingCanidateBrokers;
    }

    public ConcurrentHashMap<String, BrokeredAgent> getBrokeredAgents() {
        return brokeredAgents;
    }
    public void setBrokeredAgents(ConcurrentHashMap<String, BrokeredAgent> brokeredAgents) {
        this.brokeredAgents = brokeredAgents;
    }

    public boolean isDBManagerActive() {
        return DBManagerActive;
    }
    public void setDBManagerActive(boolean DBManagerActive) {
        this.DBManagerActive = DBManagerActive;
    }

    public boolean isActiveBrokerManagerActive() {
        return ActiveBrokerManagerActive;
    }
    public void setActiveBrokerManagerActive(boolean activeBrokerManagerActive) {
        ActiveBrokerManagerActive = activeBrokerManagerActive;
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

    public boolean isGlobalControllerManagerActive() {
        return GlobalControllerManagerActive;
    }
    public void setGlobalControllerManagerActive(boolean activeBrokerManagerActive) {
        GlobalControllerManagerActive = activeBrokerManagerActive;
    }

    public boolean isDiscoveryActive() {
        return DiscoveryActive;
    }

    public KPIProducer getKPIProducer() { return this.kpip; }

    public void setDiscoveryActive(boolean discoveryActive) {
        DiscoveryActive = discoveryActive;
    }

    public void setUDPDiscoveryActive(boolean discoveryActive) {
        UDPDiscoveryActive = discoveryActive;
    }

    public boolean isUDPDiscoveryActive() {
        return UDPDiscoveryActive;
    }
    public void setTCPDiscoveryActive(boolean discoveryActive) {
        TCPDiscoveryActive = discoveryActive;
    }

    public boolean isTCPDiscoveryActive() {
        return TCPDiscoveryActive;
    }

    public boolean hasActiveProducter() {
        boolean hasAP = false;
        try {
            if(ap != null) {
                hasAP = true;
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return hasAP;
    }

    public MeasurementEngine getMeasurementEngine() {return this.measurementEngine;}

    public RegionHealthWatcher getRegionHealthWatcher() {return this.regionHealthWatcher;}

    public Map<String, Long> getDiscoveryMap() {
        return discoveryMap;
    }

    public void setDiscoveryMap(Map<String, Long> discoveryMap) {
        this.discoveryMap = discoveryMap;
    }

    public DBInterface getGDB() {
        return gdb;
    }
    public void setGDB(DBInterface gdb) {
        this.gdb = gdb;
    }

    public void removeGDBNode(String region, String agent, String pluginID) {
        if (this.gdb != null)
            this.gdb.removeNode(region, agent, pluginID);
    }

    public String getStringFromError(Exception ex) {
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

}
