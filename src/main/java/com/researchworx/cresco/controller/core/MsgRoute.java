package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

public class MsgRoute implements Runnable {
    private MsgEvent rm;
    private Launcher plugin;
    private CLogger logger;

    public MsgRoute(Launcher plugin, MsgEvent rm) {
        this.plugin = plugin;
        this.logger = new CLogger(MsgRoute.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        this.rm = rm;
    }

    public void run() {
        try {
            if (!getTTL()) { //check ttl
                return;
            }

            int routePath = getRoutePath();

            //logger.error("CASE=" + routePath + " GC=" + plugin.isGlobalController() + " RC=" + plugin.isRegionalController());
            //logger.error(rm.getParams().toString());
            //logger.error(rm.getMsgType().toString());

/*
            if(rm.getMsgType() == MsgEvent.Type.EXEC) {
                logger.error("Controller msgType: [" + rm.getMsgType().toString() + "] routepath: " + routePath + "[" + rm.getParams().toString() + "]");
            }
*/
/*
            if(rm.getMsgType() == MsgEvent.Type.CONFIG) {
                logger.error("msgType: [" + rm.getMsgType().toString() + "] routepath: " + routePath + "[" + rm.getParams().toString() + "]");
           }
*/

            rm.setParam("routepath",String.valueOf(routePath));
            MsgEvent re = null;
            switch (routePath) {

                case 0:  //System.out.println("CONTROLLER ROUTE CASE 0");
                    if (rm.getParam("configtype") != null) {
                        if (rm.getParam("configtype").equals("comminit")) {
                            logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 0");
                            logger.trace(rm.getParams().toString());
                            //PluginEngine.msgInQueue.offer(rm);
                            plugin.sendMsgEvent(rm);
                        }
                    }
                    break;

                case 16:  //INTER-REGIONAL MESSAGE REGION-to-REGION OUTGOING 16
                    logger.debug("INTER-REGIONAL MESSAGE REGION-to-REGION OUTGOING 16");
                    logger.trace(rm.getParams().toString());
                    externalSend();
                    break;
                case 20:  //INTER-REGIONAL MESSAGE OUTGOING 20
                    logger.debug("INTER-REGIONAL MESSAGE OUTGOING 20");
                    logger.trace(rm.getParams().toString());
                    externalSend();
                    break;
                    /*
                case 21:
                    logger.debug("PLUGIN SOURCED, REGIONAL CONTROLLER SENDING INTER-REGIONAL MESSAGE 21");
                    re = getRegionalCommandExec();
                    break;
                    */
                    /*
                case 21:
                    if ((plugin.isRegionalController()) && (rm.getParam("dst_agent") == null)) {
                        logger.debug("CONTROLLER SENDING INTER-REGIONAL MESSAGE 21");
                        logger.trace(rm.getParams().toString());
                        regionalSend();
                    } else {
                        externalSend();
                    }
                    break;
                case 32:
                    if ((plugin.isRegionalController()) && (rm.getParam("dst_agent") == null)) {
                        logger.debug("INTER-REGIONAL MESSAGE REGION-to-REGION INCOMING 32");
                        logger.trace(rm.getParams().toString());
                        regionalSend();
                    }
                    else {
                    logger.error("ROUTE 32 : WHY ?" + rm.getParams().toString());
                    }
                    break;
                */
                case 40:  ////INTER-REGIONAL MESSAGE INCOMING 40
                    //PluginEngine.msgInQueue.offer(rm);
                    logger.debug("INTER-REGIONAL MESSAGE INCOMING 40");
                    logger.trace(rm.getParams().toString());
                    plugin.sendMsgEvent(rm);
                    break;
                case 42:  //System.out.println("CONTROLLER ROUTE TO COMMANDEXEC : 42 "  + rm.getParams());
                    logger.debug("INTER-REGIONAL PLUGIN-PLUGIN MESSAGE 42");
                    logger.trace(rm.getParams().toString());
                    re = getCommandExec();
                    break;
                    /*
                case 48:
                    if (( plugin.isRegionalController()) && (rm.getParam("dst_agent") == null)) {
                        logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 48");
                        logger.trace(rm.getParams().toString());
                        regionalSend();
                    } else {
                        externalSend();
                    }
                    break;
                    */
                case 52:  //System.out.println("CONTROLLER ROUTE TO REGIONAL AGENT : 52 " + rm.getParams()); //also where regional messages go
                        logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 52");
                        logger.trace(rm.getParams().toString());
                        externalSend();

                    break;
                case 53:  //System.out.println("CONTROLLER ROUTE TO REGIONAL AGENT : 53 " + rm.getParams());
                        logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 53");
                        logger.trace(rm.getParams().toString());
                        externalSend();

                    break;
                case 56:  //System.out.println("CONTROLLER ROUTE TO LOCAL AGENT : 56 "  + rm.getParams());
                    //PluginEngine.msgInQueue.offer(rm);
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE TO ITS AGENT 56");
                    logger.trace(rm.getParams().toString());
                    plugin.sendMsgEvent(rm);
                    break;
                case 58:  //System.out.println("CONTROLLER ROUTE TO COMMANDEXEC : 58 "  + rm.getParams());
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 58");
                    logger.trace(rm.getParams().toString());
                    re = getCommandExec();
                    break;
                case 61:
                    logger.debug("CONTROLLER SENDING MESSAGE TO ITS OWN AGENT");
                    logger.trace(rm.getParams().toString());
                    plugin.sendMsgEvent(rm);
                    //re = getCommandExec();
                    break;
                case 62:  //System.out.println("CONTROLLER ROUTE TO COMMANDEXEC : 62 "  + rm.getParams());
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 62");
                    logger.trace(rm.getParams().toString());
                    re = getCommandExec();
                    break;
                case 64:  //System.out.println("CONTROLLER ROUTE CASE 64");
                    if (rm.getParam("configtype") != null) {
                        if (rm.getParam("configtype").equals("comminit")) {
                            logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 64");
                            logger.trace(rm.getParams().toString());
                            //PluginEngine.msgInQueue.offer(rm);
                            plugin.sendMsgEvent(rm);
                        }
                    }
                    break;
                case 80:
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE TO ANOTHER REGION 80");
                    logger.trace(rm.getParams().toString());
                    externalSend();
                    break;
                case 84:
                    logger.debug("AGENT SENDING REGIONAL MESSAGE TO REGIONAL CONTROLLER 84");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    break;
                case 85:
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE TO ANOTHER REGION 85");
                    logger.trace(rm.getParams().toString());
                    externalSend();
                    break;
                case 96:
                    logger.debug("GLOBAL/REGIONAL CONTROLLER RECIEVING MESSAGE FROM REGIONAL CONTROLLER 96");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    break;
                case 104:
                    logger.debug("CONTROLLER SENDING MESSAGE TO ITS OWN AGENT");
                    logger.trace(rm.getParams().toString());
                    plugin.sendMsgEvent(rm);
                    //re = getCommandExec();
                    break;

                case 106:
                    logger.debug("GLOBAL/REGIONAL CONTROLLER PLUGIN RECIEVING MESSAGE FROM REGIONAL CONTROLLER 106");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    break;
                case 112:
                    logger.debug("REGIONAL AGENT SENDING MESSAGE TO REGIONAL CONTROLLER 112");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    break;
                case 116:
                    logger.debug("CONTROLLER AGENT SENDING MESSAGE TO ITS CONTROLLER 116");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    break;
                case 117:
                    logger.debug("CONTROLLER PLUGIN SENDING MESSAGE TO REGION PLUGIN 117");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    //re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    externalSend();
                    break;
                case 120:
                    logger.debug("CONTROLLER PLUGIN SENDING MESSAGE TO ITS AGENT 120");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    //re = getRegionalCommandExec();
                    plugin.sendMsgEvent(rm);
                    break;
                case 122:
                    logger.debug("LOCAL PLUGIN SENDING MESSAGE TO SELF 122");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    //externalSend();
                    break;
                case 125:
                    logger.debug("CONTROLLER PLUGIN SENDING MESSAGE TO ITS AGENT 125");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    //re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    //externalSend();
                    plugin.sendMsgEvent(rm);
                    break;
                case 126:
                    logger.debug("CONTROLLER AGENT SENDING MESSAGE TO ITS CONTROLLER PLUGIN 126");
                    logger.trace(rm.getParams().toString());
                    re = getCommandExec();
                    //re = getRegionalCommandExec();
                    break;
                case 127:
                    logger.debug("CONTROLLER PLUGIN SENDING MESSAGE TO SELF 127");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    //externalSend();
                    break;

                    /*
                case 122:
                    logger.debug("REGIONAL CONTROLLER SENDING TO ANOTHER REGION 122");
                    logger.trace(rm.getParams().toString());
                    re = getRegionalCommandExec();
                    break;
                    */
                /*
                case 64:  //System.out.println("CONTROLLER ROUTE CASE 64");
                    if (rm.getParam("configtype") != null) {
                        if (rm.getParam("configtype").equals("comminit")) {
                            logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 64");
                            logger.trace(rm.getParams().toString());
                            //PluginEngine.msgInQueue.offer(rm);
                            plugin.sendMsgEvent(rm);
                        }
                    }
                    break;
                case 117:
                        logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 117");
                        logger.trace(rm.getParams().toString());
                        //regionalSend();
                        re = getRegionalCommandExec();
                    break;
                case 240:
                    logger.debug("CONTROLLER SENDING MESSAGE TO ITS AGENT 240");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    //plugin.sendMsgEvent(rm);
                    break;
                case 244:
                        logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 244");
                        logger.trace(rm.getParams().toString());
                        //regionalSend();
                        re = getRegionalCommandExec();
                    break;
                case 245:
                        logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 244");
                        logger.trace(rm.getParams().toString());
                        //regionalSend();
                        re = getRegionalCommandExec();
                        break;
                case 248:  //System.out.println("CONTROLLER ROUTE TO LOCAL AGENT : 56 "  + rm.getParams());
                    //PluginEngine.msgInQueue.offer(rm);
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE TO ITS AGENT 248");
                    logger.trace(rm.getParams().toString());
                    plugin.sendMsgEvent(rm);
                    break;
                case 250:
                    logger.debug("CONTROLLER SENDING REGIONAL MESSAGE 250");
                    logger.trace(rm.getParams().toString());
                    //regionalSend();
                    re = getRegionalCommandExec();
                    break;

*/
                    //new

                default:
                    //System.out.println("CONTROLLER ROUTE CASE " + routePath + " " + rm.getParams());
                    logger.error("CONTROLLER ROUTE CASE " + routePath + " " + rm.getParams());
                    //logger.error(rm.getParams().toString());
                    re = null;
                    break;
            }

            if (re != null) {
                re.setReturn(); //reverse to-from for return
                //PluginEngine.msgInQueue.offer(re);
                plugin.sendMsgEvent(re);
                //PluginEngine.msgIn(re);
            }
            //	AgentEngine.commandExec.cmdExec(me);

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Controller : MsgRoute : Route Failed " + ex.toString() + " " + rm.getParams().toString());

        }

    }

    /*
    private void regionalSend() {
        try {
            //PluginEngine.agentDiscover.discover(rm);
            plugin.discover(rm);
        } catch (Exception ex) {
            logger.error("regionalSend - " + ex.getMessage());
        }
    }
    */


    private MsgEvent getRegionalCommandExec() {
        try {
            String callId = "callId-" + /*PluginEngine.region*/plugin.getRegion() + "_" + /*PluginEngine.agent*/plugin.getAgent() + "_" + /*PluginEngine.plugin*/plugin.getPluginID(); //calculate callID
            if (rm.getParam(callId) != null) { //send message to RPC hash
                //PluginEngine.rpcMap.put(rm.getParam(callId), rm);
                plugin.receiveRPC(rm.getParam(callId), rm);
            } else {
                //return PluginEngine.commandExec.cmdExec(rm);
                if(plugin.getRegionHealthWatcher() != null) {
                    if(plugin.getRegionHealthWatcher().rce != null) {
                        return plugin.getRegionHealthWatcher().rce.execute(rm);
                    } else {
                        logger.error("getRegionHealthWatcher().rce = null");
                    }

                } else {
                    logger.error("getRegionHealthWatcher() = null");
                }
            }
        } catch (Exception ex) {
            logger.error("getRegionalCommandExec - " + ex.getMessage());
            ex.printStackTrace();
        }
        return null;
    }



    private MsgEvent getCommandExec() {
        try {
            String callId = "callId-" + /*PluginEngine.region*/plugin.getRegion() + "_" + /*PluginEngine.agent*/plugin.getAgent() + "_" + /*PluginEngine.plugin*/plugin.getPluginID(); //calculate callID
            if (rm.getParam(callId) != null) { //send message to RPC hash
                //PluginEngine.rpcMap.put(rm.getParam(callId), rm);
                plugin.receiveRPC(rm.getParam(callId), rm);
            } else {
                //return PluginEngine.commandExec.cmdExec(rm);
                return plugin.execute(rm);
            }
        } catch (Exception ex) {
            logger.error("getCommandExec - " + ex.getMessage());
            ex.printStackTrace();
        }
        return null;
    }


    private void externalSend() {
        String targetAgent = null;
        try {
            if ((rm.getParam("dst_region") != null) && (rm.getParam("dst_agent") != null)) {
                //agent message
                targetAgent = rm.getParam("dst_region") + "_" + rm.getParam("dst_agent");

            } else if ((rm.getParam("dst_region") != null) && (rm.getParam("dst_agent") == null)) {
                //regional message
                targetAgent = rm.getParam("dst_region");
            }
            logger.trace("Send Target : " + targetAgent);
            if (/*PluginEngine.isReachableAgent(targetAgent)*/plugin.isReachableAgent(targetAgent)) {
                //PluginEngine.ap.sendMessage(rm);
                plugin.sendAPMessage(rm);
                logger.trace("Send Target : " + targetAgent + " params : " + rm.getParams().toString());
                //System.out.println("SENT NOT CONTROLLER MESSAGE / REMOTE=: " + targetAgent + " " + " region=" + ce.getParam("dst_region") + " agent=" + ce.getParam("dst_agent") + " "  + ce.getParams());
            } else {
                logger.error("Unreachable External Agent : " + targetAgent);
            }
        } catch (Exception ex) {
            logger.error("External Send Error : " + targetAgent);
            ex.printStackTrace();
        }
    }

    private int getRoutePath() {
        int routePath;
        try {
            //determine if local or controller
            String RC = "0";
            String RXr = "0";
            String RXa = "0";
            String RXp = "0";
            String TXr = "0";
            String TXa = "0";
            String TXp = "0";


            if(plugin.isRegionalController()) {
                RC = "1";
            }

            if (rm.getParam("dst_region") != null) {
                if (rm.getParam("dst_region").equals(/*PluginEngine.region*/plugin.getRegion())) {
                    RXr = "1";
                    if (rm.getParam("dst_agent") != null) {
                        if (rm.getParam("dst_agent").equals(/*PluginEngine.agent*/plugin.getAgent())) {
                            RXa = "1";
                            if (rm.getParam("dst_plugin") != null) {
                                if (rm.getParam("dst_plugin").equals(/*PluginEngine.plugin*/plugin.getPluginID())) {
                                    RXp = "1";
                                }
                            }
                        }
                    }
                }

            }
            if (rm.getParam("src_region") != null) {
                if (rm.getParam("src_region").equals(/*PluginEngine.region*/plugin.getRegion())) {
                    TXr = "1";
                    if (rm.getParam("src_agent") != null) {
                        if (rm.getParam("src_agent").equals(/*PluginEngine.agent*/plugin.getAgent())) {
                            TXa = "1";
                            if (rm.getParam("src_plugin") != null) {
                                if (rm.getParam("src_plugin").equals(/*PluginEngine.plugin*/plugin.getPluginID())) {
                                    TXp = "1";
                                }
                            }
                        }
                    }
                }

            }
            String routeString = RC + RXr + TXr + RXa + TXa + RXp + TXp;
            routePath = Integer.parseInt(routeString, 2);
        } catch (Exception ex) {
            if(rm != null) {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " " + rm.getParams().toString());
            } else {
                logger.error("Controller : MsgRoute : getRoutePath Error: " + ex.getMessage() + " RM=NULL");
            }
            ex.printStackTrace();
            routePath = -1;
        }
        //System.out.println("REGIONAL CONTROLLER ROUTEPATH=" + routePath + " MsgType=" + rm.getMsgType() + " Params=" + rm.getParams());

        return routePath;
    }

    private int getRoutePath2() {
        /*
         * rE aE pE  rM aM pM   Logic                                             Values   Action
         * -- -- --  -- -- --   ------------------------------------------------  -------  ---------------------------------------
         *  0  X  X   X  X  X   Global Broadcast Message                          0 - 31   Broadcast to Global
         *  1  0  0   0  X  X   Regional Broadcast Message (External Region)      32 - 35  Forward to Global
         *  1  0  0   1  X  X   Regional Broadcast Message (Current Region)       36 - 39  Broadcast to Region
         *  1  0  1   0  X  X   Regional Broadcast Message (External Region)      40 - 43  Forward to Global
         *  1  0  1   1  X  0   Regional Broadcast Message (Current Region)       44 - 47  Broadcast to Region
         *  1  0  1   1  X  1   Regional Broadcast Message (Current Region)       44 - 47  Broadcast to Region & Execute
         *  1  1  0   0  X  X   Agent Message (External Agent / External Region)  48 - 51  Forward to Global
         *  1  1  0   1  0  X   Agent Message (External Agent / Current Region)   52 - 53  Forward to Regional Controller or Agent
         *  1  1  0   1  1  X   Agent Message                                     54 - 55  Forward to Agent
         *  1  1  1   0  X  X   Plugin Message (External Region)                  56 - 59  Forward to Global
         *  1  1  1   1  0  X   Plugin Message (External Agent / Current Region)  60 - 61  Forward to Regional Controller or Agent
         *  1  1  1   1  1  0   Plugin Message (External Plugin / Current Agent)  62       Forward to Agent
         *  1  1  1   1  1  1   Plugin Message (Current Plugin / Current Agent)   63       Execute
         *
         *  Results:
         *  --------
         *  GlobalForward & Broadcast       0-31
         *  GlobalForward                   32-35, 40-43, 48-51, 56-59
         *  RegionalForward & Broadcast     36-39
         *  RegionalForward & Execute       44-47
         *  RegionalForward                 52-53, 60-61
         *  AgentForward                    54-55, 62
         *  Execute                         63
         */
        try {
            String rExists = "0";
            String aExists = "0";
            String pExists = "0";
            String rMatches = "0";
            String aMatches = "0";
            String pMatches = "0";

            if (rm.getParam("dst_region") != null) {
                rExists = "1";
                if (rm.getParam("dst_region").equals(plugin.getRegion()))
                    rMatches = "1";
            }
            if (rm.getParam("dst_agent") != null) {
                aExists = "1";
                if (rm.getParam("dst_agent").equals(plugin.getAgent()))
                    aMatches = "1";
            }
            if (rm.getParam("dst_plugin") != null) {
                pExists = "1";
                if (rm.getParam("dst_plugin").equals(plugin.getPluginID()))
                    pMatches = "1";
            }

            return Integer.parseInt(rExists + aExists + pExists + rMatches + aMatches + pMatches);
        } catch (Exception e) {
            logger.error("getRoutePath2 Error : {}", e.getMessage());
            return -1;
        }
    }

    private boolean getTTL() {
        boolean isValid = true;
        try {
            if (rm.getParam("ttl") != null) {
                int ttlCount = Integer.valueOf(rm.getParam("ttl"));

                if (ttlCount > 10) {
                    System.out.println("**Controller : MsgRoute : High Loop Count**");
                    System.out.println("MsgType=" + rm.getMsgType().toString());
                    System.out.println("Region=" + rm.getMsgRegion() + " Agent=" + rm.getMsgAgent() + " plugin=" + rm.getMsgPlugin());
                    System.out.println("params=" + rm.getParams());
                    isValid = false;
                }

                ttlCount++;
                rm.setParam("ttl", String.valueOf(ttlCount));
            } else {
                rm.setParam("ttl", "0");
            }
        } catch (Exception ex) {
            isValid = false;
        }
        return isValid;
    }
}
