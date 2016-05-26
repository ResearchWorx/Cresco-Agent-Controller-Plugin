package com.researchworx.cresco.controller.regionalcontroller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalControllerChannel {
    //private static final Logger logger = LoggerFactory.getLogger(GlobalControllerChannel.class);

    private final String USER_AGENT = "Cresco-Agent-Controller-Plugin/0.5.0";
    private Launcher plugin;
    private CLogger logger;
    private Timer timer;
    private long startTS;


    private String controllerUrl;
    private String KPIUrl;

    public GlobalControllerChannel(Launcher plugin) {
        this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
        this.plugin = plugin;
        if (plugin.getConfig().getStringParam("globalcontroller_host") != null) {
            if (plugin.getConfig().getStringParam("globalcontroller_port") != null) {
                controllerUrl = "http://" + plugin.getConfig().getStringParam("globalcontroller_host") + ":" + plugin.getConfig().getStringParam("globalcontroller_port") + "/API";
            } else {
                controllerUrl = "http://" + plugin.getConfig().getStringParam("globalcontroller_host") + ":32000/API";
            }
            KPIUrl = "http://" + plugin.getConfig().getStringParam("globalcontroller_host") + ":32002/API";

            //Create pooling agent
            startTS = System.currentTimeMillis();
            timer = new Timer();
            timer.scheduleAtFixedRate(new CmdPoolTask(), 500, plugin.getConfig().getIntegerParam("watchdogtimer"));
        }

    }

    class CmdPoolTask extends TimerTask {
        private boolean pool() {
            try {
                String url = controllerUrl + "?type=exec&paramkey=cmd&paramvalue=getmsg&paramkey=getregion&paramvalue=" + plugin.getRegion() + "&paramkey=src_region&paramvalue=" + plugin.getRegion() + "&paramkey=src_agent&paramvalue=" + plugin.getAgent() + "&paramkey=src_plugin&paramvalue=" + plugin.getPluginID();
                URL obj = new URL(url);
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();

                con.setConnectTimeout(5000);

                // optional default is GET
                con.setRequestMethod("GET");

                //add request header
                con.setRequestProperty("User-Agent", USER_AGENT);

                int responseCode = con.getResponseCode();

                if (responseCode == 200) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                    in.close();

                    MsgEvent ce = null;
                    try {
                        ce = meFromJson(response.toString());
                    } catch (Exception ex) {
                        //its ok to fail
                        //logger.debug("Controller : ControllerChannel : Error meFromJson");
                    }
                    if (ce != null) {
                        if (ce.getMsgBody() != null) {
                            logger.debug("Incoming Regional Message: " + ce.getParams());
                            plugin.sendMsgEvent(ce);
                            //PluginEngine.msgInQueue.offer(ce);
                            Thread.sleep(500); //take it easy on server
                            return true; //try again for another message
                        }
                    }
                }
                con.disconnect();
                return false;

            } catch (Exception ex) {
                logger.debug("Controller : ControllerChannel : CmdPoolTasks : " + ex.toString());
                return false; //wait for timeout for messages
            }
        }

        public void run() {
            if (plugin.hasGlobalController()) {
                while (pool()) {
                    //logger.debug("pool");
                }
            }
            /*
	    	if(AgentEngine.watchDogActive)
	    	{
	    		long runTime = System.currentTimeMillis() - startTS;
	    		wdMap.put("runtime", String.valueOf(runTime));
	    		wdMap.put("timestamp", String.valueOf(System.currentTimeMillis()));
	    	 
	    		MsgEvent le = new MsgEvent(MsgEventType.WATCHDOG,AgentEngine.config.getRegion(),null,null,wdMap);
	    		le.setParam("src_region", AgentEngine.region);
	  		    le.setParam("src_agent", AgentEngine.agent);
	  		    le.setParam("dst_region", AgentEngine.region);
	  		    AgentEngine.clog.log(le);
	    	}
	    	*/
        }
    }

    public boolean getController() {
        try {
            String url = controllerUrl + "?type=config&paramkey=controllercmd&paramvalue=registercontroller&paramkey=src_region&paramvalue=" + plugin.getRegion() + "&paramkey=src_agent&paramvalue=" + plugin.getAgent() + "&paramkey=src_plugin&paramvalue=" + plugin.getPluginID();
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setConnectTimeout(5000);

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                MsgEvent ce = null;
                try {
                    ce = meFromJson(response.toString());
                } catch (Exception ex) {
                    logger.debug("Controller : ControllerChannel : Error meFromJson");
                }
                if (ce != null) {
                    if (ce.getMsgBody() != null) {
                        if (ce.getMsgBody().equals("controllerregistered")) {
                            return true;
                        }
                    }
                }
            }
            return false;
        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : getController : " + ex.toString());
            return false;
        }
    }

    public String urlFromMsg(String type, Map<String, String> leMap) {

        try {
            StringBuilder sb = new StringBuilder();

            sb.append("?type=" + type);

            Iterator it = leMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry) it.next();
                sb.append("&paramkey=" + URLEncoder.encode(pairs.getKey().toString(), "UTF-8") + "&paramvalue=" + URLEncoder.encode(pairs.getValue().toString(), "UTF-8"));
                it.remove(); // avoids a ConcurrentModificationException
            }
            //logger.debug(sb.toString());
            return sb.toString();
        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : urlFromMsg :" + ex.toString());
            return null;
        }
    }

    public boolean removeNode(MsgEvent le) {
        try {
            Map<String, String> tmpMap = le.getParams();
            Map<String, String> leMap = null;
            String type = null;
            synchronized (tmpMap) {
                leMap = new ConcurrentHashMap<String, String>(tmpMap);
                type = le.getMsgType().toString();
            }
            String url = controllerUrl + urlFromMsg(type, leMap);
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setConnectTimeout(5000);

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                MsgEvent ce = null;
                try {
                    ce = meFromJson(response.toString());
                } catch (Exception ex) {
                    logger.debug("Controller : ControllerChannel : Error meFromJson");
                }
                if (ce != null) {
                    if (ce.getMsgBody() != null) {
                        if (ce.getMsgBody().equals("noderemoved")) {
                            return true;
                        }
                    }
                }
            }
            return false;

        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
            return false;
        }
    }

    public boolean addNode(MsgEvent le) {
        try {

            logger.debug("*addNode Controller Channel: sendParams: " + le.getParams());
            //logger.debug(le.getParamsString());
            //CODY

            Map<String, String> tmpMap = le.getParams();
            Map<String, String> leMap = null;
            String type = null;
            synchronized (tmpMap) {
                leMap = new ConcurrentHashMap<String, String>(tmpMap);
                type = le.getMsgType().toString();
            }
            String url = controllerUrl + urlFromMsg(type, leMap);

            //logger.debug(url);
            logger.debug("*addNode Controller Channel url: " + url);

            //CODY

            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setConnectTimeout(5000);

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                MsgEvent ce = null;
                try {
                    //logger.debug(response);
                    ce = meFromJson(response.toString());
                    logger.debug("*addNode Controller Channel : return responce" + response.toString());

                    //logger.debug(ce.getParamsString());
                    //CODY

                } catch (Exception ex) {
                    logger.debug("Controller : ControllerChannel : Error meFromJson");
                }
                if (ce != null) {
                    if (ce.getMsgBody() != null) {
                        if (ce.getMsgBody().equals("nodeadded")) {
                            return true;
                        }
                    }
                }
            }
            return false;

        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
            return false;
        }
    }

    public boolean setNodeParams(MsgEvent le) {
        try {
            Map<String, String> tmpMap = le.getParams();
            Map<String, String> leMap = null;
            String type = null;
            synchronized (tmpMap) {
                leMap = new ConcurrentHashMap<String, String>(tmpMap);
                type = le.getMsgType().toString();
            }
            String url = controllerUrl + urlFromMsg(type, leMap);
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setConnectTimeout(5000);

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                MsgEvent ce = null;
                try {
                    //logger.debug(response);
                    ce = meFromJson(response.toString());
                } catch (Exception ex) {
                    logger.debug("Controller : ControllerChannel : Error meFromJson");
                }
                if (ce != null) {
                    if (ce.getMsgBody() != null) {
                        if (ce.getMsgBody().equals("paramsadded")) {
                            return true;
                        }
                    }
                }
            }
            return false;

        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
            return false;
        }
    }

    public boolean updatePerf(MsgEvent le) {
        try {
            Map<String, String> tmpMap = le.getParams();
            Map<String, String> leMap = null;
            String type = null;
            synchronized (tmpMap) {
                leMap = new ConcurrentHashMap<String, String>(tmpMap);
                type = le.getMsgType().toString();
            }
            //String url = controllerUrl + urlFromMsg(type,leMap);
            String url = KPIUrl + urlFromMsg(type, leMap);
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setConnectTimeout(5000);

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                MsgEvent ce = null;
                try {
                    //logger.debug(response);
                    ce = meFromJson(response.toString());
                } catch (Exception ex) {
                    logger.debug("Controller : ControllerChannel : Error meFromJson");
                }
                if (ce != null) {
                    if (ce.getMsgBody() != null) {
                        if (ce.getMsgBody().equals("updatedperf")) {
                            return true;
                        }
                    }
                }
            }
            return false;

        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
            return false;
        }
    }

    public void sendController(MsgEvent le) {
        try {
            Map<String, String> tmpMap = le.getParams();
            Map<String, String> leMap = null;
            String type = null;
            synchronized (tmpMap) {
                leMap = new ConcurrentHashMap<String, String>(tmpMap);
                type = le.getMsgType().toString();
            }
            String url = controllerUrl + urlFromMsg(type, leMap);
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setConnectTimeout(5000);

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();

            if (responseCode != 200) {
                logger.debug("Controller : ControllerChannel : sendControllerLog Error RepsonceCode: " + responseCode);
                logger.debug(url);
            }

        } catch (Exception ex) {
            logger.debug("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
        }
    }

    // HTTP GET request
    private void sendGet() throws Exception {

        String url = "http://www.google.com/search?q=mkyong";

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        logger.debug("\nSending 'GET' request to URL : " + url);
        logger.debug("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();


        //print result
        logger.debug(response.toString());

    }

    private MsgEvent meFromJson(String jsonMe) {
        Gson gson = new GsonBuilder().create();
        MsgEvent me = gson.fromJson(jsonMe, MsgEvent.class);
        //logger.debug(p);
        return me;
    }
}
