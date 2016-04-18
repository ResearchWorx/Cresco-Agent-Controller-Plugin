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

public class GlobalControllerChannel {

    private final String USER_AGENT = "Cresco-Agent-Controller-Plugin/0.5.0";
    private Launcher plugin;
    private Timer timer;
    private long startTS;


    private String controllerUrl;
    private String performanceUrl;

    public GlobalControllerChannel(Launcher plugin) {
        this.plugin = plugin;
        if (plugin.getConfig().getStringParam("globalcontroller_host") != null) {
            if (plugin.getConfig().getStringParam("globalcontroller_port") != null) {
                controllerUrl = "http://" + plugin.getConfig().getStringParam("globalcontroller_host") + ":" + plugin.getConfig().getStringParam("globalcontroller_port") + "/API";
            } else {
                controllerUrl = "http://" + plugin.getConfig().getStringParam("globalcontroller_host") + ":32000/API";
            }
            performanceUrl = "http://" + plugin.getConfig().getStringParam("globalcontroller_host") + ":32002/API";

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
                        //System.out.println("Controller : ControllerChannel : Error meFromJson");
                    }
                    if (ce != null) {
                        if (ce.getMsgBody() != null) {
                            System.out.println("Incoming Regional Message: " + ce.getParams());
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
                System.out.println("Controller : ControllerChannel : CmdPoolTasks : " + ex.toString());
                return false; //wait for timeout for messages
            }
        }

        public void run() {
            if (plugin.hasGlobalController()) {
                while (pool()) {
                    //System.out.println("pool");
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
                    System.out.println("Controller : ControllerChannel : Error meFromJson");
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
            System.out.println("Controller : ControllerChannel : getController : " + ex.toString());
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
            //System.out.println(sb.toString());
            return sb.toString();
        } catch (Exception ex) {
            System.out.println("Controller : ControllerChannel : urlFromMsg :" + ex.toString());
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
                    System.out.println("Controller : ControllerChannel : Error meFromJson");
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
            System.out.println("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
            return false;
        }
    }

    public boolean addNode(MsgEvent le) {
        try {

            System.out.println("*addNode Controller Channel: sendParams: " + le.getParams());
            //System.out.println(le.getParamsString());
            //CODY

            Map<String, String> tmpMap = le.getParams();
            Map<String, String> leMap = null;
            String type = null;
            synchronized (tmpMap) {
                leMap = new ConcurrentHashMap<String, String>(tmpMap);
                type = le.getMsgType().toString();
            }
            String url = controllerUrl + urlFromMsg(type, leMap);

            //System.out.println(url);
            System.out.println("*addNode Controller Channel url: " + url);

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
                    //System.out.println(response);
                    ce = meFromJson(response.toString());
                    System.out.println("*addNode Controller Channel : return responce" + response.toString());

                    //System.out.println(ce.getParamsString());
                    //CODY

                } catch (Exception ex) {
                    System.out.println("Controller : ControllerChannel : Error meFromJson");
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
            System.out.println("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
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
                    //System.out.println(response);
                    ce = meFromJson(response.toString());
                } catch (Exception ex) {
                    System.out.println("Controller : ControllerChannel : Error meFromJson");
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
            System.out.println("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
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
            String url = performanceUrl + urlFromMsg(type, leMap);
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
                    //System.out.println(response);
                    ce = meFromJson(response.toString());
                } catch (Exception ex) {
                    System.out.println("Controller : ControllerChannel : Error meFromJson");
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
            System.out.println("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
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
                System.out.println("Controller : ControllerChannel : sendControllerLog Error RepsonceCode: " + responseCode);
                System.out.println(url);
            }

        } catch (Exception ex) {
            System.out.println("Controller : ControllerChannel : sendControllerLog : " + ex.toString());
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
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();


        //print result
        System.out.println(response.toString());

    }

    private MsgEvent meFromJson(String jsonMe) {
        Gson gson = new GsonBuilder().create();
        MsgEvent me = gson.fromJson(jsonMe, MsgEvent.class);
        //System.out.println(p);
        return me;
    }
}