package com.researchworx.cresco.controller.globalscheduler;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.db.DBApplicationFunctions;
import com.researchworx.cresco.controller.db.DBBaseFunctions;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.sun.media.jfxmedia.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.util.*;
import java.util.Map.Entry;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;


public class ResourceSchedulerEngine implements Runnable {

	private Launcher plugin;
	private GlobalHealthWatcher ghw;
	private CLogger logger;

	public ResourceSchedulerEngine(Launcher plugin, GlobalHealthWatcher ghw) {
		this.plugin = plugin;
		this.ghw = ghw;
        logger = new CLogger(ResourceSchedulerEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
    }
		
	public void run() 
	{
		try
		{
			ghw.SchedulerActive = true;
			while (ghw.SchedulerActive)
			{
				try
				{
					MsgEvent ce = ghw.resourceScheduleQueue.poll();
					if(ce != null)
					{

						logger.debug("me offered");
						//check the pipeline node
						if(ce.getParam("globalcmd").equals("addplugin"))
						{
							//do something to activate a plugin
							logger.debug("starting precheck...");
							//String pluginJar = verifyPlugin(ce);
							if(!verifyPlugin(ce))
							{
								if((plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_code","1")) &&
										(plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_desc","iNode Failed Activation : Plugin not found!")))
								{
									logger.debug("Provisioning Failed: No matching controller plugins found!");
								}
							}
							else
							{
								//adding in jar name information
								//ce.setParam("configparams",ce.getParam("configparams") + ",jarfile=" + pluginJar);

								//Here is where scheduling is taking place
								logger.debug("plugin precheck = OK");
								String agentPath = getLowAgent();

								if(agentPath == null)
								{
									logger.debug("ResourceSchedulerEngine : Unable to find agent for plugin scheduling");
								}
								else
								{
									logger.debug("agent precheck = OK");
									
									String[] agentPath_s = agentPath.split(",");
									String region = agentPath_s[0];
									String agent = agentPath_s[1];
									String resource_id = ce.getParam("resource_id");
									String inode_id = ce.getParam("inode_id");
									//have agent download plugin
									String pluginurl = "http://127.0.0.1:32003/";
									//downloadPlugin(region,agent,pluginJar,pluginurl, false);
									logger.debug("Downloading plugin on region=" + region + " agent=" + agent);
									
									
									//schedule plugin
									logger.debug("Scheduling plugin on region=" + region + " agent=" + agent);
									MsgEvent me = addPlugin(region,agent,ce.getParam("configparams"));
									logger.debug("pluginadd message: " + me.getParams().toString());
									
									//ControllerEngine.commandExec.cmdExec(me);
                                    //logger.error("before send");
                                    //MsgEvent re = plugin.getRPC().call(me);
                                    //logger.error("after send");

                                    plugin.msgIn(me);

									new Thread(new PollAddPlugin(plugin,resource_id, inode_id,region,agent)).start();
								}
								
								/*
								if((ControllerEngine.gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_code","10")) &&
										(ControllerEngine.gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_desc","iNode Active.")))
								{
										//recorded plugin activations
									
								}
								*/
							}
						}
						else if(ce.getParam("globalcmd").equals("removeplugin"))
						{
							new Thread(new PollRemovePlugin(plugin,  ce.getParam("resource_id"),ce.getParam("inode_id"))).start();
						}
					}
					else
					{
						Thread.sleep(1000);
					}
				}
				catch(Exception ex)
				{
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    ex.printStackTrace(pw);
                    logger.error(sw.toString());

                    logger.error("ResourceSchedulerEngine Error: " + ex.toString());
				}
			}
		}
		catch(Exception ex)
		{
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("ResourceSchedulerEngine Error: " + ex.toString());
		}
	}
	
	private Boolean verifyPlugin(MsgEvent ce) {
	    boolean isVerified = false;
		//pre-schedule check
		String configparams = ce.getParam("configparams");
		logger.debug("verifyPlugin params " + configparams);

        Map<String,String> params = getMapFromString(configparams, false);

        String requestedPlugin = params.get("pluginname");

        List<String> pluginMap = getPluginInventory();
        for(String pluginfile : pluginMap) {
            logger.debug("plugin = " + pluginfile);
            logger.debug("plugin name = " + getPluginName(pluginfile));
            if(requestedPlugin.equals(getPluginName(pluginfile))) {
                isVerified = true;
            }
        }

        /*
        String[] cparams = configparams.split(",");
		Map<String,String> cm = new HashMap<String,String>();
		for(String param : cparams)
		{
			String[] paramkv = param.split("=");
			cm.put(paramkv[0], paramkv[1]);
		}

		String requestedPlugin = cm.get("pluginname") + "=" + cm.get("pluginversion");
        String requestedPlugin = params.get("pluginname");
		logger.debug("Requested Plugin=" + requestedPlugin);
		if(pluginMap.contains(requestedPlugin))
		{
			return getPluginFileMap().get(requestedPlugin);
		}
		else
		{
			ce.setMsgBody("Matching plugin could not be found!");
			ce.setParam("pluginstatus","failed");
		}
		*/
		return isVerified;
	}
	
	public Map<String,String> paramStringToMap(String param)
	{
		Map<String,String> params = null;
		try
		{
			params = new HashMap<String,String>();
			String[] pstr = param.split(",");
			for(String str : pstr)
			{
				String[] pstrs = str.split("=");
				params.put(pstrs[0], pstrs[1]);
			}
		}
		catch(Exception ex)
		{
			logger.error("ResourceSchedulerEngine : Error " + ex.toString());
		}
		return params;
	}

	public String getLowAgent()
	{
		
		Map<String,Integer> pMap = new HashMap<String,Integer>();
		String agent_path = null;
		try
		{
			List<String> regionList = plugin.getGDB().gdb.getNodeList(null,null,null);
			//logger.debug("Region Count: " + regionList.size());
			for(String region : regionList)
			{
				List<String> agentList = plugin.getGDB().gdb.getNodeList(region,null,null);
				//logger.debug("Agent Count: " + agentList.size());
				
				for(String agent: agentList)
				{
					List<String> pluginList = plugin.getGDB().gdb.getNodeList(region,agent,null);
					int pluginCount = 0;
					if(pluginList != null)
					{
						pluginCount = pluginList.size();
					}
					String tmp_agent_path = region + "," + agent;
					pMap.put(tmp_agent_path, pluginCount);
				}
			}
			
			
			if(pMap != null)
			{
				Map<String, Integer> sortedMapAsc = sortByComparator(pMap, true);
				Entry<String, Integer> entry = sortedMapAsc.entrySet().iterator().next();
				agent_path = entry.getKey().toString();
				/*
				for (Entry<String, Integer> entry : sortedMapAsc.entrySet())
				{
					logger.debug("Key : " + entry.getKey() + " Value : "+ entry.getValue());
				}
				*/
			}
	        
		}
		catch(Exception ex)
		{
			logger.error("DBEngine : getLowAgent : Error " + ex.toString());
		}
		
		return agent_path;
	}

	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order)
    {

        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	public MsgEvent addPlugin(String region, String agent, String configParams)
	{

	    //else if (ce.getParam("configtype").equals("pluginadd"))

		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"add plugin");
		me.setParam("src_region", plugin.getRegion());
		me.setParam("src_agent", plugin.getAgent());
        me.setParam("src_plugin", plugin.getPluginID());
        me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		me.setParam("configtype", "pluginadd");
		me.setParam("configparams",configParams);
		return me;
	}
	
	public MsgEvent downloadPlugin(String region, String agent, String pluginId, String pluginurl, boolean forceDownload)
	{
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"download plugin");
        me.setParam("src_region", plugin.getRegion());
        me.setParam("src_agent", plugin.getAgent());
        me.setParam("src_plugin", plugin.getPluginID());
        me.setParam("dst_region", region);
        me.setParam("dst_agent", agent);
        me.setParam("configtype", "plugindownload");
		me.setParam("plugin", pluginId);
		me.setParam("pluginurl", pluginurl);
		//me.setParam("configparams", "perflevel="+ perflevel + ",pluginname=DummyPlugin,jarfile=..//Cresco-Agent-Dummy-Plugin/target/cresco-agent-dummy-plugin-0.5.0-SNAPSHOT-jar-with-dependencies.jar,region=test2,watchdogtimer=5000");
		if(forceDownload)
		{
			me.setParam("forceplugindownload", "true");
		}
		return me;
	}

    public List<String> getPluginInventory()
    {
        List<String> pluginFiles = null;
        try
        {
            String pluginDirectory = null;
            if(plugin.getConfig().getStringParam("localpluginrepo") != null) {
                pluginDirectory = plugin.getConfig().getStringParam("localpluginrepo");
            }
            else {
                //if not listed use the controller directory
                File jarLocation = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
                pluginDirectory = jarLocation.getParent(); // to get the parent dir name
            }

            File folder = new File(pluginDirectory);
            if(folder.exists())
            {
                pluginFiles = new ArrayList<String>();
                File[] listOfFiles = folder.listFiles();

                for (int i = 0; i < listOfFiles.length; i++)
                {
                    if (listOfFiles[i].isFile())
                    {
                        pluginFiles.add(listOfFiles[i].getAbsolutePath());
                        logger.debug(listOfFiles[i].getAbsolutePath().toString());
                    }

                }
                if(pluginFiles.isEmpty())
                {
                    pluginFiles = null;
                }
            }
            else {
                logger.error("getPluginFiles Directory ");
            }

        }
        catch(Exception ex)
        {
            logger.debug(ex.toString());
            pluginFiles = null;
        }
        return pluginFiles;
    }

    public static String getPluginName(String jarFile) //This should pull the version information from jar Meta data
    {
        String version;
        try{
            //String jarFile = AgentEngine.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            //logger.debug("JARFILE:" + jarFile);
            //File file = new File(jarFile.substring(5, (jarFile.length() )));
            File file = new File(jarFile);
            FileInputStream fis = new FileInputStream(file);
            @SuppressWarnings("resource")
            JarInputStream jarStream = new JarInputStream(fis);
            Manifest mf = jarStream.getManifest();

            Attributes mainAttribs = mf.getMainAttributes();
            version = mainAttribs.getValue("artifactId");
        }
        catch(Exception ex)
        {
            String msg = "Unable to determine Plugin Version " + ex.toString();
            System.err.println(msg);
            version = "Unable to determine Version";
        }
        return version;
    }

    public static String getPluginVersion(String jarFile) //This should pull the version information from jar Meta data
    {
        String version;
        try{
            //String jarFile = AgentEngine.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            //logger.debug("JARFILE:" + jarFile);
            //File file = new File(jarFile.substring(5, (jarFile.length() )));
            File file = new File(jarFile);
            FileInputStream fis = new FileInputStream(file);
            @SuppressWarnings("resource")
            JarInputStream jarStream = new JarInputStream(fis);
            Manifest mf = jarStream.getManifest();

            Attributes mainAttribs = mf.getMainAttributes();
            version = mainAttribs.getValue("Implementation-Version");
        }
        catch(Exception ex)
        {
            String msg = "Unable to determine Plugin Version " + ex.toString();
            System.err.println(msg);
            version = "Unable to determine Version";
        }
        return version;
    }

	public Map<String,String> getPluginFileMap()
	{
		Map<String,String> pluginList = new HashMap<String,String>();
		
		try
		{
		File jarLocation = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
		String parentDirName = jarLocation.getParent(); // to get the parent dir name
		
		File folder = new File(parentDirName);
		if(folder.exists())
		{
		File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) 
		    {
		      if (listOfFiles[i].isFile()) 
		      {
		        //logger.debug("Found Plugin: " + listOfFiles[i].getName());
		        //<pluginName>=<pluginVersion>,
		        String pluginPath = listOfFiles[i].getAbsolutePath();
		        //pluginList.add(ControllerEngine.commandExec.getPluginName(pluginPath) + "=" + ControllerEngine.commandExec.getPluginVersion(pluginPath));
		        String pluginKey = getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath);
		        String pluginValue = listOfFiles[i].getName();
		        pluginList.put(pluginKey, pluginValue);
		        //pluginList = pluginList + getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath) + ",";
		        //pluginList = pluginList + listOfFiles[i].getName() + ",";
		      } 
		      
		    }
		    if(pluginList.size() > 0)
		    {
		    	return pluginList;
		    }
		}
		
		
		}
		catch(Exception ex)
		{
			logger.debug(ex.toString());
		}
		return null; 
		
	}


    public Map<String,String> getMapFromString(String param, boolean isRestricted) {
        Map<String,String> paramMap = null;

        logger.debug("PARAM: " + param);

        try{
            String[] sparam = param.split(",");
            logger.debug("PARAM LENGTH: " + sparam.length);

            paramMap = new HashMap<String,String>();

            for(String str : sparam)
            {
                String[] sstr = str.split(":");

                if(isRestricted)
                {
                    paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                }
                else
                {
                    if(sstr.length > 1)
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), URLDecoder.decode(sstr[1], "UTF-8"));
                    }
                    else
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                    }
                }
            }
        }
        catch(Exception ex)
        {
            logger.error("getMapFromString Error: " + ex.toString());
        }

        return paramMap;
    }


}



