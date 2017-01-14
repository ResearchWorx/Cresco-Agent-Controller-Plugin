package com.researchworx.cresco.controller.globalscheduler;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalHealthWatcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.File;
import java.io.FileInputStream;
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
        logger = new CLogger(ResourceSchedulerEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
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

						System.out.println("me offered");
						//check the pipeline node
						if(ce.getParam("globalcmd").equals("addplugin"))
						{
							//do something to activate a plugin
							System.out.println("starting precheck...");
							String pluginJar = verifyPlugin(ce);
							if(pluginJar == null)
							{
								if((plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_code","1")) &&
										(plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_desc","iNode Failed Activation : Plugin not found!")))
								{
									System.out.println("Provisioning Failed: No matching controller plugins found!");
								}
							}
							else
							{
								//adding in jar name information
								ce.setParam("configparams",ce.getParam("configparams") + ",jarfile=" + pluginJar);
								
								System.out.println("plugin precheck = OK");
								String agentPath = getLowAgent();
								if(agentPath == null)
								{
									System.out.println("ResourceSchedulerEngine : Unable to find agent for plugin scheduling");
								}
								else
								{
									System.out.println("agent precheck = OK");
									
									String[] agentPath_s = agentPath.split(",");
									String region = agentPath_s[0];
									String agent = agentPath_s[1];
									String resource_id = ce.getParam("resource_id");
									String inode_id = ce.getParam("inode_id");
									//have agent download plugin
									String pluginurl = "http://127.0.0.1:32003/";
									downloadPlugin(region,agent,pluginJar,pluginurl, false);
									System.out.println("Downloading plugin on region=" + region + " agent=" + agent);
									
									
									//schedule plugin
									System.out.println("Scheduling plugin on region=" + region + " agent=" + agent);
									MsgEvent me = addPlugin(region,agent,ce.getParam("configparams"));
									System.out.println("pluginadd message: " + me.getParams().toString());
									
									//ControllerEngine.commandExec.cmdExec(me);

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
					System.out.println("ResourceSchedulerEngine Error: " + ex.toString());
				}
			}
		}
		catch(Exception ex)
		{
			System.out.println("ResourceSchedulerEngine Error: " + ex.toString());
		}
	}
	
	private String verifyPlugin(MsgEvent ce)
	{
		//pre-schedule check
		String configparams = ce.getParam("configparams");
		String[] cparams = configparams.split(",");
		Map<String,String> cm = new HashMap<String,String>();
		for(String param : cparams)
		{
			String[] paramkv = param.split("=");
			cm.put(paramkv[0], paramkv[1]);
		}
		
		//check if we have the plugin
		List<String> pluginMap = getPluginInventory();
		String requestedPlugin = cm.get("pluginname") + "=" + cm.get("pluginversion");
		System.out.println("Requested Plugin=" + requestedPlugin);
		if(pluginMap.contains(requestedPlugin))
		{
			return getPluginFileMap().get(requestedPlugin);
		}
		else
		{
			ce.setMsgBody("Matching plugin could not be found!");
			ce.setParam("pluginstatus","failed");
		}
		return null;
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
			System.out.println("ResourceSchedulerEngine : Error " + ex.toString());
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
			//System.out.println("Region Count: " + regionList.size());
			for(String region : regionList)
			{
				List<String> agentList = plugin.getGDB().gdb.getNodeList(region,null,null);
				//System.out.println("Agent Count: " + agentList.size());
				
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
					System.out.println("Key : " + entry.getKey() + " Value : "+ entry.getValue());
				}
				*/
			}
	        
		}
		catch(Exception ex)
		{
			System.out.println("DBEngine : getLowAgent : Error " + ex.toString());
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
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"add plugin");
		me.setParam("src_region", region);
		me.setParam("src_agent", "external");
		me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		me.setParam("controllercmd", "regioncmd");
		me.setParam("configtype", "pluginadd");
		me.setParam("configparams",configParams);
		return me;
	}
	
	public MsgEvent downloadPlugin(String region, String agent, String plugin, String pluginurl, boolean forceDownload)
	{
		MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG,region,null,null,"download plugin");
		me.setParam("src_region", region);
		me.setParam("src_agent", "external");
		me.setParam("dst_region", region);
		me.setParam("dst_agent", agent);
		me.setParam("controllercmd", "regioncmd");
		me.setParam("configtype", "plugindownload");
		me.setParam("plugin", plugin);
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
                        System.out.println(listOfFiles[i].toPath());
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
            System.out.println(ex.toString());
            pluginFiles = null;
        }
        return pluginFiles;
    }

    public static String getPluginName(String jarFile) //This should pull the version information from jar Meta data
    {
        String version;
        try{
            //String jarFile = AgentEngine.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            //System.out.println("JARFILE:" + jarFile);
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
            //System.out.println("JARFILE:" + jarFile);
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

	public static Map<String,String> getPluginFileMap()
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
		        //System.out.println("Found Plugin: " + listOfFiles[i].getName());
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
			System.out.println(ex.toString());
		}
		return null; 
		
	}
		
}



