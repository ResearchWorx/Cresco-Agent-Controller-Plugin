package com.researchworx.cresco.controller.globalcontroller;


import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalscheduler.PollRemovePipeline;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.jar.*;

public class GlobalCommandExec {

	private Launcher plugin;
	private CLogger logger;
	private ExecutorService removePipelineExecutor;

	public GlobalCommandExec(Launcher plugin)
	{
		this.logger = new CLogger(GlobalCommandExec.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		removePipelineExecutor = Executors.newFixedThreadPool(1);
    }

	public MsgEvent execute(MsgEvent ce) {

		/*
		Region:
  		Param - "action":"listregions"
  		Response:
    	Param - "regionslist":"{"regions":[{"name":"blah","agents":#}]}"
		*/

			if(ce.getMsgType() == MsgEvent.Type.EXEC) {
				switch (ce.getParam("action")) {
					case "listregions":
                        ce.setParam("regionslist",plugin.getGDB().getRegionList());
                        logger.trace("list regions return : " + ce.getParams().toString());
                        break;
                    case "listagents":
                        String actionRegionAgents = null;

                        if(ce.getParam("action_region") != null) {
                            actionRegionAgents = ce.getParam("action_region");
                        }
                        ce.setParam("agentslist",plugin.getGDB().getAgentList(actionRegionAgents));
                        logger.trace("list agents return : " + ce.getParams().toString());
                        break;
                    case "listplugins":
                        String actionRegionPlugins = null;
                        String actionAgentPlugins = null;

                        if((ce.getParam("action_region") != null) && (ce.getParam("action_agent") != null)) {
                            actionRegionPlugins = ce.getParam("action_region");
                            actionAgentPlugins = ce.getParam("action_agent");
                        } else if((ce.getParam("action_region") != null) && (ce.getParam("action_agent") == null)) {
                            actionRegionPlugins = ce.getParam("action_region");
                        }
                        ce.setParam("pluginslist",plugin.getGDB().getPluginList(actionRegionPlugins, actionAgentPlugins));
                        logger.trace("list plugins return : " + ce.getParams().toString());
                        break;
                    case "plugininfo":
                        ce.setParam("plugininfo",plugin.getGDB().getPluginInfo(ce.getParam("action_region"),ce.getParam("action_agent"),ce.getParam("action_plugin")));
                        logger.trace("plugins info return : " + ce.getParams().toString());
                        break;
                    case "resourceinfo":
                        String actionRegionResourceInfo = null;
                        String actionAgentResourceInfo = null;

                        if((ce.getParam("action_region") != null) && (ce.getParam("action_agent") != null)) {
                            actionRegionResourceInfo = ce.getParam("action_region");
                            actionAgentResourceInfo = ce.getParam("action_agent");
                        } else if((ce.getParam("action_region") != null) && (ce.getParam("action_agent") == null)) {
                            actionRegionResourceInfo = ce.getParam("action_region");
                        }
                        ce.setParam("pluginslist",plugin.getGDB().getResourceInfo(actionRegionResourceInfo, actionAgentResourceInfo));
                        logger.trace("list plugins return : " + ce.getParams().toString());
                        break;

                    default:
						logger.debug("Unknown configtype found: {}", ce.getParam("action"));
						return null;
				}
				return ce;
			}
			else if(ce.getMsgType() == MsgEvent.Type.CONFIG)
			{
                if(ce.getParam("action") != null) {
                    switch (ce.getParam("action")) {
                        case "disable":
                            logger.debug("CONFIG : AGENTDISCOVER REMOVE: Region:" + ce.getParam("src_region") + " Agent:" + ce.getParam("src_agent"));
                            logger.trace("Message Body [" + ce.getMsgBody() + "] [" + ce.getParams().toString() + "]");
                            plugin.getGDB().removeNode(ce);
                            break;
                        case "enable":
                            logger.debug("CONFIG : AGENTDISCOVER ADD: Region:" + ce.getParam("src_region") + " Agent:" + ce.getParam("src_agent"));
                            logger.trace("Message Body [" + ce.getMsgBody() + "] [" + ce.getParams().toString() + "]");
                            plugin.getGDB().addNode(ce);
                            break;
                        case "regionalimport":
                            logger.debug("CONFIG : regionalimport message type found");
                            logger.debug(ce.getParam("exportdata"));
                            if(plugin.getGDB().gdb.setDBImport(ce.getParam("exportdata"))) {
                                logger.debug("Database Imported.");
                            }
                            else {
                                logger.debug("Database Import Failed!");
                            }
                            break;
                        default:
                            logger.debug("Unknown configtype found: {}", ce.getParam("action"));
                            return null;
                    }
                }
					else if(ce.getParam("globalcmd").equals("addplugin"))
					{
						if((ce.getParam("inode_id") != null) && (ce.getParam("resource_id") != null) && (ce.getParam("configparams") != null))
						{
							
							if(plugin.getGDB().dba.getpNodeINode(ce.getParam("inode_id")) == null)
							{
								if(plugin.getGDB().dba.addINode(ce.getParam("resource_id"),ce.getParam("inode_id")) != null)
								{
									if((plugin.getGDB().dba.setINodeParam(ce.getParam("inode_id"),"status_code","0")) &&
										(plugin.getGDB().dba.setINodeParam(ce.getParam("inode_id"),"status_desc","iNode Scheduled.")) &&
										(plugin.getGDB().dba.setINodeParam(ce.getParam("inode_id"),"configparams",ce.getParam("configparams"))))
									{
										ce.setParam("status_code","0");
										ce.setParam("status_desc","iNode Scheduled");
										//ControllerEngine.resourceScheduleQueue.offer(ce);
									}
									else
									{
										ce.setParam("status_code","1");
										ce.setParam("status_desc","Could not set iNode params");
									}
								}
								else
								{
									ce.setParam("status_code","1");
									ce.setParam("status_desc","Could not create iNode_id!");	
								}
							}
							else
							{
								ce.setParam("status_code","1");
								ce.setParam("status_desc","iNode_id already exist!");
							}
						}
						else
						{
							ce.setParam("status_code","1");
							ce.setParam("status_desc","No iNode_id found in payload!");	
						}
							
						return ce;
					}
					else if(ce.getParam("globalcmd").equals("removeplugin"))
					{
						if((ce.getParam("inode_id") != null) && (ce.getParam("resource_id") != null))
						{
							if(plugin.getGDB().dba.getpNodeINode(ce.getParam("inode_id")) != null)
							{
								if((plugin.getGDB().dba.setINodeParam(ce.getParam("inode_id"),"status_code","10")) &&
								(plugin.getGDB().dba.setINodeParam(ce.getParam("inode_id"),"status_desc","iNode scheduled for removal.")))
								{
									ce.setParam("status_code","10");
									ce.setParam("status_desc","iNode scheduled for removal.");
									//ControllerEngine.resourceScheduleQueue.offer(ce);
								}
								else
								{
									ce.setParam("status_code","1");
									ce.setParam("status_desc","Could not set iNode params");
								}
							}
							else
							{
								ce.setParam("status_code","1");
								ce.setParam("status_desc","iNode_id does not exist in DB!");	
							}
						}
						else
						{
							ce.setParam("status_code","1");
							ce.setParam("status_desc","No resource_id or iNode_id found in payload!");	
						}
							
						return ce;
					}
					else if(ce.getParam("globalcmd").equals("plugininfo"))
					{
						try
						{
							if(ce.getParam("plugin_id") != null)
							{
								String plugin_id = ce.getParam("plugin_id");
								List<String> pluginFiles = getPluginFiles();
							
								if(pluginFiles != null)
								{
									for (String pluginPath : pluginFiles) 
									{
										String found_plugin_id = getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath);
										if(plugin_id.equals(found_plugin_id))
										{
											String params = getPluginParams(pluginPath);
											if(params != null)
											{
												System.out.println("Found Plugin: " + plugin_id);
												ce.setParam("node_name",getPluginName(pluginPath));
												ce.setParam("node_id",plugin_id);
												ce.setParam("params",params);
											}
											
										}
									}
								}
								else
								{
									ce.setMsgBody("Plugin does not exist");
								}
							}
						}
						catch(Exception ex)
						{
							System.out.println(ex.toString());
							ce.setMsgBody("Error: " + ex.toString());
						}
						return ce;   
					}
					else if(ce.getParam("globalcmd").equals("getenvstatus"))
					{
						try
						{
							if((ce.getParam("environment_id") != null) && (ce.getParam("environment_value") != null))
							{
								String indexName = ce.getParam("environment_id");
								String indexValue = ce.getParam("environment_value");
								
								List<String> envNodeList = plugin.getGDB().gdb.getANodeFromIndex(indexName, indexValue);
								ce.setParam("count",String.valueOf(envNodeList.size()));
							}
							else
							{
								ce.setParam("count","unknown");
							}					
							
						}
						catch(Exception ex)
						{
							ce.setParam("count","unknown");
						}
						return ce;
					}
					else if(ce.getParam("globalcmd").equals("getpluginstatus"))
					{
						try
						{
							if((ce.getParam("inode_id") != null) && (ce.getParam("resource_id") != null))
							{
								String status_code = plugin.getGDB().dba.getINodeParam(ce.getParam("inode_id"),"status_code");
								String status_desc = plugin.getGDB().dba.getINodeParam(ce.getParam("inode_id"),"status_desc");
								if((status_code != null) && (status_desc != null))
								{
									ce.setParam("status_code",status_code);
									ce.setParam("status_desc",status_desc);
								}
								else
								{
									ce.setParam("status_code","1");
									ce.setParam("status_desc","Could not read iNode params");
								}
							}
							else
							{
								ce.setParam("status_code","1");
								ce.setParam("status_desc","No iNode_id found in payload!");	
							}					
							
						}
						catch(Exception ex)
						{
							ce.setParam("status_code","1");
							ce.setParam("status_desc",ex.toString());	
						}
						return ce;
					}
					else if(ce.getParam("globalcmd").equals("resourceinventory"))
					{
						try
						{
						    Map<String,String> resourceTotal = plugin.getGDB().getResourceTotal();


                            if(resourceTotal != null)
							{
							    logger.trace(resourceTotal.toString());
								ce.setParam("resourceinventory", resourceTotal.toString());
								ce.setMsgBody("Inventory found.");
							}
							else
							{
								ce.setMsgBody("No plugin directory exist to inventory");
							}
						}
						catch(Exception ex)
						{
							System.out.println(ex.toString());
							ce.setMsgBody("Error: " + ex.toString());
						}
						return ce;   
					}
					else if(ce.getParam("globalcmd").equals("plugininventory"))
					{
					    try
						{
							List<String> pluginFiles = getPluginFiles();

                            if(pluginFiles != null)
							{
								String pluginList = null;
								for (String pluginPath : pluginFiles) 
								{
									if(pluginList == null)
									{
										pluginList = getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath) + ",";
									}
									else
									{
										pluginList = pluginList + getPluginName(pluginPath) + "=" + getPluginVersion(pluginPath) + ",";
									}
								}
								pluginList = pluginList.substring(0, pluginList.length() - 1);
								ce.setParam("pluginlist", pluginList);
								ce.setMsgBody("There were " + pluginFiles.size() + " plugins found.");
							}
							else
							{
								ce.setMsgBody("No plugin directory exist to inventory");
							}
						}
						catch(Exception ex)
						{
							System.out.println(ex.toString());
							ce.setMsgBody("Error: " + ex.toString());
						}
						return ce;   
					}
                    else if(ce.getParam("globalcmd").equals("gpipelinesubmit"))
                    {
                        try
                        {
                            if((ce.getParam("gpipeline") != null) && (ce.getParam("tenant_id") != null)) {
                                String pipelineJSON = ce.getParam("gpipeline");
                                String tenantID = ce.getParam("tenant_id");
                                if(ce.getParam("gpipeline_compressed") != null) {
									boolean isCompressed = Boolean.parseBoolean(ce.getParam("gpipeline_compressed"));
									if(isCompressed) {
										pipelineJSON = plugin.getGDB().gdb.stringUncompress(pipelineJSON);
									}
                                    logger.debug("Pipeline Compressed " + isCompressed + " " + ce.getParam("gpipeline"));
                                    logger.debug("*" + pipelineJSON + "*");

                                }
                                gPayload gpay = plugin.getGDB().dba.createPipelineRecord(tenantID, pipelineJSON);
                                //String returnGpipeline = plugin.getGDB().dba.JsonFromgPayLoad(gpay);
                                ce.setParam("gpipeline_id",gpay.pipeline_id);
                            }
                        }
                        catch(Exception ex)
                        {
                            logger.error("gpipelinesubmit " + ex.getMessage());
                        }
                        return ce;
                    }
                    else if(ce.getParam("globalcmd").equals("getgpipeline"))
                    {
                        try
                        {
                            if(ce.getParam("pipeline_id") != null) {
                                String pipelineId = ce.getParam("pipeline_id");
                                String returnGetGpipeline = plugin.getGDB().dba.getPipeline(pipelineId);
                                ce.setParam("gpipeline",returnGetGpipeline);
                            }
                        }
                        catch(Exception ex)
                        {
                            logger.error("getgpipeline " + ex.getMessage());
                        }
                        return ce;
                    }
                    else if(ce.getParam("globalcmd").equals("getgpipelinelist"))
                    {
                        try
                        {
                            StringBuilder pipelineString = new StringBuilder();
                            List<String> pipelines = plugin.getGDB().dba.getPipelineIdList();
                            for(String pipelineId :pipelines) {
                                pipelineString.append(pipelineId + ",");
                            }
                            if(pipelineString.length() > 0) {
                                pipelineString.deleteCharAt(pipelineString.length() - 1);
                            }
                            ce.setParam("gpipeline_ids",pipelineString.toString());

                        }
                        catch(Exception ex)
                        {
                            logger.error("getgpipelinelist " + ex.getMessage());
                        }
                        return ce;
                    }
                    else if(ce.getParam("globalcmd").equals("getgpipelinestatus"))
                    {
                        try
                        {
                            if(ce.getParam("pipeline_id") != null) {
                                String pipelineId = ce.getParam("pipeline_id");
                                int pipelineStatus = plugin.getGDB().dba.getPipelineStatus(pipelineId);
                                ce.setParam("status_code",String.valueOf(pipelineStatus));
                            }
                        }
                        catch(Exception ex)
                        {
                            logger.error("getgpipelinelist " + ex.getMessage());
                        }
                        return ce;
                    }
                    else if(ce.getParam("globalcmd").equals("gpipelineremove"))
                    {
                        try
                        {
                            if(ce.getParam("pipeline_id") != null) {
                                String pipelineId = ce.getParam("pipeline_id");
                                removePipelineExecutor.execute(new PollRemovePipeline(plugin, pipelineId));
                                /*
                                List<String> iNodeList = plugin.getGDB().dba.getresourceNodeList(pipelineId,null);

                                for(String iNodeId : iNodeList) {

                                    logger.info("removing iNode " + iNodeId);
                                    MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "add application node");

                                    me.setParam("globalcmd", "removeplugin");
                                    me.setParam("inode_id", iNodeId);
                                    me.setParam("resource_id", pipelineId);
                                    //ghw.resourceScheduleQueue.offer(me);
                                    plugin.getResourceScheduleQueue().offer(me);

                                }
                                */

                                ce.setParam("isremoved","true");
                            }
                        }
                        catch(Exception ex)
                        {
                            logger.error("ggpipelineremove " + ex.getMessage());
                        }
                        return ce;
                    }
					else if(ce.getParam("globalcmd").equals("plugindownload"))
					{
						try
						{
						String baseUrl = ce.getParam("pluginurl");
						if(!baseUrl.endsWith("/"))
						{
							baseUrl = baseUrl + "/";
						}
						
						URL website = new URL(baseUrl + ce.getParam("plugin"));
						ReadableByteChannel rbc = Channels.newChannel(website.openStream());
						
						File jarLocation = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
						String parentDirName = jarLocation.getParent(); // to get the parent dir name
						String pluginDir = parentDirName + "/plugins";
						//check if directory exist, if not create it
						File pluginDirfile = new File(pluginDir);
						if (!pluginDirfile.exists()) {
							if (pluginDirfile.mkdir()) {
								System.out.println("Directory " + pluginDir + " didn't exist and was created.");
							} else {
								System.out.println("Directory " + pluginDir + " didn't exist and we failed to create it!");
							}
						}
						String pluginFile = parentDirName + "/plugins/" + ce.getParam("plugin");
						boolean forceDownload = false;
						if(ce.getParam("forceplugindownload") != null)
						{
							forceDownload = true;
							System.out.println("Forcing Plugin Download");
						}
						
						File pluginFileObject = new File(pluginFile);
						if (!pluginFileObject.exists() || forceDownload) 
						{
							FileOutputStream fos = new FileOutputStream(parentDirName + "/plugins/" + ce.getParam("plugin"));
							
							fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
							if(pluginFileObject.exists())
							{
								ce.setParam("hasplugin", ce.getParam("plugin"));
								ce.setMsgBody("Downloaded Plugin:" + ce.getParam("plugin"));
								System.out.println("Downloaded Plugin:" + ce.getParam("plugin"));
							}
							else
							{
								ce.setMsgBody("Problem Downloading Plugin:" + ce.getParam("plugin"));
								System.out.println("Problem Downloading Plugin:" + ce.getParam("plugin"));
							}
						}
						else
						{
							ce.setMsgBody("Plugin already exists:" + ce.getParam("plugin"));
							ce.setParam("hasplugin", ce.getParam("plugin"));
							System.out.println("Plugin already exists:" + ce.getParam("plugin"));
						}
						
						}
						catch(Exception ex)
						{
							System.out.println(ex.toString());
							ce.setMsgBody("Error: " + ex.toString());
						}
						return ce;
					}
					

				
			}

			else if(ce.getMsgType() == MsgEvent.Type.WATCHDOG)
			{
				String region = null;
				String agent = null;
				String pluginid = null;
				String resource_id = null;
				String inode_id = null;
				
				region = ce.getParam("src_region");
				agent = ce.getParam("src_agent");
				pluginid = ce.getParam("src_plugin");
				resource_id = ce.getParam("resource_id");
				inode_id = ce.getParam("inode_id");
				
				//clean params for edge
				/*
				ce.removeParam("loop");
				ce.removeParam("isGlobal");
				ce.removeParam("src_agent");
				ce.removeParam("src_region");
				ce.removeParam("src_plugin");
				ce.removeParam("dst_agent");
				ce.removeParam("dst_region");
				ce.removeParam("dst_plugin");
				*/
				Map<String,String> params = ce.getParams();
				
				plugin.getGDB().dba.updateKPI(region, agent, pluginid, resource_id, inode_id, params);
				
				ce.setMsgBody("updatedperf");
                ce.setParam("source","watchdog");

                return ce;
			}
            else if(ce.getMsgType() == MsgEvent.Type.KPI)
            {
                String region = null;
                String agent = null;
                String plugin = null;
                String resource_id = null;
                String inode_id = null;

                region = ce.getParam("src_region");
                agent = ce.getParam("src_agent");
                plugin = ce.getParam("src_plugin");
                resource_id = ce.getParam("resource_id");
                inode_id = ce.getParam("inode_id");

                //clean params for edge
				/*
				ce.removeParam("loop");
				ce.removeParam("isGlobal");
				ce.removeParam("src_agent");
				ce.removeParam("src_region");
				ce.removeParam("src_plugin");
				ce.removeParam("dst_agent");
				ce.removeParam("dst_region");
				ce.removeParam("dst_plugin");
				*/
                Map<String,String> params = ce.getParams();

                //plugin  updatePerf(region, agent, plugin, resource_id, inode_id, params);

                ce.setMsgBody("updatedperf");
                ce.setParam("source","watchdog");
                return ce;
            }
		return null;
	}
	
	public String getPluginName(String jarFile) {
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
	
	public Map<String,String> getPluginParamMap(String jarFileName) {
		Map<String,String> phm = null;
		try 
		{
			phm = new HashMap<String,String>();
	        JarFile jarFile = new JarFile(jarFileName);
            JarEntry je = jarFile.getJarEntry("plugin.conf");
            InputStream in = jarFile.getInputStream(je);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) 
            {
              	line = line.replaceAll("\\s+","");
              	String[] sline = line.split("=");
               	if((sline[0] != null) && (sline[1] != null))
              	{
               		phm.put(sline[0], sline[1]);
                }
            }
            reader.close();
            in.close();
            jarFile.close();
        } 
		catch (IOException e) 
		{
            e.printStackTrace();
        }
		return phm;
	}
	
	public  String getPluginParams(String jarFileName) {
		String params = "";
		try 
		{
			JarFile jarFile = new JarFile(jarFileName);
            JarEntry je = jarFile.getJarEntry("plugin.conf");
            InputStream in = jarFile.getInputStream(je);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) 
            {
              	line = line.replaceAll("\\s+","");
              	if(line.contains("="))
              	{
              		String[] sline = line.split("=");
              		if((sline[0] != null) && (sline[1] != null))
              		{
              			//phm.put(sline[0], sline[1]);
              			if((sline[1].equals("required")) || sline[1].equals("optional"))
              			{
              				params = params + sline[0] + ":" + sline[1] + ",";
              			}
              		}
              	}
            }
            reader.close();
            in.close();
            jarFile.close();
            if(params.length() == 0)
            {
            	params = null;
            }
            else
            {
            	params = params.substring(0,params.length() -1);
            }
        } 
		catch (IOException e) 
		{
			params = null;
            e.printStackTrace();
        }
		return params;
	}
	
	public  String getPluginVersion(String jarFile) {
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

	public List<String> getPluginFiles() {
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
			logger.error("getPluginFiles() " + ex.getMessage());
			pluginFiles = null;
		}
		return pluginFiles;
	}
	
	private String executeCommand(String command) {

		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));

                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}


}
