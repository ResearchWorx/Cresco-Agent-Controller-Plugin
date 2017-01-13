package com.researchworx.cresco.controller.globalcontroller;


import app.gFunctions;
import com.researchworx.cresco.controller.core.Launcher;
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
import java.util.jar.*;

public class GlobalCommandExec {

	private Launcher plugin;
	private CLogger logger;
	private gFunctions gfunc;

	public GlobalCommandExec(Launcher plugin)
	{
		this.logger = new CLogger(GlobalCommandExec.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
		this.plugin = plugin;
        this.gfunc = new gFunctions(plugin);
	}
	
	public MsgEvent cmdExec(MsgEvent ce)
	{
			if(ce.getMsgType() == MsgEvent.Type.CONFIG)
			{
				if(ce.getParam("globalcmd") != null)
				{
					if(ce.getParam("globalcmd").equals("addplugin"))
					{
						if((ce.getParam("inode_id") != null) && (ce.getParam("resource_id") != null) && (ce.getParam("configparams") != null))
						{
							
							if(plugin.getGDB().gdb.getINodeId(ce.getParam("resource_id"),ce.getParam("inode_id")) == null)
							{
								if(plugin.getGDB().gdb.addINode(ce.getParam("resource_id"),ce.getParam("inode_id")) != null)
								{
									if((plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_code","0")) &&
										(plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_desc","iNode Scheduled.")) &&
										(plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"configparams",ce.getParam("configparams"))))
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
							if(plugin.getGDB().gdb.getINodeId(ce.getParam("resource_id"),ce.getParam("inode_id")) != null)
							{
								if((plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_code","10")) &&
								(plugin.getGDB().gdb.setINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_desc","iNode scheduled for removal.")))
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
								String status_code = plugin.getGDB().gdb.getINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_code");
								String status_desc = plugin.getGDB().gdb.getINodeParam(ce.getParam("resource_id"),ce.getParam("inode_id"),"status_desc");
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
								String pluginList = null;;
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
								System.out.println("pluginList=" + pluginList);
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
                            if(ce.getParam("gpipeline") != null) {
                                String pipelineJSON = ce.getParam("gpipeline");
                                logger.info("*" + pipelineJSON + "*");
                                if(gfunc.gPipelineSubmit(pipelineJSON)) {
                                    logger.info("Pipeline Submitted");
                                }
                                else {
                                    logger.error("Pipeline submission failed.");
                                }
                            }
                        }
                        catch(Exception ex)
                        {
                            logger.error("gpipelinesubmit " + ex.getMessage());
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
				
				plugin.getGDB().gdb.updateKPI(region, agent, pluginid, resource_id, inode_id, params);
				
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
	
	public static Map<String,String> getPluginParamMap(String jarFileName)
	{
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
	
	public static String getPluginParams(String jarFileName)
	{
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


	public List<String> getPluginFiles()
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
