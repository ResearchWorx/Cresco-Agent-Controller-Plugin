package com.researchworx.cresco.controller.globalhttp;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.library.utilities.CLogger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;



@Path("/PLUGINS")
public class webDownload {


	private static Launcher mainPlugin;
	private static CLogger logger;

	private static GlobalCommandExec gce;

	public static void connectPlugin(Launcher plugin) {
		mainPlugin = plugin;
		logger = new CLogger(webDownload.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		gce = new GlobalCommandExec(plugin);
	}
	
	@GET
	@Path("{subResources:.*}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getfile(@PathParam("subResources") String subResources) 
	{
		//System.out.println("Requesting file: " + subResources);
		//	subResources = "/" + subResources;
		InputStream in = null;
		try{

            //File jarLocation = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
            //String parentDirName = jarLocation.getParent(); // to get the parent dir name
            String pluginDirectory = null;
            if(mainPlugin.getConfig().getStringParam("localpluginrepo") != null) {
                pluginDirectory = mainPlugin.getConfig().getStringParam("localpluginrepo");
            }
            else {
                //if not listed use the controller directory
                File jarLocation = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
                pluginDirectory = jarLocation.getParent(); // to get the parent dir name
            }

			//InputStream in = new FileInputStream(theFile);
            if((pluginDirectory.endsWith("/") || (pluginDirectory.endsWith("\\")))) {
                pluginDirectory = pluginDirectory.substring(0,pluginDirectory.length() - 1);
            }
			File pluginFile = new File(pluginDirectory + "/" + subResources);

			if(pluginFile.exists())
			{
				in = new FileInputStream(pluginFile);
			}
			else
			{
				System.out.println("File not found: " + subResources);
			}
			//in = getClass().getResourceAsStream(subResources);
			
			if(in == null) 
			{
				
				//in = getClass().getResourceAsStream("/404.html");
				return Response.status(Response.Status.NOT_FOUND).build();
		    }
		}
		catch(Exception ex)
		{
			System.out.println(ex.toString());
			//in = getClass().getResourceAsStream("/500.html");
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
	    
		}
		
		if(subResources.endsWith(".jar"))
		{
			return Response.ok(in, "application/java-archive").build();
		}
		else
		{
			return Response.ok(in, MediaType.APPLICATION_OCTET_STREAM)
					.header("Content-Disposition", "attachment; filename=\"" + "somefile" + "\"" ) //optional
					.build();
		}
	}

 
}

