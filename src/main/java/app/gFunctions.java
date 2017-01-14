package app;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.List;
import java.util.UUID;


public class gFunctions {

	private Launcher plugin;
	private CLogger logger;

	public gFunctions(Launcher plugin)
	{
		this.logger = new CLogger(gFunctions.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
		this.plugin = plugin;

	}

	public String gPipelineSubmit(String pipelineJSON)
	{
		//boolean isSubmitted = false;
        String returnGpipeline = null;

		try {
			System.out.println(pipelineJSON);
			gPayload gpay = gPayLoadFromJson(pipelineJSON);
			gpay.pipeline_id = UUID.randomUUID().toString();
			pipelineJSON = JsonFromgPayLoad(gpay);
			gpay = plugin.getGDB().dba.createPipelineRecord(pipelineJSON, gpay);
			//add pipeline submission record
			//gpay = Launcher.graphDB.createPipelineRecord(cookie, message, gpay);
			//add nodes and edges
            //isSubmitted = true;

		}
		catch(Exception ex) {
			logger.error("gPipelineSubmit " + ex.getMessage());
		}
		return returnGpipeline;
	}

	private String JsonFromgPayLoad(gPayload gpay)
	{
		Gson gson = new GsonBuilder().create();
		//gPayload me = gson.fromJson(json, gPayload.class);
		//System.out.println(p);
		return gson.toJson(gpay);

	}

	private gPayload gPayLoadFromJson(String json)
	{
		Gson gson = new GsonBuilder().create();
		gPayload me = gson.fromJson(json, gPayload.class);
		//System.out.println(p);
		return me;
	}
	  
	}