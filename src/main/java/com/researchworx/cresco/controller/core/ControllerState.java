package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

public class ControllerState {

	private Mode currentMode  = Mode.PRE_INIT;
	private String currentDesc;
	private Launcher mainPlugin;
	private CLogger logger;

	private String globalAgent;
	private String globalRegion;
	private String regionalAgent;
	private String regionalRegion;


	public ControllerState(Launcher plugin)
	{
		this.mainPlugin = plugin;
		this.logger = new CLogger(ControllerState.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
	}

	public boolean isActive() {
		if((currentMode == Mode.AGENT) || (currentMode == Mode.GLOBAL) || (currentMode == Mode.REGION_GLOBAL)) {
			return true;
		} else {
			return false;
		}
	}

	public String getControllerState() {
		return currentMode.toString();
	}

	public String getCurrentDesc() {
		return  currentDesc;
	}

	public boolean isRegionalController() {
		boolean isRC = false;

		if((currentMode.toString().startsWith("REGION")) || isGlobalController()) {
			isRC = true;
		}
		return isRC;
	}

	public boolean isGlobalController() {
		boolean isGC = false;

		if(currentMode.toString().startsWith("GLOBAL")) {
			isGC = true;
		}
		return isGC;
	}

	public String getControllerId() {
		return "plugin/0";
	}

	public String getGlobalAgent() {
		return globalAgent;
	}

	public String getGlobalRegion() {
		return globalRegion;
	}

	public String getRegionalAgent() {
		return regionalAgent;
	}

	public String getRegionalRegion() {
		return regionalRegion;
	}

	public String getGlobalControllerPath() {
		if(isRegionalController()) {
			return globalRegion + "_" + globalAgent;
		} else {
			return null;
		}
	}

	public String getRegionalControllerPath() {
		if(isRegionalController()) {
			return regionalRegion + "_" + regionalAgent;
		} else {
			return null;
		}
	}

	//private String agentpath;

	public String getAgentPath() {
		return mainPlugin.getRegion() + "_" + mainPlugin.getAgent();
	}

	/*

		public void setRegionalController(boolean regionalController) {
		isRegionalController = regionalController;
	}

	public String[] getRegionalController() {
		return this.regionalController;
	}
	public void setRegionalController(String controllerRegion, String controllerAgent) {

		logger.trace("SETTING REGIONAL CONTROLLER PATH : OLD : " + this.regionalController);
		this.regionalController = new String[2];
		this.regionalController[0] = controllerRegion;
		this.regionalController[1] = controllerAgent;
		logger.trace("SETTING REGIONAL CONTROLLER PATH : NEW : " + this.regionalController);

	}

	public String[] getGlobalController() {
		return this.globalController;
	}
	public void setGlobalController(String controllerRegion, String controllerAgent) {

		logger.trace("SETTING GLOBAL CONTROLLER PATH : OLD : " + this.globalController);
		this.globalController = new String[2];
		this.globalController[0] = controllerRegion;
		this.globalController[1] = controllerAgent;
		logger.trace("SETTING GLOBAL CONTROLLER PATH : NEW : " + this.globalController);


	}

   	public boolean isGlobalController() {
		return this.isGlobalController;
	}
	public void setGlobalController(boolean globalController) {
		isGlobalController = globalController;
	}


    */

	public void setAgentSuccess(String regionalRegion, String regionalAgent, String desc) {
		currentMode = Mode.AGENT_INIT;
		currentDesc = desc;
		this.globalAgent = null;
		this.globalRegion = null;
		this.regionalRegion = regionalRegion;
		this.regionalAgent = regionalAgent;
	}

	public void setAgentInit(String desc) {
		currentMode = Mode.AGENT_INIT;
		currentDesc = desc;
	}

	public void setRegionInit(String desc) {
		currentMode = Mode.REGION_INIT;
		currentDesc = desc;
	}

	public void setRegionGlobalInit(String desc) {
		currentMode = Mode.REGION_GLOBAL_INIT;
		currentDesc = desc;
		this.globalAgent = null;
		this.globalRegion = null;
		this.regionalAgent = mainPlugin.getAgent();
		this.regionalRegion = mainPlugin.getRegion();
	}

	public void setRegionFailed(String desc) {
		currentMode = Mode.REGION_FAILED;
		currentDesc = desc;
		this.globalAgent = null;
		this.globalRegion = null;
		this.regionalAgent = null;
		this.regionalRegion = null;
	}

	public void setGlobalSuccess(String desc) {
		currentMode = Mode.GLOBAL;
		currentDesc = desc;
		this.globalAgent = mainPlugin.getAgent();
		this.globalRegion = mainPlugin.getRegion();
		this.regionalAgent = mainPlugin.getAgent();
		this.regionalRegion = mainPlugin.getRegion();
	}

	public void setRegionalGlobalSuccess(String globalRegion, String globalAgent, String desc) {
		currentMode = Mode.REGION_GLOBAL;
		currentDesc = desc;
		this.globalRegion = globalRegion;
		this.globalAgent = globalAgent;
	}

	public void setRegionalGlobalFailed(String desc) {
		currentMode = Mode.REGION_GLOBAL_FAILED;
		currentDesc = desc;
		globalAgent = null;
		globalRegion = null;
	}

	public static enum Mode {
		PRE_INIT,
		AGENT_INIT,
		AGENT,
		AGENT_SHUTDOWN,
		REGION_INIT,
		REGION_FAILED,
		REGION_GLOBAL_INIT,
		REGION_GLOBAL_FAILED,
		REGION_GLOBAL,
		REGION_SHUTDOWN,
		GLOBAL_INIT,
		GLOBAL,
		GLOBAL_FAILED,
		GLOBAL_SHUTDOWN;

		private Mode() {

		}
	}

}
