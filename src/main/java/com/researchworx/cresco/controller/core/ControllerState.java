package com.researchworx.cresco.controller.core;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

public class ControllerState {

	private Mode currentMode  = Mode.COMMINIT;
	private Launcher mainPlugin;
	private CLogger logger;

	public ControllerState(Launcher plugin)
	{
		this.mainPlugin = plugin;
		this.logger = new CLogger(ControllerState.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
	}

	public static enum Mode {
		COMMINIT,
		AGENT,
		REGION,
		GLOBAL;

		private Mode() {

		}
	}

}
