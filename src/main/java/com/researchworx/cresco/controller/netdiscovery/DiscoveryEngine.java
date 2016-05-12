package com.researchworx.cresco.controller.netdiscovery;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class DiscoveryEngine implements Runnable {
    private Launcher plugin;
    private static Map<NetworkInterface, MulticastSocket> workers = new HashMap<>();


    private static final Logger logger = LoggerFactory.getLogger(DiscoveryEngine.class);

    public DiscoveryEngine(Launcher plugin) {
        logger.trace("Discovery Engine initialized");
        this.plugin = plugin;

    }

    public static void shutdown() {
        for (Map.Entry<NetworkInterface, MulticastSocket> entry : workers.entrySet()) {
            entry.getValue().close();
        }
    }

    public void run() {
        logger.info("Discovery Engine started");
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                Thread thread = new Thread(new DiscoveryEngineWorker(networkInterface, plugin, workers));
                thread.start();
            }
            this.plugin.setDiscoveryActive(true);
            logger.trace("Discovery Engine has shutdown");
        } catch (Exception ex) {
            logger.error("Run {}", ex.getMessage());
        }
    }

	/*public static DiscoveryEngine getInstance() {
        return DiscoveryThreadHolder.INSTANCE;
	}

	private static class DiscoveryThreadHolder {
		private static final DiscoveryEngine INSTANCE = new DiscoveryEngine();
	}*/

}