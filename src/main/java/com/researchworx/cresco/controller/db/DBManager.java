package com.researchworx.cresco.controller.db;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.security.spec.ECField;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

public class DBManager implements Runnable  {
	private Launcher plugin;
	private CLogger logger;
	private Timer timer;
	private BlockingQueue<String> importQueue;
	public DBManager(Launcher plugin, BlockingQueue<String> importQueue) {
		//importQueue = new LinkedBlockingQueue<>();
		this.importQueue = importQueue;
		this.logger = new CLogger(DBManager.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		logger.debug("DB Manager initialized");
		this.plugin = plugin;
		//timer = new Timer();
		//timer.scheduleAtFixedRate(new DBWatchDog(logger), 500, 15000);//remote
	}

	public void importRegionalDB(String importData) {

		importQueue.offer(importData);

	}

	public void shutdown() {
		logger.debug("DB Manager shutdown initialized");
	}

	private void processDBImports() {
		try {
			while (!importQueue.isEmpty()) {
				plugin.getGDB().gdb.setDBImport(importQueue.take());
			}
		} catch(Exception ex) {
			logger.error("processDBImports() Error : " + ex.toString());
		}
	}

	public void run() {
		logger.info("Initialized");
		this.plugin.setDBManagerActive(true);
		while(this.plugin.isDBManagerActive()) {
			try {

				processDBImports();
				Thread.sleep(1000);

			} catch (Exception ex) {
				logger.error("Run {}", ex.getMessage());
                ex.printStackTrace();
			}
		}
		//timer.cancel();
		logger.debug("Broker Manager has shutdown");
	}

	class DBWatchDog extends TimerTask {
		//private final Logger logger = LoggerFactory.getLogger(BrokerWatchDog.class);
        private CLogger logger;

        public DBWatchDog(CLogger logger) {
            this.logger = logger;
        }
		public void run() {
        	//Do Something
		}
	}
}