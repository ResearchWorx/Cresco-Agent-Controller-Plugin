package com.researchworx.cresco.controller.kpi.kpireporter;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.meter.CrescoConfig;
import com.researchworx.cresco.controller.meter.CrescoMeterRegistry;
import com.researchworx.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;

public class MeasurementEngine {

    private Launcher plugin;
    private CLogger logger;
    //public CrescoMeterRegistry crescoMeterRegistry;
    public SimpleMeterRegistry crescoMeterRegistry;
    private Timer msgRouteTimer;

    public MeasurementEngine(Launcher plugin) {
        this.plugin = plugin;
        this.logger = new CLogger(MeasurementEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);

        //logger.error("STARTED M ENGINE");
       // MetricRegistry metricRegistry = new MetricRegistry();
        /*
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain("cresco")
                .build();
        */
        //jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM, HierarchicalNameMapper.DEFAULT, metricRegistry, jmxReporter);

        //crescoMeterRegistry = new CrescoMeterRegistry(plugin,CrescoConfig.DEFAULT, Clock.SYSTEM);
        crescoMeterRegistry = new SimpleMeterRegistry();

        //Metrics.globalRegistry.



        //this.jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

        /*
        new ClassLoaderMetrics().bindTo(jmxMeterRegistry);
        new JvmMemoryMetrics().bindTo(jmxMeterRegistry);
        new JvmGcMetrics().bindTo(jmxMeterRegistry);
        new ProcessorMetrics().bindTo(jmxMeterRegistry);
        new JvmThreadMetrics().bindTo(jmxMeterRegistry);
        */

        //this.msgRouteTimer = this.jmxMeterRegistry.timer("cresco_message.transaction.time");
        this.msgRouteTimer = this.crescoMeterRegistry.timer("cresco_message.transaction.time");

    }

    public void updateMsgRouteTimer(long timeStamp) {
        msgRouteTimer.record(System.nanoTime() - timeStamp, TimeUnit.NANOSECONDS);
    }

}
