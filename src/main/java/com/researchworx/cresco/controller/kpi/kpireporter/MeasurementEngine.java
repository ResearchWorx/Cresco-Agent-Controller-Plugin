package com.researchworx.cresco.controller.kpi.kpireporter;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import java.util.concurrent.TimeUnit;

public class MeasurementEngine {

    private Launcher plugin;
    private CLogger logger;
    public JmxMeterRegistry jmxMeterRegistry;
    private Timer msgRouteTimer;

    public MeasurementEngine(Launcher plugin) {
        this.plugin = plugin;
        this.logger = new CLogger(MeasurementEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);


        MetricRegistry metricRegistry = new MetricRegistry();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain("cresco")
                .build();

        jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM, HierarchicalNameMapper.DEFAULT, metricRegistry, jmxReporter);

        //this.jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

        new ClassLoaderMetrics().bindTo(jmxMeterRegistry);
        new JvmMemoryMetrics().bindTo(jmxMeterRegistry);
        new JvmGcMetrics().bindTo(jmxMeterRegistry);
        new ProcessorMetrics().bindTo(jmxMeterRegistry);
        new JvmThreadMetrics().bindTo(jmxMeterRegistry);


        this.msgRouteTimer = this.jmxMeterRegistry.timer("cresco_message.transaction.time");
    }

    public void updateMsgRouteTimer(long timeStamp) {
        msgRouteTimer.record(System.nanoTime() - timeStamp, TimeUnit.NANOSECONDS);
    }

}
