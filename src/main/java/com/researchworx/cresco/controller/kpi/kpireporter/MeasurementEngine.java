package com.researchworx.cresco.controller.kpi.kpireporter;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.lang.Nullable;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

public class MeasurementEngine {


    private Launcher plugin;
    private CLogger logger;
    //public CrescoMeterRegistry crescoMeterRegistry;
    private SimpleMeterRegistry crescoMeterRegistry;
    //private Timer msgRouteTimer;

    private Gson gson;
    private Map<String,CMetric> metricMap;

    public MeasurementEngine(Launcher plugin) {
        this.plugin = plugin;
        this.logger = new CLogger(MeasurementEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);

        this.metricMap = new ConcurrentHashMap<>();

        gson = new Gson();

        crescoMeterRegistry = new SimpleMeterRegistry();
        metricInit();

        //logger.error("STARTED M ENGINE");
       // MetricRegistry metricRegistry = new MetricRegistry();
        /*
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain("cresco")
                .build();
        */
        //jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM, HierarchicalNameMapper.DEFAULT, metricRegistry, jmxReporter);

        //crescoMeterRegistry = new CrescoMeterRegistry(plugin,CrescoConfig.DEFAULT, Clock.SYSTEM);

        /*
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        new ClassLoaderMetrics().bindTo(jmxMeterRegistry);
        new JvmMemoryMetrics().bindTo(jmxMeterRegistry);
        new JvmGcMetrics().bindTo(jmxMeterRegistry);
        new ProcessorMetrics().bindTo(jmxMeterRegistry);
        new JvmThreadMetrics().bindTo(jmxMeterRegistry);
        */
        //this.msgRouteTimer = this.jmxMeterRegistry.timer("cresco_message.transaction.time");
        //this.msgRouteTimer = this.crescoMeterRegistry.timer("cresco_message.transaction.time");

        new ClassLoaderMetrics().bindTo(crescoMeterRegistry);
        new JvmMemoryMetrics().bindTo(crescoMeterRegistry);
        //new JvmGcMetrics().bindTo(crescoMeterRegistry);
        new ProcessorMetrics().bindTo(crescoMeterRegistry);
        new JvmThreadMetrics().bindTo(crescoMeterRegistry);

        initInternal();

        /*
        for (Map.Entry<String, CMetric> entry : metricMap.entrySet()) {
            //String name = entry.getKey();
            CMetric metric = entry.getValue();
            //logger.error(metric.name + " " + metric.group + " " + metric.className + " " + metric.type.name());
            logger.info(writeMetricString(metric));

        }
        */

        /*
        for(Meter m : crescoMeterRegistry.getMeters()) {
            logger.error(m.getId().getName() + " " + m.getId().getDescription() + " " + m.getId().getType().name());

        }
        */

    }

    public List<Map<String,String>> getMetricGroupList(String group) {
        List<Map<String,String>> returnList = null;
        try {
            returnList = new ArrayList<>();

            for (Map.Entry<String, CMetric> entry : metricMap.entrySet()) {
                //String name = entry.getKey();
                CMetric metric = entry.getValue();
                //logger.error(metric.name + " " + metric.group + " " + metric.className + " " + metric.type.name());
                returnList.add(writeMetricMap(metric));
            }
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return returnList;
    }

    public Map<String,String> writeMetricMap(CMetric metric) {

        Map<String,String> metricValueMap = null;

        try {
            metricValueMap = new HashMap<>();

            if (Meter.Type.valueOf(metric.className) == Meter.Type.GAUGE) {

                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("value",String.valueOf(crescoMeterRegistry.get(metric.name).gauge().value()));

            } else if (Meter.Type.valueOf(metric.className) == Meter.Type.TIMER) {
                TimeUnit timeUnit = crescoMeterRegistry.get(metric.name).timer().baseTimeUnit();
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("mean",String.valueOf(crescoMeterRegistry.get(metric.name).timer().mean(timeUnit)));
                metricValueMap.put("max",String.valueOf(crescoMeterRegistry.get(metric.name).timer().max(timeUnit)));
                metricValueMap.put("totaltime",String.valueOf(crescoMeterRegistry.get(metric.name).timer().totalTime(timeUnit)));
                metricValueMap.put("count",String.valueOf(crescoMeterRegistry.get(metric.name).timer().count()));

            } else  if (Meter.Type.valueOf(metric.className) == Meter.Type.COUNTER) {
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                try {
                    metricValueMap.put("count", String.valueOf(crescoMeterRegistry.get(metric.name).functionCounter().count()));
                } catch (Exception ex) {
                    metricValueMap.put("count", String.valueOf(crescoMeterRegistry.get(metric.name).counter().count()));
                }

            } else {
                logger.error("NO WRITER FOUND " + metric.className);
            }

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return metricValueMap;
    }

    private void initInternal() {

        Map<String,String> internalMap = new HashMap<>();

        internalMap.put("jvm.memory.max", "controller");
        internalMap.put("system.load.average.1m", "controller");
        internalMap.put("jvm.memory.used", "controller");
        internalMap.put("jvm.memory.committed", "controller");
        internalMap.put("jvm.buffer.memory.used", "controller");
        internalMap.put("system.cpu.count", "controller");
        internalMap.put("jvm.threads.daemon", "controller");
        internalMap.put("system.cpu.usage", "controller");
        internalMap.put("jvm.threads.live", "controller");
        internalMap.put("jvm.threads.peak", "controller");
        internalMap.put("process.cpu.usage", "controller");
        internalMap.put("jvm.classes.loaded", "controller");
        internalMap.put("jvm.memory.committed", "controller");
        internalMap.put("jvm.classes.unloaded", "controller");
        internalMap.put("jvm.buffer.count", "controller");

        internalMap.put("jvm.buffer.total.capacity", "controller");
        internalMap.put("jvm.buffer.count", "controller");
        internalMap.put("jvm.memory.committed", "controller");

        for (Map.Entry<String, String> entry : internalMap.entrySet()) {
            String name = entry.getKey();
            String group = entry.getValue();
            setExisting(name,group);
        }



    }

    public void setExisting(String name, String group) {

        Meter m = crescoMeterRegistry.get(name).meter();

        if(m != null) {
            metricMap.put(name,new CMetric(name,m.getId().getDescription(),group,m.getId().getType().name()));
        }

    }

    public Boolean setTimer(String name, String description, String group) {

        if(metricMap.containsKey(name)) {
            return false;
        } else {
            Timer timer = Timer.builder(name).description(description).register(crescoMeterRegistry);
            //Timer timer = this.crescoMeterRegistry.timer(plugin.getPluginID() + "_" + name);
            metricMap.put(name,new CMetric(name,description,group,"TIMER"));
            return true;
        }
    }

    public Timer getTimer(String name) {
        if(metricMap.containsKey(name)) {
            return this.crescoMeterRegistry.timer(name);
        } else {
            return null;
        }
    }

    public void updateTimer(String name, long timeStamp) {
        getTimer(name).record(System.nanoTime() - timeStamp, TimeUnit.NANOSECONDS);
    }

    public Gauge getGauge(String name) {
        if(metricMap.containsKey(name)) {
            return this.crescoMeterRegistry.get(name).gauge();
        } else {
            return null;
        }
    }

    public Gauge getGaugeRaw(String name) {
            return this.crescoMeterRegistry.get(name).gauge();
    }

    private void metricInit() {

        setTimer("message.transaction.time", "The timer for messages", "controller");


    }



}
