package com.researchworx.cresco.controller.meter;

/**
 * Copyright 2017 Pivotal Software, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.kpi.kpireporter.MeasurementEngine;
import com.researchworx.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.lang.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author Nicolas Portmann
 * @author Jon Schneider
 */
public class CrescoMeterRegistry extends StepMeterRegistry {

    private final CrescoConfig config;
    private boolean checkedForIndexTemplate = false;
    private CLogger logger;

    private Launcher plugin;

    //public CrescoMeterRegistry(CrescoConfig config, Clock clock, NamingConvention namingConvention, ThreadFactory threadFactory) {
    public CrescoMeterRegistry(Launcher plugin, CrescoConfig config, Clock clock, NamingConvention namingConvention, ThreadFactory threadFactory) {
            super(config, clock);
        this.plugin = plugin;
        this.logger = new CLogger(CrescoMeterRegistry.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        this.config().namingConvention(namingConvention);
        this.config = config;
        start(threadFactory);
    }

    public CrescoMeterRegistry(Launcher plugin, CrescoConfig config, Clock clock) {
        this(plugin, config, clock, new CrescoNamingConvention(), Executors.defaultThreadFactory());
    }

    @Override
    protected void publish() {

        for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
            long wallTime = config().clock().wallTime();

            for(Meter m : batch) {

                logger.info(m.getClass().getCanonicalName());



                if (m instanceof TimeGauge) {
                    //return writeGauge((TimeGauge) m, wallTime);
                } else if (m instanceof Gauge) {
                    //return writeGauge((Gauge) m, wallTime);
                } else if (m instanceof Counter) {
                    //return writeCounter((Counter) m, wallTime);
                } else if (m instanceof FunctionCounter) {
                    //return writeCounter((FunctionCounter) m, wallTime);
                } else if (m instanceof Timer) {
                    //return writeTimer((Timer) m, wallTime);
                    writeTimer((Timer) m, wallTime);
                } else if (m instanceof FunctionTimer) {
                    //return writeTimer((FunctionTimer) m, wallTime);
                } else if (m instanceof DistributionSummary) {
                    //return writeSummary((DistributionSummary) m, wallTime);
                } else if (m instanceof LongTaskTimer) {
                    //return writeLongTaskTimer((LongTaskTimer) m, wallTime);
                } else {
                    //return writeMeter(m, wallTime);
                }
            }

            /*
            String bulkPayload = batch.stream().flatMap(m -> {
                if (m instanceof TimeGauge) {
                    return writeGauge((TimeGauge) m, wallTime);
                } else if (m instanceof Gauge) {
                    return writeGauge((Gauge) m, wallTime);
                } else if (m instanceof Counter) {
                    return writeCounter((Counter) m, wallTime);
                } else if (m instanceof FunctionCounter) {
                    return writeCounter((FunctionCounter) m, wallTime);
                } else if (m instanceof Timer) {
                    return writeTimer((Timer) m, wallTime);
                } else if (m instanceof FunctionTimer) {
                    return writeTimer((FunctionTimer) m, wallTime);
                } else if (m instanceof DistributionSummary) {
                    return writeSummary((DistributionSummary) m, wallTime);
                } else if (m instanceof LongTaskTimer) {
                    return writeLongTaskTimer((LongTaskTimer) m, wallTime);
                } else {
                    return writeMeter(m, wallTime);
                }
            }).collect(Collectors.joining("\n")) + "\n";
            */
            //outputStream.write(bulkPayload.getBytes());

        }
    }

    // VisibleForTesting
    Stream<String> writeCounter(Counter counter, long wallTime) {
        return Stream.of(index(counter, wallTime).field("count", counter.count()).build());
    }

    private Stream<String> writeCounter(FunctionCounter counter, long wallTime) {
        return Stream.of(index(counter, wallTime).field("count", counter.count()).build());
    }

    // VisibleForTesting
    Stream<String> writeGauge(Gauge gauge, long wallTime) {
        Double value = gauge.value();
        return value.isNaN() ? Stream.empty() : Stream.of(index(gauge, wallTime).field("value", value).build());
    }

    // VisibleForTesting
    Stream<String> writeGauge(TimeGauge gauge, long wallTime) {
        Double value = gauge.value();
        return value.isNaN() ? Stream.empty() : Stream.of(index(gauge, wallTime).field("value", gauge.value(getBaseTimeUnit())).build());
    }

    private Stream<String> writeTimer(FunctionTimer timer, long wallTime) {
        return Stream.of(index(timer, wallTime)
                .field("count", timer.count())
                .field("sum", timer.totalTime(getBaseTimeUnit()))
                .field("mean", timer.mean(getBaseTimeUnit()))
                .build());
    }

    private Stream<String> writeLongTaskTimer(LongTaskTimer timer, long wallTime) {
        return Stream.of(index(timer, wallTime)
                .field("activeTasks", timer.activeTasks())
                .field("duration", timer.duration(getBaseTimeUnit()))
                .build());
    }


    private void writeTimer(Timer timer, long wallTime) {
        Stream.Builder<String> stream = Stream.builder();
        Map<String,String> metrics = new HashMap<>();
                metrics.put("count", String.valueOf(timer.count()));
                metrics.put("sum", String.valueOf(timer.totalTime(getBaseTimeUnit())));
                metrics.put("mean", String.valueOf(timer.mean(getBaseTimeUnit())));
                metrics.put("max",String.valueOf( timer.max(getBaseTimeUnit())));

        logger.error(String.valueOf(plugin.getMeasurementEngine().crescoMeterRegistry.get("cresco_message.transaction.time").timer().count()));
        logger.error(timer.getId().getName() + " " + metrics.toString());
    }
/*


    private Stream<String> writeTimer(Timer timer, long wallTime) {
        Stream.Builder<String> stream = Stream.builder();
        stream.add(index(timer, wallTime)
                .field("count", timer.count())
                .field("sum", timer.totalTime(getBaseTimeUnit()))
                .field("mean", timer.mean(getBaseTimeUnit()))
                .field("max", timer.max(getBaseTimeUnit()))
                .build());

        return stream.build();
    }
*/

    private Stream<String> writeSummary(DistributionSummary summary, long wallTime) {
        summary.takeSnapshot();
        Stream.Builder<String> stream = Stream.builder();
        stream.add(index(summary, wallTime)
                .field("count", summary.count())
                .field("sum", summary.totalAmount())
                .field("mean", summary.mean())
                .field("max", summary.max())
                .build());

        return stream.build();
    }

    private Stream<String> writeMeter(Meter meter, long wallTime) {
        IndexBuilder index = index(meter, wallTime);
        for (Measurement measurement : meter.measure()) {
            index.field(measurement.getStatistic().getTagValueRepresentation(), measurement.getValue());
        }
        return Stream.of(index.build());
    }

    private IndexBuilder index(Meter meter, long wallTime) {
        return new IndexBuilder(config, getConventionName(meter.getId()), meter.getId().getType().toString().toLowerCase(), getConventionTags(meter.getId()), wallTime);
    }

    // VisibleForTesting
    IndexBuilder index(String name, String type, long wallTime) {
        return new IndexBuilder(config, name, type, Collections.emptyList(), wallTime);
    }

    static class IndexBuilder {
        private StringBuilder indexLine = new StringBuilder();

        private IndexBuilder(CrescoConfig config, String name, String type, List<Tag> tags, long wallTime) {
            indexLine.append(indexLine(config, wallTime))
                    .append("{\"").append(config.timestampFieldName()).append("\":\"").append(timestamp(wallTime)).append("\"")
                    .append(",\"name\":\"").append(name).append("\"")
                    .append(",\"type\":\"").append(type).append("\"");

            for (Tag tag : tags) {
                field(tag.getKey(), tag.getValue());
            }
        }

        IndexBuilder field(String name, double value) {
            //indexLine.append(",\"").append(name).append("\":").append(DoubleFormat.decimal(value));
            return this;
        }

        IndexBuilder field(String name, String value) {
            indexLine.append(",\"").append(name).append("\":\"").append(value).append("\"");
            return this;
        }

        // VisibleForTesting
        static String timestamp(long wallTime) {
            return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(wallTime));
        }

        private static String indexLine(CrescoConfig config, long wallTime) {
            ZonedDateTime dt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(wallTime), ZoneId.of("UTC"));
            String indexName = config.index() + "-" + DateTimeFormatter.ofPattern(config.indexDateFormat()).format(dt);
            return "{\"index\":{\"_index\":\"" + indexName + "\",\"_type\":\"doc\"}}\n";
        }

        String build() {
            return indexLine.toString() + "}";
        }
    }

    @Override
    @NonNull
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

}