package com.researchworx.cresco.controller.globalscheduler;

import org.apache.commons.math3.ml.clustering.Clusterable;

public class ResourceMetricWrapper implements Clusterable {

    private double[] points;
    private ResourceMetric metric;

    public ResourceMetricWrapper(ResourceMetric metric, int clusterType) {
        this.metric = metric;
        //this.points = new double[] { metric.getCPU(), metric.getMemory(), metric.getDiskRead(), metric.getDiskWrite(), metric.getNetworkRx(), metric.getNetworkTx()};
        setPoints(clusterType);
    }
    public ResourceMetric getResourceMetric() {
        return metric;
    }

    public void setPoints(int clusterType) {

        //clusterTypes
        //0 = all ResourceMetrics
        //1 = Network ResourceMetrics
        //2 = CPU ResourceMetrics
        //3 = Mem ResourceMetrics
        //4 = Disk ResourceMetrics

        try {
            switch (clusterType) {
                //all metrics
                case 0:  this.points = new double[] { metric.getCpuAve(), metric.getMemAve(), metric.getDiskRead(), metric.getDiskWrite(), metric.getNetworkRx(), metric.getNetworkTx()};
                    break;
                //network
                case 1:  this.points = new double[] { (metric.getNetworkRx() + metric.getNetworkTx())};
                    break;
                //cpu
                case 2:  this.points = new double[] { metric.getCpuAve()};
                    break;
                //memory
                case 3:  this.points = new double[] { metric.getMemAve()};
                    break;
                //disk io
                case 4:  this.points = new double[] { (metric.getDiskRead() + metric.getDiskWrite())};
                    break;
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public double[] getPoint() {
        return points;
    }

}