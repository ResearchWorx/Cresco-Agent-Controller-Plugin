package com.researchworx.cresco.controller.globalscheduler;

public class ResourceProvider {

    private String region;
    private String agent;
    private long sysUptime;
    private double cpuIdle;
    private int cpuCompositeBenchmark;
    private long memAvailable;
    private long memLimit;
    private int cpuLogicalCount;


    public ResourceProvider(String region, String agent, long sysUptime, double cpuIdle, int cpuLogicalCount, int cpuCompositeBenchmark, long memAvailable, long memLimit) {
        this.region = region;
        this.agent = agent;
        this.sysUptime = sysUptime;
        this.cpuIdle = cpuIdle;
        this.cpuLogicalCount = cpuLogicalCount;
        this.cpuCompositeBenchmark = cpuCompositeBenchmark;
        this.memAvailable = memAvailable;
        this.memLimit = memLimit;

    }

    public long getSysUptime() {
        return sysUptime;
    }

    public double getCpuIdle() { return  cpuIdle; }

    public int getCpuLogicalCount() { return cpuLogicalCount; }

    public int getCpuCompositeBenchmark() { return cpuCompositeBenchmark; }

    public long getMemLimit() {
        return memLimit;
    }

    public long getMemmemAvailable() {
        return memAvailable;
    }

    public String getRegion() {
        return region;
    }

    public String getAgent() {
        return agent;
    }


    @Override
    public String toString() {
        return String.format("region=" + getRegion() + " agent=" + getAgent() + " sysuptime=" + getSysUptime() + " cpuidle=" + getCpuIdle() + " cpucount=" + getCpuLogicalCount() + " cpubench=" + getCpuCompositeBenchmark() + " memAvailable=" + getMemmemAvailable() + " memlimit=" + getMemLimit());
    }

}