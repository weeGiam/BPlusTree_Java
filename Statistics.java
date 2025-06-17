package cn.weeg.exp.databaseDesign.impl2;

/**
 * @author weeGiam
 */

public class Statistics {
    private double fillRate;
    private int height;
    private int nodeCount;
    private int splitCount;
    private int mergeCount;
    private int pageAccessCount;

    public Statistics(double fillRate, int height, int nodeCount,
                      int splitCount, int mergeCount, int pageAccessCount) {
        this.fillRate = fillRate;
        this.height = height;
        this.nodeCount = nodeCount;
        this.splitCount = splitCount;
        this.mergeCount = mergeCount;
        this.pageAccessCount = pageAccessCount;
    }

    // Getters
    public double getFillRate() {
        return fillRate;
    }

    public int getHeight() {
        return height;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public int getMergeCount() {
        return mergeCount;
    }

    public int getPageAccessCount() {
        return pageAccessCount;
    }

    @Override
    public String toString() {
        return String.format(
                "Statistics{fillRate=%.2f%%, height=%d, nodeCount=%d, " +
                        "splitCount=%d, mergeCount=%d, pageAccessCount=%d}",
                fillRate * 100, height, nodeCount, splitCount, mergeCount, pageAccessCount
        );
    }
}