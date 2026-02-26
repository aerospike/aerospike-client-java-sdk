package com.aerospike.client.fluent.info.classes;

public class Geo2dSphereWithin {
    private boolean strict;
    private int minLevel;
    private int maxLevel;
    private int maxCells;
    private int levelMod;
    private long earthRadiusMeters;
    public boolean isStrict() {
        return strict;
    }
    public int getMinLevel() {
        return minLevel;
    }
    public int getMaxLevel() {
        return maxLevel;
    }
    public int getMaxCells() {
        return maxCells;
    }
    public int getLevelMod() {
        return levelMod;
    }
    public long getEarthRadiusMeters() {
        return earthRadiusMeters;
    }
}
