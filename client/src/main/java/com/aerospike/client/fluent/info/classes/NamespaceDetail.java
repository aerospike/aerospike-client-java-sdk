package com.aerospike.client.fluent.info.classes;

import com.aerospike.client.fluent.info.annotations.Aggregate;
import com.aerospike.client.fluent.info.annotations.And;
import com.aerospike.client.fluent.info.annotations.Average;
import com.aerospike.client.fluent.info.annotations.Mapping;
import com.aerospike.client.fluent.info.annotations.Mappings;

@Mappings({
    @Mapping(from = "storage-engine", to = "storage-engine.type"),
    @Mapping(from = "storage-engine.file\\[(\\d+)\\]", to = "storage-engine.files[$1].filePath")
})
public class NamespaceDetail {
    @Average
    public int effectiveReplicationFactor;
    @Aggregate
    public long objects;

    private long tombstones;
    private long xdrTombstones;
    private long xdrBinCemeteries;
    private long mrtProvisionals;
    @Aggregate
    private long masterObjects;
    @Aggregate
    private long masterTombstones;
    @Aggregate
    private long proleObjects;
    @Aggregate
    private long proleTombstones;
    @Aggregate
    private long nonReplicaObjects;
    @Aggregate
    private long nonReplicaTombstones;
    @Aggregate
    private long unreplicatedRecords;
    private long deadPartitions;
    private long unavailablePartitions;
    private long autoRevivedPartitions;
    private boolean clockSkewStopWrites;
    private boolean stopWrites;
    private boolean hwmBreached;
    private long currentTime;
    private long nonExpirableObjects;
    private long expiredObjects;
    private long evictedObjects;
    private long evictTtl;
    private long evictVoidTime;
    private long smdEvictVoidTime;
    private long nsupCycleDuration;
    private double nsupCycleDeletedPct;
    private long nsupXdrKeyBusy;
    private long truncateLut;
    private boolean truncating;
    private long sindexGcCleaned;
    private long mrtMonitorsActive;
    private long xmemId;
    private long indexUsedBytes;
    private long setIndexUsedBytes;
    private long sindexUsedBytes;
    private long dataTotalBytes;
    private long dataUsedBytes;
    private long dataUsedPct;
    private long dataAvailPct;
    private double dataCompressionRatio;
    private long cacheReadPct;
    private double recordProtoUncompressedPct;
    private double recordProtoCompressionRatio;
    private double queryProtoUncompressedPct;
    private double queryProtoCompressionRatio;
    private boolean pendingQuiesce;
    private boolean effectiveIsQuiesced;
    private long nodesQuiesced;
    private boolean effectivePreferUniformBalance;
    private long effectiveActiveRack;
    private long migrateTxPartitionsImbalance;
    private long migrateTxInstances;
    private long migrateRxInstances;
    private long migrateTxPartitionsActive;
    private long migrateRxPartitionsActive;
    private long migrateTxPartitionsInitial;
    private long migrateTxPartitionsRemaining;
    private long migrateTxPartitionsLeadRemaining;
    private long migrateRxPartitionsInitial;
    private long migrateRxPartitionsRemaining;
    private long migrateRecordsSkipped;
    private long migrateRecordsTransmitted;
    private long migrateRecordRetransmits;
    private long migrateRecordReceives;
    private long migrateRecordsUnreadable;
    private long migrateSignalsActive;
    private long migrateSignalsRemaining;
    private long migrateFreshPartitions;
    private long appealsTxActive;
    private long appealsRxActive;
    private long appealsTxRemaining;
    private long appealsRecordsExonerated;
    private long clientTsvcError;
    private long clientTsvcTimeout;
    private long clientProxyComplete;
    private long clientProxyError;
    private long clientProxyTimeout;
    private long clientReadSuccess;
    private long clientReadError;
    private long clientReadTimeout;
    private long clientReadNotFound;
    private long clientReadFilteredOut;
    private long clientWriteSuccess;
    private long clientWriteError;
    private long clientWriteTimeout;
    private long clientWriteFilteredOut;
    private long xdrClientWriteSuccess;
    private long xdrClientWriteError;
    private long xdrClientWriteTimeout;
    private long clientDeleteSuccess;
    private long clientDeleteError;
    private long clientDeleteTimeout;
    private long clientDeleteNotFound;
    private long clientDeleteFilteredOut;
    private long xdrClientDeleteSuccess;
    private long xdrClientDeleteError;
    private long xdrClientDeleteTimeout;
    private long xdrClientDeleteNotFound;
    private long clientUdfComplete;
    private long clientUdfError;
    private long clientUdfTimeout;
    private long clientUdfFilteredOut;
    private long clientLangReadSuccess;
    private long clientLangWriteSuccess;
    private long clientLangDeleteSuccess;
    private long clientLangError;
    private long fromProxyTsvcError;
    private long fromProxyTsvcTimeout;
    private long fromProxyReadSuccess;
    private long fromProxyReadError;
    private long fromProxyReadTimeout;
    private long fromProxyReadNotFound;
    private long fromProxyReadFilteredOut;
    private long fromProxyWriteSuccess;
    private long fromProxyWriteError;
    private long fromProxyWriteTimeout;
    private long fromProxyWriteFilteredOut;
    private long xdrFromProxyWriteSuccess;
    private long xdrFromProxyWriteError;
    private long xdrFromProxyWriteTimeout;
    private long fromProxyDeleteSuccess;
    private long fromProxyDeleteError;
    private long fromProxyDeleteTimeout;
    private long fromProxyDeleteNotFound;
    private long fromProxyDeleteFilteredOut;
    private long xdrFromProxyDeleteSuccess;
    private long xdrFromProxyDeleteError;
    private long xdrFromProxyDeleteTimeout;
    private long xdrFromProxyDeleteNotFound;
    private long fromProxyUdfComplete;
    private long fromProxyUdfError;
    private long fromProxyUdfTimeout;
    private long fromProxyUdfFilteredOut;
    private long fromProxyLangReadSuccess;
    private long fromProxyLangWriteSuccess;
    private long fromProxyLangDeleteSuccess;
    private long fromProxyLangError;
    private long batchSubTsvcError;
    private long batchSubTsvcTimeout;
    private long batchSubProxyComplete;
    private long batchSubProxyError;
    private long batchSubProxyTimeout;
    private long batchSubReadSuccess;
    private long batchSubReadError;
    private long batchSubReadTimeout;
    private long batchSubReadNotFound;
    private long batchSubReadFilteredOut;
    private long batchSubWriteSuccess;
    private long batchSubWriteError;
    private long batchSubWriteTimeout;
    private long batchSubWriteFilteredOut;
    private long batchSubDeleteSuccess;
    private long batchSubDeleteError;
    private long batchSubDeleteTimeout;
    private long batchSubDeleteNotFound;
    private long batchSubDeleteFilteredOut;
    private long batchSubUdfComplete;
    private long batchSubUdfError;
    private long batchSubUdfTimeout;
    private long batchSubUdfFilteredOut;
    private long batchSubLangReadSuccess;
    private long batchSubLangWriteSuccess;
    private long batchSubLangDeleteSuccess;
    private long batchSubLangError;
    private long fromProxyBatchSubTsvcError;
    private long fromProxyBatchSubTsvcTimeout;
    private long fromProxyBatchSubReadSuccess;
    private long fromProxyBatchSubReadError;
    private long fromProxyBatchSubReadTimeout;
    private long fromProxyBatchSubReadNotFound;
    private long fromProxyBatchSubReadFilteredOut;
    private long fromProxyBatchSubWriteSuccess;
    private long fromProxyBatchSubWriteError;
    private long fromProxyBatchSubWriteTimeout;
    private long fromProxyBatchSubWriteFilteredOut;
    private long fromProxyBatchSubDeleteSuccess;
    private long fromProxyBatchSubDeleteError;
    private long fromProxyBatchSubDeleteTimeout;
    private long fromProxyBatchSubDeleteNotFound;
    private long fromProxyBatchSubDeleteFilteredOut;
    private long fromProxyBatchSubUdfComplete;
    private long fromProxyBatchSubUdfError;
    private long fromProxyBatchSubUdfTimeout;
    private long fromProxyBatchSubUdfFilteredOut;
    private long fromProxyBatchSubLangReadSuccess;
    private long fromProxyBatchSubLangWriteSuccess;
    private long fromProxyBatchSubLangDeleteSuccess;
    private long fromProxyBatchSubLangError;
    private long udfSubTsvcError;
    private long udfSubTsvcTimeout;
    private long udfSubUdfComplete;
    private long udfSubUdfError;
    private long udfSubUdfTimeout;
    private long udfSubUdfFilteredOut;
    private long udfSubLangReadSuccess;
    private long udfSubLangWriteSuccess;
    private long udfSubLangDeleteSuccess;
    private long udfSubLangError;
    private long opsSubTsvcError;
    private long opsSubTsvcTimeout;
    private long opsSubWriteSuccess;
    private long opsSubWriteError;
    private long opsSubWriteTimeout;
    private long opsSubWriteFilteredOut;
    private long dupResAsk;
    private long dupResRespondRead;
    private long dupResRespondNoRead;
    private long retransmitAllReadDupRes;
    private long retransmitAllWriteDupRes;
    private long retransmitAllDeleteDupRes;
    private long retransmitAllUdfDupRes;
    private long retransmitAllBatchSubReadDupRes;
    private long retransmitAllBatchSubWriteDupRes;
    private long retransmitAllBatchSubDeleteDupRes;
    private long retransmitAllBatchSubUdfDupRes;
    private long retransmitUdfSubDupRes;
    private long retransmitOpsSubDupRes;
    private long retransmitAllReadReplPing;
    private long retransmitAllBatchSubReadReplPing;
    private long retransmitAllWriteReplWrite;
    private long retransmitAllDeleteReplWrite;
    private long retransmitAllUdfReplWrite;
    private long retransmitAllBatchSubWriteReplWrite;
    private long retransmitAllBatchSubDeleteReplWrite;
    private long retransmitAllBatchSubUdfReplWrite;
    private long retransmitUdfSubReplWrite;
    private long retransmitOpsSubReplWrite;
    private long piQueryShortBasicComplete;
    private long piQueryShortBasicError;
    private long piQueryShortBasicTimeout;
    private long piQueryLongBasicComplete;
    private long piQueryLongBasicError;
    private long piQueryLongBasicAbort;
    private long piQueryAggrComplete;
    private long piQueryAggrError;
    private long piQueryAggrAbort;
    private long piQueryUdfBgComplete;
    private long piQueryUdfBgError;
    private long piQueryUdfBgAbort;
    private long piQueryOpsBgComplete;
    private long piQueryOpsBgError;
    private long piQueryOpsBgAbort;
    private long siQueryShortBasicComplete;
    private long siQueryShortBasicError;
    private long siQueryShortBasicTimeout;
    private long siQueryLongBasicComplete;
    private long siQueryLongBasicError;
    private long siQueryLongBasicAbort;
    private long siQueryAggrComplete;
    private long siQueryAggrError;
    private long siQueryAggrAbort;
    private long siQueryUdfBgComplete;
    private long siQueryUdfBgError;
    private long siQueryUdfBgAbort;
    private long siQueryOpsBgComplete;
    private long siQueryOpsBgError;
    private long siQueryOpsBgAbort;
    private long geoRegionQueryReqs;
    private long geoRegionQueryCells;
    private long geoRegionQueryPoints;
    private long geoRegionQueryFalsepos;
    private long readTouchTsvcError;
    private long readTouchTsvcTimeout;
    private long readTouchSuccess;
    private long readTouchError;
    private long readTouchTimeout;
    private long readTouchSkip;
    private long reReplTsvcError;
    private long reReplTsvcTimeout;
    private long reReplSuccess;
    private long reReplError;
    private long reReplTimeout;
    private long mrtVerifyReadSuccess;
    private long mrtVerifyReadError;
    private long mrtVerifyReadTimeout;
    private long mrtRollForwardSuccess;
    private long mrtRollForwardError;
    private long mrtRollForwardTimeout;
    private long mrtMonitorRollForwardSuccess;
    private long mrtMonitorRollForwardError;
    private long mrtMonitorRollForwardTimeout;
    private long mrtRollBackSuccess;
    private long mrtRollBackError;
    private long mrtRollBackTimeout;
    private long mrtMonitorRollBackSuccess;
    private long mrtMonitorRollBackError;
    private long mrtMonitorRollBackTimeout;
    private long failXdrForbidden;
    private long failKeyBusy;
    private long failXdrKeyBusy;
    private long failGeneration;
    private long failRecordTooBig;
    private long failClientLostConflict;
    private long failXdrLostConflict;
    private long failMrtBlocked;
    private long failMrtVersionMismatch;
    private long deletedLastBin;
    private long mrtMonitorRollTombstoneCreates;
    private long ttlReductionsIgnored;
    private long ttlReductionsApplied;
    private long activeRack;
    private boolean allowTtlWithoutNsup;
    private boolean autoRevive;
    private long backgroundQueryMaxRps;
    private ConflictResolutionPolicy conflictResolutionPolicy;
    private boolean conflictResolveWrites;
    private long defaultReadTouchTtlPct;
    private long defaultTtl;
    private boolean disableColdStartEviction;
    private boolean disableMrtWrites;
    private boolean disableWriteDupRes;
    private boolean disallowExpunge;
    private boolean disallowNullSetname;
    private boolean enableBenchmarksBatchSub;
    private boolean enableBenchmarksOpsSub;
    private boolean enableBenchmarksRead;
    private boolean enableBenchmarksUdf;
    private boolean enableBenchmarksUdfSub;
    private boolean enableBenchmarksWrite;
    private boolean enableHistProxy;
    private long evictHistBuckets;
    private long evictIndexesMemoryPct;
    private long evictTenthsPct;
    private boolean forceLongQueries;
    private boolean ignoreMigrateFillDelay;
    private long indexStageSize;
    private long indexesMemoryBudget;
    private boolean inlineShortQueries;
    private long maxRecordSize;
    private long migrateOrder;
    private long migrateRetransmitMs;
    private boolean migrateSkipUnreadable;
    @Average
    private long migrateSleep;
    private long mrtDuration;
    private long nsupHistPeriod;
    private long nsupPeriod;
    private long nsupThreads;
    private long partitionTreeSprigs;
    @And
    private boolean preferUniformBalance;
    private long rackId;
    private ReadConsistencyLevelOverride readConsistencyLevelOverride;
    private boolean rejectNonXdrWrites;
    private boolean rejectXdrWrites;
    private long replicationFactor;
    private long sindexStageSize;
    private long singleQueryThreads;
    private long stopWritesSysMemoryPct;
    private boolean strongConsistency;
    private boolean strongConsistencyAllowExpunge;
    private long tombRaiderEligibleIge;
    private long tombRaiderPeriod;
    private long transactionPendingLimit;
    private long truncateThreads;
    private WriteCommitLevelOverride writeCommitLevelOverride;
    private long xdrBinTombstoneTtl;
    private long xdrTombRaiderPeriod;
    private long xdrTombRaiderThreads;
    private IndexType indexStorageType;
    private IndexType sindexStorageType;
    private StorageEngine storageEngine;
    private Geo2dsphereWithin geo2dsphereWithin;
    @Override
    public String toString() {
        return "NamespaceDetail [effectiveReplicationFactor=" + effectiveReplicationFactor + ", objects=" + objects
                + ", masterObjects=" + masterObjects + ", proleObjects=" + proleObjects + ", nonReplicaObjects="
                + nonReplicaObjects + ", unreplicatedRecords=" + unreplicatedRecords + ", deadPartitions="
                + deadPartitions + ", unavailablePartitions=" + unavailablePartitions + ", stopWrites=" + stopWrites
                + ", currentTime=" + currentTime + ", expiredObjects=" + expiredObjects + ", evictedObjects="
                + evictedObjects + ", indexUsedBytes=" + indexUsedBytes + ", setIndexUsedBytes=" + setIndexUsedBytes
                + ", sindexUsedBytes=" + sindexUsedBytes + ", dataTotalBytes=" + dataTotalBytes + ", dataUsedBytes="
                + dataUsedBytes + ", cacheReadPct=" + cacheReadPct + "]";
    }
    public int getEffectiveReplicationFactor() {
        return effectiveReplicationFactor;
    }
    public long getObjects() {
        return objects;
    }
    public long getTombstones() {
        return tombstones;
    }
    public long getXdrTombstones() {
        return xdrTombstones;
    }
    public long getXdrBinCemeteries() {
        return xdrBinCemeteries;
    }
    public long getMrtProvisionals() {
        return mrtProvisionals;
    }
    public long getMasterObjects() {
        return masterObjects;
    }
    public long getMasterTombstones() {
        return masterTombstones;
    }
    public long getProleObjects() {
        return proleObjects;
    }
    public long getProleTombstones() {
        return proleTombstones;
    }
    public long getNonReplicaObjects() {
        return nonReplicaObjects;
    }
    public long getNonReplicaTombstones() {
        return nonReplicaTombstones;
    }
    public long getUnreplicatedRecords() {
        return unreplicatedRecords;
    }
    public long getDeadPartitions() {
        return deadPartitions;
    }
    public long getUnavailablePartitions() {
        return unavailablePartitions;
    }
    public long getAutoRevivedPartitions() {
        return autoRevivedPartitions;
    }
    public boolean isClockSkewStopWrites() {
        return clockSkewStopWrites;
    }
    public boolean isStopWrites() {
        return stopWrites;
    }
    public boolean isHwmBreached() {
        return hwmBreached;
    }
    public long getCurrentTime() {
        return currentTime;
    }
    public long getNonExpirableObjects() {
        return nonExpirableObjects;
    }
    public long getExpiredObjects() {
        return expiredObjects;
    }
    public long getEvictedObjects() {
        return evictedObjects;
    }
    public long getEvictTtl() {
        return evictTtl;
    }
    public long getEvictVoidTime() {
        return evictVoidTime;
    }
    public long getSmdEvictVoidTime() {
        return smdEvictVoidTime;
    }
    public long getNsupCycleDuration() {
        return nsupCycleDuration;
    }
    public double getNsupCycleDeletedPct() {
        return nsupCycleDeletedPct;
    }
    public long getNsupXdrKeyBusy() {
        return nsupXdrKeyBusy;
    }
    public long getTruncateLut() {
        return truncateLut;
    }
    public boolean isTruncating() {
        return truncating;
    }
    public long getSindexGcCleaned() {
        return sindexGcCleaned;
    }
    public long getMrtMonitorsActive() {
        return mrtMonitorsActive;
    }
    public long getXmemId() {
        return xmemId;
    }
    public long getIndexUsedBytes() {
        return indexUsedBytes;
    }
    public long getSetIndexUsedBytes() {
        return setIndexUsedBytes;
    }
    public long getSindexUsedBytes() {
        return sindexUsedBytes;
    }
    public long getDataTotalBytes() {
        return dataTotalBytes;
    }
    public long getDataUsedBytes() {
        return dataUsedBytes;
    }
    public long getDataUsedPct() {
        return dataUsedPct;
    }
    public long getDataAvailPct() {
        return dataAvailPct;
    }
    public double getDataCompressionRatio() {
        return dataCompressionRatio;
    }
    public long getCacheReadPct() {
        return cacheReadPct;
    }
    public double getRecordProtoUncompressedPct() {
        return recordProtoUncompressedPct;
    }
    public double getRecordProtoCompressionRatio() {
        return recordProtoCompressionRatio;
    }
    public double getQueryProtoUncompressedPct() {
        return queryProtoUncompressedPct;
    }
    public double getQueryProtoCompressionRatio() {
        return queryProtoCompressionRatio;
    }
    public boolean isPendingQuiesce() {
        return pendingQuiesce;
    }
    public boolean isEffectiveIsQuiesced() {
        return effectiveIsQuiesced;
    }
    public long getNodesQuiesced() {
        return nodesQuiesced;
    }
    public boolean isEffectivePreferUniformBalance() {
        return effectivePreferUniformBalance;
    }
    public long getEffectiveActiveRack() {
        return effectiveActiveRack;
    }
    public long getMigrateTxPartitionsImbalance() {
        return migrateTxPartitionsImbalance;
    }
    public long getMigrateTxInstances() {
        return migrateTxInstances;
    }
    public long getMigrateRxInstances() {
        return migrateRxInstances;
    }
    public long getMigrateTxPartitionsActive() {
        return migrateTxPartitionsActive;
    }
    public long getMigrateRxPartitionsActive() {
        return migrateRxPartitionsActive;
    }
    public long getMigrateTxPartitionsInitial() {
        return migrateTxPartitionsInitial;
    }
    public long getMigrateTxPartitionsRemaining() {
        return migrateTxPartitionsRemaining;
    }
    public long getMigrateTxPartitionsLeadRemaining() {
        return migrateTxPartitionsLeadRemaining;
    }
    public long getMigrateRxPartitionsInitial() {
        return migrateRxPartitionsInitial;
    }
    public long getMigrateRxPartitionsRemaining() {
        return migrateRxPartitionsRemaining;
    }
    public long getMigrateRecordsSkipped() {
        return migrateRecordsSkipped;
    }
    public long getMigrateRecordsTransmitted() {
        return migrateRecordsTransmitted;
    }
    public long getMigrateRecordRetransmits() {
        return migrateRecordRetransmits;
    }
    public long getMigrateRecordReceives() {
        return migrateRecordReceives;
    }
    public long getMigrateRecordsUnreadable() {
        return migrateRecordsUnreadable;
    }
    public long getMigrateSignalsActive() {
        return migrateSignalsActive;
    }
    public long getMigrateSignalsRemaining() {
        return migrateSignalsRemaining;
    }
    public long getMigrateFreshPartitions() {
        return migrateFreshPartitions;
    }
    public long getAppealsTxActive() {
        return appealsTxActive;
    }
    public long getAppealsRxActive() {
        return appealsRxActive;
    }
    public long getAppealsTxRemaining() {
        return appealsTxRemaining;
    }
    public long getAppealsRecordsExonerated() {
        return appealsRecordsExonerated;
    }
    public long getClientTsvcError() {
        return clientTsvcError;
    }
    public long getClientTsvcTimeout() {
        return clientTsvcTimeout;
    }
    public long getClientProxyComplete() {
        return clientProxyComplete;
    }
    public long getClientProxyError() {
        return clientProxyError;
    }
    public long getClientProxyTimeout() {
        return clientProxyTimeout;
    }
    public long getClientReadSuccess() {
        return clientReadSuccess;
    }
    public long getClientReadError() {
        return clientReadError;
    }
    public long getClientReadTimeout() {
        return clientReadTimeout;
    }
    public long getClientReadNotFound() {
        return clientReadNotFound;
    }
    public long getClientReadFilteredOut() {
        return clientReadFilteredOut;
    }
    public long getClientWriteSuccess() {
        return clientWriteSuccess;
    }
    public long getClientWriteError() {
        return clientWriteError;
    }
    public long getClientWriteTimeout() {
        return clientWriteTimeout;
    }
    public long getClientWriteFilteredOut() {
        return clientWriteFilteredOut;
    }
    public long getXdrClientWriteSuccess() {
        return xdrClientWriteSuccess;
    }
    public long getXdrClientWriteError() {
        return xdrClientWriteError;
    }
    public long getXdrClientWriteTimeout() {
        return xdrClientWriteTimeout;
    }
    public long getClientDeleteSuccess() {
        return clientDeleteSuccess;
    }
    public long getClientDeleteError() {
        return clientDeleteError;
    }
    public long getClientDeleteTimeout() {
        return clientDeleteTimeout;
    }
    public long getClientDeleteNotFound() {
        return clientDeleteNotFound;
    }
    public long getClientDeleteFilteredOut() {
        return clientDeleteFilteredOut;
    }
    public long getXdrClientDeleteSuccess() {
        return xdrClientDeleteSuccess;
    }
    public long getXdrClientDeleteError() {
        return xdrClientDeleteError;
    }
    public long getXdrClientDeleteTimeout() {
        return xdrClientDeleteTimeout;
    }
    public long getXdrClientDeleteNotFound() {
        return xdrClientDeleteNotFound;
    }
    public long getClientUdfComplete() {
        return clientUdfComplete;
    }
    public long getClientUdfError() {
        return clientUdfError;
    }
    public long getClientUdfTimeout() {
        return clientUdfTimeout;
    }
    public long getClientUdfFilteredOut() {
        return clientUdfFilteredOut;
    }
    public long getClientLangReadSuccess() {
        return clientLangReadSuccess;
    }
    public long getClientLangWriteSuccess() {
        return clientLangWriteSuccess;
    }
    public long getClientLangDeleteSuccess() {
        return clientLangDeleteSuccess;
    }
    public long getClientLangError() {
        return clientLangError;
    }
    public long getFromProxyTsvcError() {
        return fromProxyTsvcError;
    }
    public long getFromProxyTsvcTimeout() {
        return fromProxyTsvcTimeout;
    }
    public long getFromProxyReadSuccess() {
        return fromProxyReadSuccess;
    }
    public long getFromProxyReadError() {
        return fromProxyReadError;
    }
    public long getFromProxyReadTimeout() {
        return fromProxyReadTimeout;
    }
    public long getFromProxyReadNotFound() {
        return fromProxyReadNotFound;
    }
    public long getFromProxyReadFilteredOut() {
        return fromProxyReadFilteredOut;
    }
    public long getFromProxyWriteSuccess() {
        return fromProxyWriteSuccess;
    }
    public long getFromProxyWriteError() {
        return fromProxyWriteError;
    }
    public long getFromProxyWriteTimeout() {
        return fromProxyWriteTimeout;
    }
    public long getFromProxyWriteFilteredOut() {
        return fromProxyWriteFilteredOut;
    }
    public long getXdrFromProxyWriteSuccess() {
        return xdrFromProxyWriteSuccess;
    }
    public long getXdrFromProxyWriteError() {
        return xdrFromProxyWriteError;
    }
    public long getXdrFromProxyWriteTimeout() {
        return xdrFromProxyWriteTimeout;
    }
    public long getFromProxyDeleteSuccess() {
        return fromProxyDeleteSuccess;
    }
    public long getFromProxyDeleteError() {
        return fromProxyDeleteError;
    }
    public long getFromProxyDeleteTimeout() {
        return fromProxyDeleteTimeout;
    }
    public long getFromProxyDeleteNotFound() {
        return fromProxyDeleteNotFound;
    }
    public long getFromProxyDeleteFilteredOut() {
        return fromProxyDeleteFilteredOut;
    }
    public long getXdrFromProxyDeleteSuccess() {
        return xdrFromProxyDeleteSuccess;
    }
    public long getXdrFromProxyDeleteError() {
        return xdrFromProxyDeleteError;
    }
    public long getXdrFromProxyDeleteTimeout() {
        return xdrFromProxyDeleteTimeout;
    }
    public long getXdrFromProxyDeleteNotFound() {
        return xdrFromProxyDeleteNotFound;
    }
    public long getFromProxyUdfComplete() {
        return fromProxyUdfComplete;
    }
    public long getFromProxyUdfError() {
        return fromProxyUdfError;
    }
    public long getFromProxyUdfTimeout() {
        return fromProxyUdfTimeout;
    }
    public long getFromProxyUdfFilteredOut() {
        return fromProxyUdfFilteredOut;
    }
    public long getFromProxyLangReadSuccess() {
        return fromProxyLangReadSuccess;
    }
    public long getFromProxyLangWriteSuccess() {
        return fromProxyLangWriteSuccess;
    }
    public long getFromProxyLangDeleteSuccess() {
        return fromProxyLangDeleteSuccess;
    }
    public long getFromProxyLangError() {
        return fromProxyLangError;
    }
    public long getBatchSubTsvcError() {
        return batchSubTsvcError;
    }
    public long getBatchSubTsvcTimeout() {
        return batchSubTsvcTimeout;
    }
    public long getBatchSubProxyComplete() {
        return batchSubProxyComplete;
    }
    public long getBatchSubProxyError() {
        return batchSubProxyError;
    }
    public long getBatchSubProxyTimeout() {
        return batchSubProxyTimeout;
    }
    public long getBatchSubReadSuccess() {
        return batchSubReadSuccess;
    }
    public long getBatchSubReadError() {
        return batchSubReadError;
    }
    public long getBatchSubReadTimeout() {
        return batchSubReadTimeout;
    }
    public long getBatchSubReadNotFound() {
        return batchSubReadNotFound;
    }
    public long getBatchSubReadFilteredOut() {
        return batchSubReadFilteredOut;
    }
    public long getBatchSubWriteSuccess() {
        return batchSubWriteSuccess;
    }
    public long getBatchSubWriteError() {
        return batchSubWriteError;
    }
    public long getBatchSubWriteTimeout() {
        return batchSubWriteTimeout;
    }
    public long getBatchSubWriteFilteredOut() {
        return batchSubWriteFilteredOut;
    }
    public long getBatchSubDeleteSuccess() {
        return batchSubDeleteSuccess;
    }
    public long getBatchSubDeleteError() {
        return batchSubDeleteError;
    }
    public long getBatchSubDeleteTimeout() {
        return batchSubDeleteTimeout;
    }
    public long getBatchSubDeleteNotFound() {
        return batchSubDeleteNotFound;
    }
    public long getBatchSubDeleteFilteredOut() {
        return batchSubDeleteFilteredOut;
    }
    public long getBatchSubUdfComplete() {
        return batchSubUdfComplete;
    }
    public long getBatchSubUdfError() {
        return batchSubUdfError;
    }
    public long getBatchSubUdfTimeout() {
        return batchSubUdfTimeout;
    }
    public long getBatchSubUdfFilteredOut() {
        return batchSubUdfFilteredOut;
    }
    public long getBatchSubLangReadSuccess() {
        return batchSubLangReadSuccess;
    }
    public long getBatchSubLangWriteSuccess() {
        return batchSubLangWriteSuccess;
    }
    public long getBatchSubLangDeleteSuccess() {
        return batchSubLangDeleteSuccess;
    }
    public long getBatchSubLangError() {
        return batchSubLangError;
    }
    public long getFromProxyBatchSubTsvcError() {
        return fromProxyBatchSubTsvcError;
    }
    public long getFromProxyBatchSubTsvcTimeout() {
        return fromProxyBatchSubTsvcTimeout;
    }
    public long getFromProxyBatchSubReadSuccess() {
        return fromProxyBatchSubReadSuccess;
    }
    public long getFromProxyBatchSubReadError() {
        return fromProxyBatchSubReadError;
    }
    public long getFromProxyBatchSubReadTimeout() {
        return fromProxyBatchSubReadTimeout;
    }
    public long getFromProxyBatchSubReadNotFound() {
        return fromProxyBatchSubReadNotFound;
    }
    public long getFromProxyBatchSubReadFilteredOut() {
        return fromProxyBatchSubReadFilteredOut;
    }
    public long getFromProxyBatchSubWriteSuccess() {
        return fromProxyBatchSubWriteSuccess;
    }
    public long getFromProxyBatchSubWriteError() {
        return fromProxyBatchSubWriteError;
    }
    public long getFromProxyBatchSubWriteTimeout() {
        return fromProxyBatchSubWriteTimeout;
    }
    public long getFromProxyBatchSubWriteFilteredOut() {
        return fromProxyBatchSubWriteFilteredOut;
    }
    public long getFromProxyBatchSubDeleteSuccess() {
        return fromProxyBatchSubDeleteSuccess;
    }
    public long getFromProxyBatchSubDeleteError() {
        return fromProxyBatchSubDeleteError;
    }
    public long getFromProxyBatchSubDeleteTimeout() {
        return fromProxyBatchSubDeleteTimeout;
    }
    public long getFromProxyBatchSubDeleteNotFound() {
        return fromProxyBatchSubDeleteNotFound;
    }
    public long getFromProxyBatchSubDeleteFilteredOut() {
        return fromProxyBatchSubDeleteFilteredOut;
    }
    public long getFromProxyBatchSubUdfComplete() {
        return fromProxyBatchSubUdfComplete;
    }
    public long getFromProxyBatchSubUdfError() {
        return fromProxyBatchSubUdfError;
    }
    public long getFromProxyBatchSubUdfTimeout() {
        return fromProxyBatchSubUdfTimeout;
    }
    public long getFromProxyBatchSubUdfFilteredOut() {
        return fromProxyBatchSubUdfFilteredOut;
    }
    public long getFromProxyBatchSubLangReadSuccess() {
        return fromProxyBatchSubLangReadSuccess;
    }
    public long getFromProxyBatchSubLangWriteSuccess() {
        return fromProxyBatchSubLangWriteSuccess;
    }
    public long getFromProxyBatchSubLangDeleteSuccess() {
        return fromProxyBatchSubLangDeleteSuccess;
    }
    public long getFromProxyBatchSubLangError() {
        return fromProxyBatchSubLangError;
    }
    public long getUdfSubTsvcError() {
        return udfSubTsvcError;
    }
    public long getUdfSubTsvcTimeout() {
        return udfSubTsvcTimeout;
    }
    public long getUdfSubUdfComplete() {
        return udfSubUdfComplete;
    }
    public long getUdfSubUdfError() {
        return udfSubUdfError;
    }
    public long getUdfSubUdfTimeout() {
        return udfSubUdfTimeout;
    }
    public long getUdfSubUdfFilteredOut() {
        return udfSubUdfFilteredOut;
    }
    public long getUdfSubLangReadSuccess() {
        return udfSubLangReadSuccess;
    }
    public long getUdfSubLangWriteSuccess() {
        return udfSubLangWriteSuccess;
    }
    public long getUdfSubLangDeleteSuccess() {
        return udfSubLangDeleteSuccess;
    }
    public long getUdfSubLangError() {
        return udfSubLangError;
    }
    public long getOpsSubTsvcError() {
        return opsSubTsvcError;
    }
    public long getOpsSubTsvcTimeout() {
        return opsSubTsvcTimeout;
    }
    public long getOpsSubWriteSuccess() {
        return opsSubWriteSuccess;
    }
    public long getOpsSubWriteError() {
        return opsSubWriteError;
    }
    public long getOpsSubWriteTimeout() {
        return opsSubWriteTimeout;
    }
    public long getOpsSubWriteFilteredOut() {
        return opsSubWriteFilteredOut;
    }
    public long getDupResAsk() {
        return dupResAsk;
    }
    public long getDupResRespondRead() {
        return dupResRespondRead;
    }
    public long getDupResRespondNoRead() {
        return dupResRespondNoRead;
    }
    public long getRetransmitAllReadDupRes() {
        return retransmitAllReadDupRes;
    }
    public long getRetransmitAllWriteDupRes() {
        return retransmitAllWriteDupRes;
    }
    public long getRetransmitAllDeleteDupRes() {
        return retransmitAllDeleteDupRes;
    }
    public long getRetransmitAllUdfDupRes() {
        return retransmitAllUdfDupRes;
    }
    public long getRetransmitAllBatchSubReadDupRes() {
        return retransmitAllBatchSubReadDupRes;
    }
    public long getRetransmitAllBatchSubWriteDupRes() {
        return retransmitAllBatchSubWriteDupRes;
    }
    public long getRetransmitAllBatchSubDeleteDupRes() {
        return retransmitAllBatchSubDeleteDupRes;
    }
    public long getRetransmitAllBatchSubUdfDupRes() {
        return retransmitAllBatchSubUdfDupRes;
    }
    public long getRetransmitUdfSubDupRes() {
        return retransmitUdfSubDupRes;
    }
    public long getRetransmitOpsSubDupRes() {
        return retransmitOpsSubDupRes;
    }
    public long getRetransmitAllReadReplPing() {
        return retransmitAllReadReplPing;
    }
    public long getRetransmitAllBatchSubReadReplPing() {
        return retransmitAllBatchSubReadReplPing;
    }
    public long getRetransmitAllWriteReplWrite() {
        return retransmitAllWriteReplWrite;
    }
    public long getRetransmitAllDeleteReplWrite() {
        return retransmitAllDeleteReplWrite;
    }
    public long getRetransmitAllUdfReplWrite() {
        return retransmitAllUdfReplWrite;
    }
    public long getRetransmitAllBatchSubWriteReplWrite() {
        return retransmitAllBatchSubWriteReplWrite;
    }
    public long getRetransmitAllBatchSubDeleteReplWrite() {
        return retransmitAllBatchSubDeleteReplWrite;
    }
    public long getRetransmitAllBatchSubUdfReplWrite() {
        return retransmitAllBatchSubUdfReplWrite;
    }
    public long getRetransmitUdfSubReplWrite() {
        return retransmitUdfSubReplWrite;
    }
    public long getRetransmitOpsSubReplWrite() {
        return retransmitOpsSubReplWrite;
    }
    public long getPiQueryShortBasicComplete() {
        return piQueryShortBasicComplete;
    }
    public long getPiQueryShortBasicError() {
        return piQueryShortBasicError;
    }
    public long getPiQueryShortBasicTimeout() {
        return piQueryShortBasicTimeout;
    }
    public long getPiQueryLongBasicComplete() {
        return piQueryLongBasicComplete;
    }
    public long getPiQueryLongBasicError() {
        return piQueryLongBasicError;
    }
    public long getPiQueryLongBasicAbort() {
        return piQueryLongBasicAbort;
    }
    public long getPiQueryAggrComplete() {
        return piQueryAggrComplete;
    }
    public long getPiQueryAggrError() {
        return piQueryAggrError;
    }
    public long getPiQueryAggrAbort() {
        return piQueryAggrAbort;
    }
    public long getPiQueryUdfBgComplete() {
        return piQueryUdfBgComplete;
    }
    public long getPiQueryUdfBgError() {
        return piQueryUdfBgError;
    }
    public long getPiQueryUdfBgAbort() {
        return piQueryUdfBgAbort;
    }
    public long getPiQueryOpsBgComplete() {
        return piQueryOpsBgComplete;
    }
    public long getPiQueryOpsBgError() {
        return piQueryOpsBgError;
    }
    public long getPiQueryOpsBgAbort() {
        return piQueryOpsBgAbort;
    }
    public long getSiQueryShortBasicComplete() {
        return siQueryShortBasicComplete;
    }
    public long getSiQueryShortBasicError() {
        return siQueryShortBasicError;
    }
    public long getSiQueryShortBasicTimeout() {
        return siQueryShortBasicTimeout;
    }
    public long getSiQueryLongBasicComplete() {
        return siQueryLongBasicComplete;
    }
    public long getSiQueryLongBasicError() {
        return siQueryLongBasicError;
    }
    public long getSiQueryLongBasicAbort() {
        return siQueryLongBasicAbort;
    }
    public long getSiQueryAggrComplete() {
        return siQueryAggrComplete;
    }
    public long getSiQueryAggrError() {
        return siQueryAggrError;
    }
    public long getSiQueryAggrAbort() {
        return siQueryAggrAbort;
    }
    public long getSiQueryUdfBgComplete() {
        return siQueryUdfBgComplete;
    }
    public long getSiQueryUdfBgError() {
        return siQueryUdfBgError;
    }
    public long getSiQueryUdfBgAbort() {
        return siQueryUdfBgAbort;
    }
    public long getSiQueryOpsBgComplete() {
        return siQueryOpsBgComplete;
    }
    public long getSiQueryOpsBgError() {
        return siQueryOpsBgError;
    }
    public long getSiQueryOpsBgAbort() {
        return siQueryOpsBgAbort;
    }
    public long getGeoRegionQueryReqs() {
        return geoRegionQueryReqs;
    }
    public long getGeoRegionQueryCells() {
        return geoRegionQueryCells;
    }
    public long getGeoRegionQueryPoints() {
        return geoRegionQueryPoints;
    }
    public long getGeoRegionQueryFalsepos() {
        return geoRegionQueryFalsepos;
    }
    public long getReadTouchTsvcError() {
        return readTouchTsvcError;
    }
    public long getReadTouchTsvcTimeout() {
        return readTouchTsvcTimeout;
    }
    public long getReadTouchSuccess() {
        return readTouchSuccess;
    }
    public long getReadTouchError() {
        return readTouchError;
    }
    public long getReadTouchTimeout() {
        return readTouchTimeout;
    }
    public long getReadTouchSkip() {
        return readTouchSkip;
    }
    public long getReReplTsvcError() {
        return reReplTsvcError;
    }
    public long getReReplTsvcTimeout() {
        return reReplTsvcTimeout;
    }
    public long getReReplSuccess() {
        return reReplSuccess;
    }
    public long getReReplError() {
        return reReplError;
    }
    public long getReReplTimeout() {
        return reReplTimeout;
    }
    public long getMrtVerifyReadSuccess() {
        return mrtVerifyReadSuccess;
    }
    public long getMrtVerifyReadError() {
        return mrtVerifyReadError;
    }
    public long getMrtVerifyReadTimeout() {
        return mrtVerifyReadTimeout;
    }
    public long getMrtRollForwardSuccess() {
        return mrtRollForwardSuccess;
    }
    public long getMrtRollForwardError() {
        return mrtRollForwardError;
    }
    public long getMrtRollForwardTimeout() {
        return mrtRollForwardTimeout;
    }
    public long getMrtMonitorRollForwardSuccess() {
        return mrtMonitorRollForwardSuccess;
    }
    public long getMrtMonitorRollForwardError() {
        return mrtMonitorRollForwardError;
    }
    public long getMrtMonitorRollForwardTimeout() {
        return mrtMonitorRollForwardTimeout;
    }
    public long getMrtRollBackSuccess() {
        return mrtRollBackSuccess;
    }
    public long getMrtRollBackError() {
        return mrtRollBackError;
    }
    public long getMrtRollBackTimeout() {
        return mrtRollBackTimeout;
    }
    public long getMrtMonitorRollBackSuccess() {
        return mrtMonitorRollBackSuccess;
    }
    public long getMrtMonitorRollBackError() {
        return mrtMonitorRollBackError;
    }
    public long getMrtMonitorRollBackTimeout() {
        return mrtMonitorRollBackTimeout;
    }
    public long getFailXdrForbidden() {
        return failXdrForbidden;
    }
    public long getFailKeyBusy() {
        return failKeyBusy;
    }
    public long getFailXdrKeyBusy() {
        return failXdrKeyBusy;
    }
    public long getFailGeneration() {
        return failGeneration;
    }
    public long getFailRecordTooBig() {
        return failRecordTooBig;
    }
    public long getFailClientLostConflict() {
        return failClientLostConflict;
    }
    public long getFailXdrLostConflict() {
        return failXdrLostConflict;
    }
    public long getFailMrtBlocked() {
        return failMrtBlocked;
    }
    public long getFailMrtVersionMismatch() {
        return failMrtVersionMismatch;
    }
    public long getDeletedLastBin() {
        return deletedLastBin;
    }
    public long getMrtMonitorRollTombstoneCreates() {
        return mrtMonitorRollTombstoneCreates;
    }
    public long getTtlReductionsIgnored() {
        return ttlReductionsIgnored;
    }
    public long getTtlReductionsApplied() {
        return ttlReductionsApplied;
    }
    public long getActiveRack() {
        return activeRack;
    }
    public boolean isAllowTtlWithoutNsup() {
        return allowTtlWithoutNsup;
    }
    public boolean isAutoRevive() {
        return autoRevive;
    }
    public long getBackgroundQueryMaxRps() {
        return backgroundQueryMaxRps;
    }
    public ConflictResolutionPolicy getConflictResolutionPolicy() {
        return conflictResolutionPolicy;
    }
    public boolean isConflictResolveWrites() {
        return conflictResolveWrites;
    }
    public long getDefaultReadTouchTtlPct() {
        return defaultReadTouchTtlPct;
    }
    public long getDefaultTtl() {
        return defaultTtl;
    }
    public boolean isDisableColdStartEviction() {
        return disableColdStartEviction;
    }
    public boolean isDisableMrtWrites() {
        return disableMrtWrites;
    }
    public boolean isDisableWriteDupRes() {
        return disableWriteDupRes;
    }
    public boolean isDisallowExpunge() {
        return disallowExpunge;
    }
    public boolean isDisallowNullSetname() {
        return disallowNullSetname;
    }
    public boolean isEnableBenchmarksBatchSub() {
        return enableBenchmarksBatchSub;
    }
    public boolean isEnableBenchmarksOpsSub() {
        return enableBenchmarksOpsSub;
    }
    public boolean isEnableBenchmarksRead() {
        return enableBenchmarksRead;
    }
    public boolean isEnableBenchmarksUdf() {
        return enableBenchmarksUdf;
    }
    public boolean isEnableBenchmarksUdfSub() {
        return enableBenchmarksUdfSub;
    }
    public boolean isEnableBenchmarksWrite() {
        return enableBenchmarksWrite;
    }
    public boolean isEnableHistProxy() {
        return enableHistProxy;
    }
    public long getEvictHistBuckets() {
        return evictHistBuckets;
    }
    public long getEvictIndexesMemoryPct() {
        return evictIndexesMemoryPct;
    }
    public long getEvictTenthsPct() {
        return evictTenthsPct;
    }
    public boolean isForceLongQueries() {
        return forceLongQueries;
    }
    public boolean isIgnoreMigrateFillDelay() {
        return ignoreMigrateFillDelay;
    }
    public long getIndexStageSize() {
        return indexStageSize;
    }
    public long getIndexesMemoryBudget() {
        return indexesMemoryBudget;
    }
    public boolean isInlineShortQueries() {
        return inlineShortQueries;
    }
    public long getMaxRecordSize() {
        return maxRecordSize;
    }
    public long getMigrateOrder() {
        return migrateOrder;
    }
    public long getMigrateRetransmitMs() {
        return migrateRetransmitMs;
    }
    public boolean isMigrateSkipUnreadable() {
        return migrateSkipUnreadable;
    }
    public long getMigrateSleep() {
        return migrateSleep;
    }
    public long getMrtDuration() {
        return mrtDuration;
    }
    public long getNsupHistPeriod() {
        return nsupHistPeriod;
    }
    public long getNsupPeriod() {
        return nsupPeriod;
    }
    public long getNsupThreads() {
        return nsupThreads;
    }
    public long getPartitionTreeSprigs() {
        return partitionTreeSprigs;
    }
    public boolean isPreferUniformBalance() {
        return preferUniformBalance;
    }
    public long getRackId() {
        return rackId;
    }
    public ReadConsistencyLevelOverride getReadConsistencyLevelOverride() {
        return readConsistencyLevelOverride;
    }
    public boolean isRejectNonXdrWrites() {
        return rejectNonXdrWrites;
    }
    public boolean isRejectXdrWrites() {
        return rejectXdrWrites;
    }
    public long getReplicationFactor() {
        return replicationFactor;
    }
    public long getSindexStageSize() {
        return sindexStageSize;
    }
    public long getSingleQueryThreads() {
        return singleQueryThreads;
    }
    public long getStopWritesSysMemoryPct() {
        return stopWritesSysMemoryPct;
    }
    public boolean isStrongConsistency() {
        return strongConsistency;
    }
    public boolean isStrongConsistencyAllowExpunge() {
        return strongConsistencyAllowExpunge;
    }
    public long getTombRaiderEligibleIge() {
        return tombRaiderEligibleIge;
    }
    public long getTombRaiderPeriod() {
        return tombRaiderPeriod;
    }
    public long getTransactionPendingLimit() {
        return transactionPendingLimit;
    }
    public long getTruncateThreads() {
        return truncateThreads;
    }
    public WriteCommitLevelOverride getWriteCommitLevelOverride() {
        return writeCommitLevelOverride;
    }
    public long getXdrBinTombstoneTtl() {
        return xdrBinTombstoneTtl;
    }
    public long getXdrTombRaiderPeriod() {
        return xdrTombRaiderPeriod;
    }
    public long getXdrTombRaiderThreads() {
        return xdrTombRaiderThreads;
    }
    public IndexType getIndexStorageType() {
        return indexStorageType;
    }
    public IndexType getSindexStorageType() {
        return sindexStorageType;
    }
    public StorageEngine getStorageEngine() {
        return storageEngine;
    }
    public Geo2dsphereWithin getGeo2dsphereWithin() {
        return geo2dsphereWithin;
    }
}
