package com.aerospike.client.fluent.policy;

import java.util.List;

public enum NodeCategory {
    MASTER,
    MASTER_OR_REPLICA,
    MASTER_OR_REPLICA_IN_RACK,
    ANY_REPLICA,
    REPLICA_IN_RACK,
    RANDOM,
    RANDOM_IN_RACK;
    
    public static List<NodeCategory> SEQUENCE = List.of(NodeCategory.MASTER, NodeCategory.ANY_REPLICA);
    public static List<NodeCategory> ALLOW_RACK = List.of(NodeCategory.MASTER_OR_REPLICA_IN_RACK);

}