/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior;

public class Session {
    private final Cluster cluster;
    private final Behavior behavior;
    //private final IAerospikeClient client;

    protected Session(Cluster connection, Behavior behavior) {
        this.cluster = connection;
        this.behavior = behavior;
        //this.client = connection.getUnderlyingClient();
    }

    public class ExpressionBuilder {
        private Expression filterExpression = null;
        public ExpressionBuilder(Exp exp) {
            this.filterExpression = Exp.build(exp);
        }

        public ExpressionBuilder(Expression exp) {
            this.filterExpression = exp;
        }

        public Expression getFilterExpression() {
            return filterExpression;
        }
    }

//    public static abstract class AbstractBuilder<T extends AbstractBuilder> {
////        private RetrySettings retrySettings;
//        private boolean useCompression = false;
//        private boolean useCompressionSet = false;
//        private boolean sendKey = false;
//        private boolean sendKeySet = false;
//        private final Behavior behavor;
//        public AbstractBuilder(Behavior behavor) {
//            this.behavor = behavor;
//        }
//
//        protected Behavior getBehavior() {
//            return this.behavor;
//        }
//
//        public T usingCompression() {
//            this.useCompression = true;
//            this.useCompressionSet = true;
//            return (T)this;
//        }
//
//        public T usingCompression(boolean compress) {
//            this.useCompression = compress;
//            this.useCompressionSet = true;
//            return (T)this;
//        }
//
//        public T withSendKey(boolean sendKey) {
//            this.sendKey = sendKey;
//            this.sendKeySet = true;
//            return (T)this;
//        }
//
//        protected void mutatePolicy(@NotNull Policy policy) {
//            if (this.sendKeySet) {
//                policy.sendKey = this.sendKey;
//            }
//            else {
//                policy.sendKey = this.getBehavior().getSendKey();
//            }
//            if (this.useCompressionSet) {
//                policy.compress = this.useCompression;
//            }
//            else {
//                policy.compress = this.getBehavior().getUseCompression();
//            }
//        }
//    }
//
//    public static class WriteBuilder extends AbstractBuilder<WriteBuilder> implements BinSetter {
//        private int generation = 0;
//        private Expression filterExpression;
//        private RecordExistsAction action;
//        private final CommandBuilder commandBuilder;
//
//        // TODO: Make these one common suppertype and one list.
//        private List<Bin> bins = new ArrayList<>();
//        private List<DSLPath> dsls = new ArrayList<>();
//
//        private WriteBuilder(CommandBuilder commandBuilder) {
//            super(commandBuilder.getSession().getBehavior());
//            this.commandBuilder = commandBuilder;
//        }
//        private WriteBuilder(CommandBuilder commandBuilder, Bin bin) {
//            this(commandBuilder);
//            this.bins.add(bin);
//        }
//
//        public WriteBuilder(CommandBuilder commandBuilder, DSLPath dsl) {
//            this(commandBuilder);
//            this.dsls.add(dsl);
//        }
//        public CommandBuilder getCommandBuilder() {
//            return commandBuilder;
//        }
//
//        public void set(Bin bin) {
//            bins.add(bin);
//        }
//        public BinBuilder<WriteBuilder> aBinNamed(String name) {
//            return new BinBuilder<Session.WriteBuilder>(name, this);
//        }
//        public WriteBuilder requiringGenerationOf(int generation) {
//            this.generation = generation;
//            return this;
//        }
//
//        public WriteBuilder withBins(Bin ...bins) {
//            this.bins = Arrays.asList(bins);
//            return this;
//        }
//
//        public WriteBuilder put(String name, String value) {
//            this.bins.add(new Bin(name, value));
//            return this;
//        }
//        public WriteBuilder put(String name, int value) {
//            this.bins.add(new Bin(name, value));
//            return this;
//        }
//        public WriteBuilder put(String name, long value) {
//            this.bins.add(new Bin(name, value));
//            return this;
//        }
//        public WriteBuilder delete(String name) {
//            this.bins.add(Bin.asNull(name));
//            return this;
//        }
//        protected void mutatePolicy(@NotNull WritePolicy policy) {
//            super.mutatePolicy(policy);
//            if (this.generation > 0) {
//                policy.generation = this.generation;
//                policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
//            }
//            else {
//                policy.generationPolicy = GenerationPolicy.NONE;
//            }
//            policy.recordExistsAction = action;
//            policy.filterExp = this.filterExpression;
//        }
//
//        protected void mutatePolicy(@NotNull BatchWritePolicy policy) {
//            if (this.generation > 0) {
//                policy.generation = this.generation;
//                policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
//            }
//            else {
//                policy.generationPolicy = GenerationPolicy.NONE;
//            }
//            policy.recordExistsAction = action;
//            policy.filterExp = this.filterExpression;
//        }
//
//        public WriteBuilder when(Expression expression) {
//            this.filterExpression = expression;
//            return this;
//        }
//        public WriteBuilder when(Exp expression) {
//            this.filterExpression = Exp.build(expression);
//            return this;
//        }
//        public void execute() {
//            this.action = RecordExistsAction.UPDATE;
//            Key[] keys = this.getCommandBuilder().keys;
//            Session session = this.getCommandBuilder().getSession();
//            Behavior behavior = session.getBehavior();
//
//            if (keys.length == 1) {
//                WritePolicy writePolicy = behavior.getSharedPolicy(CommandType.WRITE_NON_RETRYABLE);
//                mutatePolicy(writePolicy);
//                // TODO: Factor in the action; refactor the bins vs DSL when merged into one superclass
//                if (dsls.size() > 0) {
//                    DSLPath dsl = dsls.get(0);
//                    if (dsl.hasOperand()) {
//                        ExpressionPart operand = dsl.getOperand();
//                        Operation operation;
//                        if (operand.isIndex()) {
//                            operation = ListOperation.removeByIndex(dsl.getBinName(), operand.getIndex(), ListReturnType.NONE, dsl.getContext());
//                        }
//                        else {
//                            operation = MapOperation.removeByKey(dsl.getBinName(), Value.get(operand.getName()), MapReturnType.NONE, dsl.getContext());
//                        }
//                        session.getClient().operate(writePolicy, keys[0], operation);
//                    }
//                    else {
//                        session.getClient().put(writePolicy, keys[0], Bin.asNull(dsl.getBinName()));
//                    }
//                }
//                else {
//                    session.getClient().put(writePolicy, keys[0], bins.toArray(new Bin[0]));
//                }
//            }
//            else {
//                // Need to do a batch write here
//                BatchPolicy batchPolicy = behavior.getSharedPolicy(CommandType.BATCH_WRITE);
//                super.mutatePolicy(batchPolicy);
//                BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
//                batchWritePolicy.sendKey = batchPolicy.sendKey;
//                batchWritePolicy.filterExp = batchPolicy.filterExp;
//                mutatePolicy(batchWritePolicy);
//                List<Operation> operations = bins.stream().map((bin) -> Operation.put(bin)).collect(Collectors.toList());
//                session.getClient().operate(batchPolicy, batchWritePolicy, keys, operations.toArray(new Operation[0]));
//            }
//        }
//    }
//
//    public static class ReadBuilder extends AbstractBuilder<ReadBuilder> {
//        private String[] bins = null;
//        private final CommandBuilder commandBuilder;
//        private boolean includeBins = true;
//
//        public ReadBuilder(CommandBuilder commandBuilder) {
//            super(commandBuilder.getSession().getBehavior());
//            this.commandBuilder = commandBuilder;
//        }
//
//        public ReadBuilder(CommandBuilder commandBuilder, String ...bins) {
//            super(commandBuilder.getSession().getBehavior());
//            this.commandBuilder = commandBuilder;
//            this.readBins(bins);
//        }
//
//        public ReadBuilder readBins(String ...bins) {
//            this.bins = bins;
//            return this;
//        }
//
//        public ReadBuilder withoutBins() {
//            this.includeBins = false;
//            return this;
//        }
//
//        public RecordList execute() {
//            Key[] keys = this.commandBuilder.getKeys();
//            Session session = this.commandBuilder.getSession();
//            Behavior behavior = session.getBehavior();
//
//            if (keys.length >1) {
//                BatchPolicy policy = behavior.getMutablePolicy(CommandType.BATCH_READ);
//                mutatePolicy(policy);
//                if (!includeBins) {
//                    return new RecordList(session.getClient().getHeader(policy, keys));
//                }
//                else if (bins == null) {
//                    return new RecordList(session.getClient().get(policy, keys));
//                }
//                else {
//                    return new RecordList(session.getClient().get(policy, keys, bins));
//                }
//            }
//            else {
//                Policy policy = behavior.getMutablePolicy(CommandType.READ_SC);
//                mutatePolicy(policy);
//                if (!includeBins) {
//                    return new RecordList(session.getClient().getHeader(policy, keys[0]));
//                }
//                else if (bins == null) {
//                    return new RecordList(session.getClient().get(policy, keys[0]));
//                }
//                else {
//                    return new RecordList(session.getClient().get(policy, keys[0], bins));
//                }
//            }
//        }
//    }
//
//    public static interface BinSetter {
//        void set(Bin bin);
//    }
//    public static class BinBuilder<T extends BinSetter> {
//        private String name;
//        private T parent;
//        public BinBuilder(String name, T parent) {
//            this.parent = parent;
//            this.name = name;
//        }
//        public T set(String value) {
//            parent.set(new Bin(name, value));
//            return parent;
//        }
//        public T set(int value) {
//            parent.set(new Bin(name, value));
//            return parent;
//        }
//        public T set(long value) {
//            parent.set(new Bin(name, value));
//            return parent;
//        }
//        public T remove() {
//            parent.set(Bin.asNull(name));
//            return parent;
//        }
//    }
//    public static enum OperationType {PUT, GET, OPERATE};
//
//    public static class CommandBuilder {
//        private final Key[] keys;
//        private final Session session;
//        private OperationType type = null;
//        public CommandBuilder(Session session, Key key, Key ...keys) {
//            this.session = session;
//            if (keys == null || keys.length == 0) {
//                this.keys = new Key[] { key };
//            }
//            else {
//                this.keys = new Key[keys.length+1];
//                this.keys[0] = key;
//                for (int i = 0; i < keys.length; i++) {
//                    this.keys[1+i] = keys[i];
//                }
//            }
//        }
//
//        public CommandBuilder(Session session, Key[] keys) {
//            this.session = session;
//            this.keys = keys;
//        }
//
//        public CommandBuilder(Session session, List<Key> keys) {
//            this.session = session;
//            if (keys == null || keys.size() == 0) {
//                throw new IllegalArgumentException("At least one key must be specified");
//            }
//            this.keys = keys.toArray(new Key[0]);
//        }
//
//        protected Session getSession() {
//            return session;
//        }
//        protected Key[] getKeys() {
//            return keys;
//        }
//        public WriteBuilder put() {
//            return new WriteBuilder(this);
//        }
//        public WriteBuilder put(String name, String value) {
//            return new WriteBuilder(this, new Bin(name, value));
//        }
//        public WriteBuilder put(String name, int value) {
//            return new WriteBuilder(this, new Bin(name, value));
//        }
//        public WriteBuilder put(String name, long value) {
//            return new WriteBuilder(this, new Bin(name, value));
//        }
//        public WriteBuilder put(String name, Map<?,?> value) {
//            return new WriteBuilder(this, new Bin(name, value));
//        }
//        public WriteBuilder put(String name, List<?> value) {
//            return new WriteBuilder(this, new Bin(name, value));
//        }
//        public ReadBuilder get() {
//            return new ReadBuilder(this);
//        }
//        public ReadBuilder get(String ... bins) {
//            return new ReadBuilder(this, bins);
//        }
//
//        public WriteBuilder delete(DSLPath dsl) {
//            return new WriteBuilder(this, dsl);
//        }
//    }
//
//    public CommandBuilder on(Key key, Key ...keys) {
//        return new CommandBuilder(this, key, keys);
//    }
//
//    public CommandBuilder on(Key[] keys) {
//        return new CommandBuilder(this, keys);
//    }
//
//    public CommandBuilder on(List<Key> keys) {
//        return new CommandBuilder(this, keys);
//    }
//
    public Behavior getBehavior() {
        return this.behavior;
    }

    // TODO: Remove ASNode, InfoData
    /*
    public ASNode[] getNodes() {
        Node[] nodes = this.client.getNodes();
        ASNode[] asNodes = new ASNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            asNodes[i] = new ASNode(nodes[i]);
        }
        return asNodes;
    }*/

//    public NamespaceInfo getNamespaceInfo(String namespaceName) {
//        return getNamespaceInfo(namespaceName, -1);
//    }
//    public NamespaceInfo getNamespaceInfo(String namespaceName, int refreshIntervalInSecs) {
//        return new NamespaceInfo(namespaceName, null, this, refreshIntervalInSecs);
//    }
/*
    public IAerospikeClient getClient() {
        return client;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void truncate(DataSet set) {
        this.client.truncate(null, set.getNamespace(), set.getSet(), null);
    }

    public RecordMappingFactory getRecordMappingFactory() {
        return this.cluster.getRecordMappingFactory();
    }

    private List<Key> buildKeyList(Key key1, Key key2, Key ...keys) {
        List<Key> keyList = new ArrayList<>();
        keyList.add(key1);
        keyList.add(key2);
        for (Key thisKey : keys) {
            keyList.add(thisKey);
        }
        return keyList;
    }
*/
    // --------------------------------------------
    // Query functionality
    // --------------------------------------------
    /*
    public QueryBuilder query(DataSet dataSet) {
        return new QueryBuilder(this, dataSet);
    }

    public QueryBuilder query(Key key) {
        return new QueryBuilder(this, key);
    }
*/
    /**
     * Point or batch read with one or more keys. Query with no parameters is valid, so must have (Key, Key...) to differentiate
     * @param key
     * @param keys
     * @return
     */
  /*
    public QueryBuilder query(Key key1, Key key2, Key...keys) {
        return new QueryBuilder(this, buildKeyList(key1, key2, keys));
    }

    public QueryBuilder query(List<Key> keyList) {
        return new QueryBuilder(this, keyList);
    }
*/
    // -------------------
    // CUD functionality
    // -------------------
    /*
    public OperationBuilder insertInto(Key key) {
        return new OperationBuilder(this, key, OpType.INSERT);
    }

    public OperationBuilder update(Key key) {
        return new OperationBuilder(this, key, OpType.UPDATE);
    }

    public OperationBuilder upsert(Key key) {
        return new OperationBuilder(this, key, OpType.UPSERT);
    }

    public OperationBuilder replace(Key key) {
        return new OperationBuilder(this, key, OpType.UPSERT);
    }

    public OperationBuilder upsert(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.UPSERT);
    }

    public OperationBuilder upsert(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.UPSERT);
    }

    public OperationBuilder insertInto(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.INSERT);
    }

    public OperationBuilder insertInto(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.INSERT);
    }

    public OperationBuilder update(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.UPDATE);
    }

    public OperationBuilder update(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.UPDATE);
    }

    public OperationBuilder replace(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.REPLACE);
    }

    public OperationBuilder replace(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.REPLACE);
    }

    public OperationWithNoBinsBuilder touch(Key key) {
        return new OperationWithNoBinsBuilder(this, key, OpType.TOUCH);
    }

    public OperationWithNoBinsBuilder touch(Key key1, Key key2, Key ... keys) {
        return new OperationWithNoBinsBuilder(this, buildKeyList(key1, key2, keys), OpType.TOUCH);
    }

    public OperationWithNoBinsBuilder touch(List<Key> keys) {
        return new OperationWithNoBinsBuilder(this, keys, OpType.TOUCH);
    }

    public OperationWithNoBinsBuilder exists(Key key) {
        return new OperationWithNoBinsBuilder(this, key, OpType.EXISTS);
    }

    public OperationWithNoBinsBuilder exists(Key key1, Key key2, Key ... keys) {
        return new OperationWithNoBinsBuilder(this, buildKeyList(key1, key2, keys), OpType.EXISTS);
    }

    public OperationWithNoBinsBuilder exists(List<Key> keys) {
        return new OperationWithNoBinsBuilder(this, keys, OpType.EXISTS);
    }
    public OperationWithNoBinsBuilder delete(Key key) {
        return new OperationWithNoBinsBuilder(this, key, OpType.DELETE);
    }

    public OperationWithNoBinsBuilder delete(Key key1, Key key2, Key ... keys) {
        return new OperationWithNoBinsBuilder(this, buildKeyList(key1, key2, keys), OpType.DELETE);
    }

    public OperationWithNoBinsBuilder delete(List<Key> keys) {
        return new OperationWithNoBinsBuilder(this, keys, OpType.DELETE);
    }

    // --------------------------------
    // Object mapping functionality
    // --------------------------------
    public OperationObjectBuilder insertInto(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.INSERT);
    }

    public <T> OperationObjectBuilder<T> insertInto(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.INSERT);
    }

    public OperationObjectBuilder upsert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPSERT);
    }

    public <T> OperationObjectBuilder<T> upsert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.UPSERT);
    }

    public OperationObjectBuilder update(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPDATE);
    }

    public <T> OperationObjectBuilder<T> update(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.UPDATE);
    }
*/

    // ---------------------------
    // Transaction functionality
    // ---------------------------
    /**
     * Return the current transaction, if any.
     * @return
     */
    public Txn getCurrentTransaction() {
        return null;
    }

    // --------------------------------------
    // Transaction helper methods
    // --------------------------------------
    // Functional interface for returning a result
    /*
    @FunctionalInterface
    public interface Transactional<T> {
        T execute(TransactionalSession txn);
    }

    // Functional interface for void-returning operations
    @FunctionalInterface
    public interface TransactionalVoid {
        void execute(TransactionalSession txn);
    }

    public <T> T doInTransaction(Transactional<T> operation) {
        return new TransactionalSession(cluster, behavior).doInTransaction(operation);
    }

    public void doInTransaction(TransactionalVoid operation) {
        new TransactionalSession(cluster, behavior).doInTransaction(txn -> {
            operation.execute(txn);
//            return null; // Hidden from user
        });
    }
*/
    // ---------------------
    // Info functionality
    // ---------------------
    /*
    public InfoCommands info() {
        return new InfoCommands(this);
    }

    public boolean isNamespaceSC(String namespace) {
        Partitions partitionMap = this.getClient().getCluster().partitionMap.get(namespace);
        if (partitionMap == null) {
            throw new IllegalArgumentException("Unknown namespace " + namespace);
        }
        return partitionMap.scMode;
    }*/
}
