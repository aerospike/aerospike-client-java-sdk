/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.examples;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DefaultRecordMappingFactory;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.RecordReadContext;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.util.MapUtil;

/**
 * Minimal end-to-end example of {@link TypedDataSet}, {@link RecordMapper},
 * {@link DefaultRecordMappingFactory}, and mapper-less reads on
 * {@link TypedRecordStream}, plus a heterogeneous batch read using
 * {@link com.aerospike.client.sdk.TypedKey} and {@link RecordResult#toObject()}.
 *
 * <p>{@link WidgetMapper} overrides {@link RecordMapper#fromMap(Map, Key, int, RecordReadContext)}
 * to load a related {@link Gadget} through {@link RecordReadContext#getSession()} when the
 * {@code related_gadget_id} bin is set.</p>
 *
 * <p>The example uses the runner-managed cluster and configured namespace.</p>
 */
public final class TypedMappingExamples extends Example {

    @Override
    protected boolean requiresStringAel() {
        return true;
    }

    /** Simple inventory row stored under an integer user key. */
    public static final class Widget {
        private long id;
        private String label;
        private int quantity;
        /** When non-zero, {@link WidgetMapper} can load this gadget key via {@link Session} in 4-arg {@code fromMap}. */
        private long relatedGadgetId;

        public Widget() {
        }

        public Widget(long id, String label, int quantity) {
            this(id, label, quantity, 0L);
        }

        public Widget(long id, String label, int quantity, long relatedGadgetId) {
            this.id = id;
            this.label = label;
            this.quantity = quantity;
            this.relatedGadgetId = relatedGadgetId;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public long getRelatedGadgetId() {
            return relatedGadgetId;
        }

        public void setRelatedGadgetId(long relatedGadgetId) {
            this.relatedGadgetId = relatedGadgetId;
        }

        @Override
        public String toString() {
            return "Widget{id=" + id + ", label='" + label + "', quantity=" + quantity
                + (relatedGadgetId != 0 ? ", relatedGadgetId=" + relatedGadgetId : "")
                + '}';
        }
    }

    /** Feature flag row stored under an integer user key. */
    public static final class Gadget {
        private long id;
        private String name;
        private boolean enabled;

        public Gadget() {
        }

        public Gadget(long id, String name, boolean enabled) {
            this.id = id;
            this.name = name;
            this.enabled = enabled;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public String toString() {
            return "Gadget{id=" + id + ", name='" + name + "', enabled=" + enabled + '}';
        }
    }

    public static final class WidgetMapper implements RecordMapper<Widget> {
        private final TypedDataSet<Gadget> gadgets;
        private final Console console;

        public WidgetMapper(TypedDataSet<Gadget> gadgets, Console console) {
            this.gadgets = gadgets;
            this.console = console;
        }

        private static Widget mapBins(Map<String, Object> map, Key recordKey) {
            Widget w = new Widget();
            w.setId(recordKey.userKey.toLong());
            w.setLabel(MapUtil.asString(map, "label"));
            w.setQuantity(MapUtil.asInt(map, "qty"));
            w.setRelatedGadgetId(MapUtil.asLong(map, "rel_gadget_id"));
            return w;
        }

        @Override
        public Widget fromMap(Map<String, Object> map, Key recordKey, int generation) {
            return mapBins(map, recordKey);
        }

        /**
         * When {@code rel_gadget_id} is set on the widget record, loads that gadget through
         * {@link RecordReadContext#getSession()} to demonstrate session-scoped dependent reads.
         */
        @Override
        public Widget fromMap(Map<String, Object> map, Key recordKey, int generation, RecordReadContext<Widget> ctx) {
            Widget w = mapBins(map, recordKey);
            long relatedId = w.getRelatedGadgetId();
            if (relatedId <= 0) {
                return w;
            }
            Session session = ctx.getSession();
            Optional<Gadget> peer = session.query(gadgets.id(relatedId))
                    .readingOnlyBins("name", "enabled")
                    .limit(1)
                    .execute()
                    .getFirst()
                    .map(RecordResult::toObject);
            if (peer.isPresent()) {
                Gadget g = peer.get();
                console.info("  [WidgetMapper 4-arg fromMap] Session read of related gadget id="
                    + relatedId + " -> name='" + g.getName() + "', enabled=" + g.isEnabled());
            } else {
                console.info("  [WidgetMapper 4-arg fromMap] No gadget found for rel_gadget_id="
                    + relatedId);
            }
            return w;
        }

        @Override
        public Map<String, Object> toMap(Widget widget) {
            var b = MapUtil.buildMap()
                    .add("label", widget.getLabel())
                    .add("qty", widget.getQuantity());
            if (widget.getRelatedGadgetId() > 0) {
                b.add("rel_gadget_id", widget.getRelatedGadgetId());
            }
            return b.done();
        }

        @Override
        public Object id(Widget widget) {
            return widget.getId();
        }
    }

    public static final class GadgetMapper implements RecordMapper<Gadget> {
        @Override
        public Gadget fromMap(Map<String, Object> map, Key recordKey, int generation) {
            Gadget g = new Gadget();
            g.setId(recordKey.userKey.toLong());
            g.setName(MapUtil.asString(map, "name"));
            g.setEnabled(MapUtil.asInt(map, "enabled") != 0);
            return g;
        }

        @Override
        public Map<String, Object> toMap(Gadget gadget) {
            return MapUtil.buildMap()
                    .add("name", gadget.getName())
                    .add("enabled", gadget.isEnabled() ? 1 : 0)
                    .done();
        }

        @Override
        public Object id(Gadget gadget) {
            return gadget.getId();
        }
    }

    @Override
    public void runExample() throws Exception {
        Cluster cluster = cluster();
        TypedDataSet<Widget> widgets = TypedDataSet.of(namespace(), "typed_demo_widgets", Widget.class);
        TypedDataSet<Gadget> gadgets = TypedDataSet.of(namespace(), "typed_demo_gadgets", Gadget.class);

        WidgetMapper widgetMapper = new WidgetMapper(gadgets, console);
        GadgetMapper gadgetMapper = new GadgetMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(
                Widget.class, widgetMapper,
                Gadget.class, gadgetMapper));

        Session session = cluster.createSession(Behavior.DEFAULT);

        // Writes: factory resolves mapper from object class (no .using()).
        // Insert gadget first so widget mapper's 4-arg fromMap can resolve it via session.query.
        session.insert(gadgets).object(new Gadget(1, "notifications", true)).execute();
        session.insert(widgets).object(new Widget(1, "alpha", 10, 1L)).execute();
        session.insert(widgets).object(new Widget(2, "beta", 20)).execute();

        // Single-row read via typed query stream: factory only (no .using() / no mapper arg).
        // Widget id=1 has related_gadget_id=1; WidgetMapper 4-arg fromMap loads that gadget via session.
        console.info("Typed query getFirstObject — watch for [WidgetMapper 4-arg fromMap] line:");
        Widget one = session.query(widgets)
                .where("$.qty == 10")
                .limit(1)
                .execute()
                .getFirstObject()
                .orElseThrow();
        console.info("Typed query getFirstObject (factory): " + one);

        // Typed dataset query → TypedRecordStream: mapper-less toObjectList() (4-arg fromMap per row).
        List<Widget> fromScan = session.query(widgets)
                .where("$.qty == 20")
                .execute()
                .toObjectList();
        console.info("Typed query (factory, qty=20 only): " + fromScan);

        console.info("Typed query toObjectList(all widgets) — id=1 triggers session read again:");
        List<Widget> allWidgets = session.query(widgets).limit(10).execute().toObjectList();
        console.info("  " + allWidgets);

        Optional<Widget> firstTyped = session.query(widgets)
                .where("$.label == 'alpha'")
                .execute()
                .getFirstObject();
        console.info("Typed query first (factory): " + firstTyped.orElseThrow());

        // Heterogeneous batch: each leg carries Class<?>; map per row with toObject() (embedded session).
        RecordStream batch = session
                .query(widgets.id(2))
                    .readingOnlyBins("label", "qty")
                .query(gadgets.id(1))
                    .readingOnlyBins("name", "enabled")
                .execute();
        try (batch) {
            Widget w = batch.next().toObject();
            Gadget g = batch.next().toObject();
            console.info("Batch widget: " + w);
            console.info("Batch gadget: " + g);
        }
    }
}
