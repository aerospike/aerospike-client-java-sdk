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
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.operation.BitOperation;
import com.aerospike.client.sdk.operation.BitOverflowAction;
import com.aerospike.client.sdk.operation.BitPolicy;
import com.aerospike.client.sdk.operation.BitResizeFlags;
import com.aerospike.client.sdk.operation.BitWriteFlags;

/**
 * Unit tests (no cluster) for bit-operation fluent builders on {@link BinBuilder}.
 */
@Disabled
public class BitBuilderTest {

  @Test
  public void bitWriteOptionsMutuallyExclusiveCreateAndUpdate() {
    BitWriteOptions options = new BitWriteOptions();
    options.createOnly();
    assertThrows(IllegalStateException.class, options::updateOnly);
  }

  @Test
  public void bitWriteOptionsComposeFlags() {
    BitWriteOptions options = new BitWriteOptions()
        .updateOnly()
        .noFail()
        .partial();
    assertEquals(
        BitWriteFlags.UPDATE_ONLY | BitWriteFlags.NO_FAIL | BitWriteFlags.PARTIAL,
        options.toFlags());
  }

  @Test
  public void bitResizeDefaultQueuesBitModify() {
    TestOpBuilder builder = new TestOpBuilder();
    builder.bin("blob").bitResize(8);
    assertEquals(1, builder.ops.size());
    assertOperationEquals(
        BitOperation.resize(BitPolicy.Default, "blob", 8, BitResizeFlags.DEFAULT),
        builder.ops.get(0));
  }

  @Test
  public void bitResizeWithFlagsAndWriteOptions() {
    TestOpBuilder builder = new TestOpBuilder();
    BitWriteOptions options = new BitWriteOptions().noFail();
    builder.bin("blob").bitResize(10, BitResizeFlags.GROW_ONLY, options);
    assertOperationEquals(
        BitOperation.resize(new BitPolicy(BitWriteFlags.NO_FAIL), "blob", 10, BitResizeFlags.GROW_ONLY),
        builder.ops.get(0));
  }

  @Test
  public void bitModifyAndReadChain() {
    TestOpBuilder builder = new TestOpBuilder();
    byte[] mask = new byte[] {(byte) 0x0f};
    builder.bin("b")
        .bitSet(0, 8, mask)
        .bin("b")
        .bitOr(0, 8, mask, opt -> opt.updateOnly())
        .bin("b")
        .bitGet(0, 8);
    assertEquals(3, builder.ops.size());
    assertEquals(Operation.Type.BIT_MODIFY, builder.ops.get(0).type);
    assertEquals(Operation.Type.BIT_MODIFY, builder.ops.get(1).type);
    assertEquals(Operation.Type.BIT_READ, builder.ops.get(2).type);
  }

  @Test
  public void bitAddDefaultUnsignedFail() {
    TestOpBuilder builder = new TestOpBuilder();
    builder.bin("b").bitAdd(0, 16, 1);
    assertOperationEquals(
        BitOperation.add(BitPolicy.Default, "b", 0, 16, 1, false, BitOverflowAction.FAIL),
        builder.ops.get(0));
  }

  @Test
  public void bitAddWithSignedOverflowAndWriteOptions() {
    TestOpBuilder builder = new TestOpBuilder();
    builder.bin("b").bitAdd(0, 16, 1, true, BitOverflowAction.WRAP, opt -> opt.createOnly());
    assertOperationEquals(
        BitOperation.add(
            new BitPolicy(BitWriteFlags.CREATE_ONLY),
            "b",
            0,
            16,
            1,
            true,
            BitOverflowAction.WRAP),
        builder.ops.get(0));
  }

  private static void assertOperationEquals(Operation expected, Operation actual) {
    assertEquals(expected.type, actual.type);
    assertEquals(expected.binName, actual.binName);
    assertEquals(expected.value, actual.value);
  }

  private static final class TestOpBuilder extends AbstractOperationBuilder<TestOpBuilder> {
    private TestOpBuilder() {
      super(null, OpType.UPDATE);
    }

    @Override
    protected TestOpBuilder self() {
      return this;
    }
  }
}
