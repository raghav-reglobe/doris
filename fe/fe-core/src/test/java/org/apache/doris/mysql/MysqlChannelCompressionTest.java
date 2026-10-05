// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.mysql;

import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.xnio.StreamConnection;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * The MySQL compressed protocol on the FE channel: frames written to the wire carry exactly the plain packet
 * stream, split and numbered as the protocol says, and frames read from the wire are reassembled into the
 * packets the plain reader would have seen.
 */
public class MysqlChannelCompressionTest {

    private static final int HEADER = MysqlChannel.COMPRESSED_HEADER_LEN;

    /** One compressed frame as decoded from the wire. */
    private static final class Frame {
        final int compressedLen;
        final int seq;
        final int uncompressedLen;
        final byte[] payload;

        Frame(int compressedLen, int seq, int uncompressedLen, byte[] payload) {
            this.compressedLen = compressedLen;
            this.seq = seq;
            this.uncompressedLen = uncompressedLen;
            this.payload = payload;
        }

        byte[] plain() throws Exception {
            if (uncompressedLen == 0) {
                return payload;
            }
            Inflater inflater = new Inflater();
            inflater.setInput(payload);
            byte[] out = new byte[uncompressedLen];
            int n = 0;
            while (n < uncompressedLen) {
                int got = inflater.inflate(out, n, uncompressedLen - n);
                Assertions.assertTrue(got > 0 || !inflater.finished(), "stream ended before the declared length");
                n += got;
            }
            Assertions.assertTrue(inflater.finished(), "stream longer than the declared length");
            inflater.end();
            return out;
        }
    }

    /** A channel over a mocked connection: everything written lands in {@code wire}, reads come from {@code input}. */
    private static final class Harness {
        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        final ByteBuffer input;
        final MysqlChannel channel;

        Harness(byte[] inputBytes) throws IOException {
            input = ByteBuffer.wrap(inputBytes);
            StreamConnection connection = Mockito.mock(StreamConnection.class);
            Mockito.when(connection.getPeerAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 3306));
            ConduitStreamSinkChannel sink = Mockito.mock(ConduitStreamSinkChannel.class);
            Mockito.when(connection.getSinkChannel()).thenReturn(sink);
            Mockito.when(sink.write(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
                ByteBuffer buffer = inv.getArgument(0);
                int len = buffer.remaining();
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                wire.write(bytes);
                return len;
            });
            Mockito.when(sink.flush()).thenReturn(true);
            ConduitStreamSourceChannel source = Mockito.mock(ConduitStreamSourceChannel.class);
            Mockito.when(connection.getSourceChannel()).thenReturn(source);
            Mockito.when(source.read(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
                ByteBuffer buffer = inv.getArgument(0);
                if (!input.hasRemaining()) {
                    return -1;
                }
                int len = Math.min(buffer.remaining(), input.remaining());
                byte[] bytes = new byte[len];
                input.get(bytes);
                buffer.put(bytes);
                return len;
            });
            ConnectContext ctx = new ConnectContext(connection);
            channel = new MysqlChannel(connection, ctx);
        }

        /** Arms compression and flushes the one plain packet that stands for the authentication OK. */
        void armAndActivate() throws IOException {
            channel.armCompressionAfterNextFlush(1);
            channel.sendAndFlush(ByteBuffer.wrap(new byte[] {0}));
            Assertions.assertTrue(channel.isCompressionActive());
        }

        byte[] wireBytes() {
            return wire.toByteArray();
        }
    }

    private static List<Frame> decodeFrames(byte[] wire, int from) {
        List<Frame> frames = new ArrayList<>();
        int pos = from;
        while (pos < wire.length) {
            Assertions.assertTrue(pos + HEADER <= wire.length, "truncated compressed header at " + pos);
            int compressedLen = (wire[pos] & 0xFF) | ((wire[pos + 1] & 0xFF) << 8) | ((wire[pos + 2] & 0xFF) << 16);
            int seq = wire[pos + 3] & 0xFF;
            int uncompressedLen = (wire[pos + 4] & 0xFF) | ((wire[pos + 5] & 0xFF) << 8)
                    | ((wire[pos + 6] & 0xFF) << 16);
            pos += HEADER;
            Assertions.assertTrue(pos + compressedLen <= wire.length, "truncated compressed payload at " + pos);
            byte[] payload = new byte[compressedLen];
            System.arraycopy(wire, pos, payload, 0, compressedLen);
            pos += compressedLen;
            frames.add(new Frame(compressedLen, seq, uncompressedLen, payload));
        }
        return frames;
    }

    private static byte[] plainStream(List<Frame> frames) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Frame frame : frames) {
            out.write(frame.plain());
        }
        return out.toByteArray();
    }

    private static byte[] textPayload(int len) {
        byte[] bytes = new byte[len];
        byte[] line = "2026-10-05 order 1234567 | cashify_mart.orders | 1.00 | delivered\n"
                .getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < len; i++) {
            bytes[i] = line[i % line.length];
        }
        return bytes;
    }

    private static byte[] randomPayload(int len) {
        byte[] bytes = new byte[len];
        new Random(7).nextBytes(bytes);
        return bytes;
    }

    /** The packets of this test, sent through a channel; returns the bytes a plain channel would write. */
    private static void sendPackets(MysqlChannel channel, List<byte[]> packets) throws IOException {
        for (byte[] packet : packets) {
            channel.sendOnePacket(ByteBuffer.wrap(packet));
        }
        channel.flush();
    }

    @Test
    public void testWriteFramesCarryThePlainPacketStream() throws Exception {
        byte[] tiny = new byte[] {1, 2, 3};                 // under MIN_COMPRESS_LENGTH: travels raw
        byte[] noise = randomPayload(4 * 1024);             // incompressible: zlib cannot shrink it, travels raw
        List<byte[]> bulk = new ArrayList<>();
        bulk.add(textPayload(200 * 1024));                  // compressible: a deflated frame
        bulk.add(textPayload(MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH + 1024 * 1024)); // split over frames

        // the reference: the same packets through a plain channel, whatever the flush boundaries
        Harness plain = new Harness(new byte[0]);
        plain.channel.setSequenceId(0);
        plain.channel.sendAndFlush(ByteBuffer.wrap(tiny));
        plain.channel.sendAndFlush(ByteBuffer.wrap(noise));
        sendPackets(plain.channel, bulk);
        byte[] expected = plain.wireBytes();

        Harness compressed = new Harness(new byte[0]);
        compressed.armAndActivate();
        int authResponseLen = compressed.wireBytes().length;
        Assertions.assertEquals(4 + 1, authResponseLen, "the authentication response travels plain");
        compressed.channel.setSequenceId(0);
        compressed.channel.sendAndFlush(ByteBuffer.wrap(tiny));
        compressed.channel.sendAndFlush(ByteBuffer.wrap(noise));
        sendPackets(compressed.channel, bulk);

        List<Frame> frames = decodeFrames(compressed.wireBytes(), authResponseLen);
        Assertions.assertArrayEquals(expected, plainStream(frames));
        for (int i = 0; i < frames.size(); i++) {
            Assertions.assertEquals(i & 0xFF, frames.get(i).seq, "compressed sequence id of frame " + i);
            Assertions.assertTrue(frames.get(i).compressedLen <= MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH);
            Assertions.assertTrue(frames.get(i).uncompressedLen <= MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH);
        }
        Assertions.assertEquals(0, frames.get(0).uncompressedLen, "a short frame is sent raw");
        Assertions.assertEquals(4 + tiny.length, frames.get(0).compressedLen);
        Assertions.assertEquals(0, frames.get(1).uncompressedLen, "an incompressible frame is sent raw");
        Assertions.assertEquals(4 + noise.length, frames.get(1).compressedLen);
        Assertions.assertTrue(frames.stream().anyMatch(f -> f.uncompressedLen > 0
                && f.compressedLen < f.uncompressedLen), "a text frame is deflated");
        Assertions.assertTrue(frames.size() >= 5, "a packet past 16 MiB spans more than one frame");
        Assertions.assertTrue(compressed.wireBytes().length < expected.length / 4, "text shrinks on the wire");
    }

    private static byte[] packet(int seq, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + payload.length);
        buffer.put((byte) payload.length).put((byte) (payload.length >> 8)).put((byte) (payload.length >> 16));
        buffer.put((byte) seq);
        buffer.put(payload);
        return buffer.array();
    }

    private static byte[] frame(int seq, byte[] plain, boolean deflate) {
        byte[] payload = plain;
        int uncompressedLen = 0;
        if (deflate) {
            Deflater deflater = new Deflater(6);
            deflater.setInput(plain);
            deflater.finish();
            byte[] out = new byte[plain.length + 64];
            int n = deflater.deflate(out);
            Assertions.assertTrue(deflater.finished());
            deflater.end();
            payload = new byte[n];
            System.arraycopy(out, 0, payload, 0, n);
            uncompressedLen = plain.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(HEADER + payload.length);
        buffer.put((byte) payload.length).put((byte) (payload.length >> 8)).put((byte) (payload.length >> 16));
        buffer.put((byte) seq);
        buffer.put((byte) uncompressedLen).put((byte) (uncompressedLen >> 8)).put((byte) (uncompressedLen >> 16));
        buffer.put(payload);
        return buffer.array();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    @Test
    public void testReadReassemblesPacketsAcrossFrames() throws Exception {
        byte[] p1 = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = textPayload(100 * 1024);
        byte[] p3 = textPayload(60);
        byte[] stream = concat(packet(0, p1), packet(1, p2), packet(2, p3));
        // frame 0 raw: the first packet plus the head of the second; frame 1 deflated: the rest of the second and
        // the whole third; frame 2 raw: nothing but the tail of the third
        int cut1 = 4 + p1.length + 5;
        int cut2 = stream.length - 10;
        byte[] wire = concat(
                frame(0, Arrays.copyOfRange(stream, 0, cut1), false),
                frame(1, Arrays.copyOfRange(stream, cut1, cut2), true),
                frame(2, Arrays.copyOfRange(stream, cut2, stream.length), false));

        Harness harness = new Harness(wire);
        harness.armAndActivate();
        harness.channel.setSequenceId(0);

        ByteBuffer got = harness.channel.fetchOnePacket();
        Assertions.assertArrayEquals(p1, remaining(got));
        got = harness.channel.fetchOnePacket();
        Assertions.assertArrayEquals(p2, remaining(got));
        got = harness.channel.fetchOnePacket();
        Assertions.assertArrayEquals(p3, remaining(got));
        // the peer closed the connection: no header to read
        Assertions.assertNull(harness.channel.fetchOnePacket());
    }

    @Test
    public void testReadRefusesAFrameOutOfSequence() throws Exception {
        byte[] wire = frame(1, packet(0, "abc".getBytes(StandardCharsets.UTF_8)), false);
        Harness harness = new Harness(wire);
        harness.armAndActivate();
        harness.channel.setSequenceId(0);
        IOException e = Assertions.assertThrows(IOException.class, harness.channel::fetchOnePacket);
        Assertions.assertTrue(e.getMessage().contains("compressed packet sequence"), e.getMessage());
    }

    @Test
    public void testReadRefusesAFrameThatInflatesToAnotherLength() throws Exception {
        byte[] plain = packet(0, textPayload(300));
        byte[] good = frame(0, plain, true);
        // declare one byte less than the stream inflates to
        good[4] = (byte) (plain.length - 1);
        good[5] = (byte) ((plain.length - 1) >> 8);
        Harness harness = new Harness(good);
        harness.armAndActivate();
        harness.channel.setSequenceId(0);
        IOException e = Assertions.assertThrows(IOException.class, harness.channel::fetchOnePacket);
        Assertions.assertTrue(e.getMessage().contains("header declared"), e.getMessage());
    }

    @Test
    public void testCompressedSequenceResetsWithThePacketSequence() throws Exception {
        // a command (frame 0) answered on the same connection: the response frames continue the counter, and
        // the next command boundary resets both counters to 0
        byte[] wire = concat(frame(0, packet(0, new byte[] {3, 'x'}), false),
                frame(0, packet(0, new byte[] {3, 'y'}), false));
        Harness harness = new Harness(wire);
        harness.armAndActivate();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(new byte[] {3, 'x'}, remaining(harness.channel.fetchOnePacket()));
        int before = harness.wireBytes().length;
        harness.channel.sendAndFlush(ByteBuffer.wrap(textPayload(500)));
        List<Frame> response = decodeFrames(harness.wireBytes(), before);
        Assertions.assertEquals(1, response.size());
        Assertions.assertEquals(1, response.get(0).seq, "the response continues the command's compressed sequence");
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(new byte[] {3, 'y'}, remaining(harness.channel.fetchOnePacket()));
    }

    @Test
    public void testPlainChannelIsUntouched() throws Exception {
        Harness harness = new Harness(packet(0, new byte[] {7}));
        harness.channel.setSequenceId(0);
        Assertions.assertFalse(harness.channel.isCompressionActive());
        Assertions.assertArrayEquals(new byte[] {7}, remaining(harness.channel.fetchOnePacket()));
        harness.channel.sendAndFlush(ByteBuffer.wrap(new byte[] {8, 9}));
        Assertions.assertArrayEquals(packet(1, new byte[] {8, 9}), harness.wireBytes());
    }

    private static byte[] remaining(ByteBuffer buffer) {
        Assertions.assertNotNull(buffer);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
