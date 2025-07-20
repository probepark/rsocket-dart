import 'dart:typed_data';

import 'package:rsocket/frame/frame.dart';
import 'package:rsocket/io/bytes.dart';
import 'package:test/test.dart';

void main() {
  group('CANCEL Frame Tests', () {
    test('encodeCancelFrame creates valid CANCEL frame', () {
      // Test encoding a CANCEL frame
      var streamId = 123;
      var cancelFrame = FrameCodec.encodeCancelFrame(streamId);
      
      // Verify the encoded frame
      expect(cancelFrame, isNotNull);
      expect(cancelFrame, isA<Uint8List>());
      
      // Parse it back to verify structure
      var buffer = RSocketByteBuffer.fromArray(cancelFrame);
      var header = RSocketHeader.fromBuffer(buffer);
      
      expect(header.streamId, equals(streamId));
      expect(header.type, equals(0x09)); // CANCEL frame type
      expect(header.frameLength, equals(6)); // Minimal CANCEL frame length
    });
    
    test('CancelFrame can be parsed from buffer', () {
      // Create a CANCEL frame using the encoder
      var streamId = 456;
      var encodedFrame = FrameCodec.encodeCancelFrame(streamId);
      
      // Parse it back
      var buffer = RSocketByteBuffer.fromArray(encodedFrame);
      var frame = parseFrame(buffer);
      
      expect(frame, isNotNull);
      expect(frame, isA<CancelFrame>());
      expect(frame!.header.streamId, equals(streamId));
      expect(frame.header.type, equals(0x09));
    });
    
    test('parseFrames handles CANCEL frame correctly', () {
      // Create multiple frames including CANCEL
      var frames = <Uint8List>[];
      
      // Add a CANCEL frame
      frames.add(FrameCodec.encodeCancelFrame(100));
      
      // Add another CANCEL frame
      frames.add(FrameCodec.encodeCancelFrame(200));
      
      // Combine all frames
      var combined = <int>[];
      for (var frame in frames) {
        combined.addAll(frame);
      }
      
      // Parse all frames
      var parsedFrames = parseFrames(combined).toList();
      
      expect(parsedFrames.length, equals(2));
      expect(parsedFrames[0], isA<CancelFrame>());
      expect(parsedFrames[0].header.streamId, equals(100));
      expect(parsedFrames[1], isA<CancelFrame>());
      expect(parsedFrames[1].header.streamId, equals(200));
    });
    
    test('CANCEL frame has correct binary format', () {
      var streamId = 789;
      var cancelFrame = FrameCodec.encodeCancelFrame(streamId);
      
      // Manually verify the binary format
      expect(cancelFrame.length, greaterThanOrEqualTo(9)); // 3 (length) + 4 (streamId) + 1 (type) + 1 (flags)
      
      // Check frame length (first 3 bytes)
      var frameLength = (cancelFrame[0] << 16) | (cancelFrame[1] << 8) | cancelFrame[2];
      expect(frameLength, equals(6)); // CANCEL frame payload is 6 bytes
      
      // Check stream ID (next 4 bytes)
      var parsedStreamId = (cancelFrame[3] << 24) | (cancelFrame[4] << 16) | (cancelFrame[5] << 8) | cancelFrame[6];
      expect(parsedStreamId, equals(streamId));
      
      // Check frame type (next byte, shifted right by 2)
      var frameType = cancelFrame[7] >> 2;
      expect(frameType, equals(0x09)); // CANCEL type
      
      // Check flags (last byte)
      var flags = cancelFrame[8];
      expect(flags, equals(0)); // No flags for CANCEL
    });
  });
}