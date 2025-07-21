import 'package:rsocket/frame/frame.dart';
import 'package:rsocket/frame/frame_types.dart' as frame_types;
import 'package:rsocket/io/bytes.dart';
import 'package:rsocket/core/stream_demand_tracker.dart';
import 'package:test/test.dart';

void main() {
  group('REQUEST_N Frame Tests', () {
    test('encodeRequestNFrame creates valid frame', () {
      var streamId = 42;
      var n = 100;

      var frameBytes = FrameCodec.encodeRequestNFrame(streamId, n);
      var buffer = RSocketByteBuffer.fromArray(frameBytes);

      // Parse the frame
      var header = RSocketHeader.fromBuffer(buffer);

      expect(header.streamId, equals(streamId));
      expect(header.type, equals(frame_types.REQUEST_N));

      // Read the request N value
      var requestN = buffer.readI32();
      expect(requestN, equals(n));
    });

    test('RequestNFrame parses correctly', () {
      var streamId = 42;
      var n = 100;

      var frameBytes = FrameCodec.encodeRequestNFrame(streamId, n);
      var buffer = RSocketByteBuffer.fromArray(frameBytes);

      var frame = parseFrame(buffer);

      expect(frame, isA<RequestNFrame>());
      var requestNFrame = frame as RequestNFrame;
      expect(requestNFrame.header.streamId, equals(streamId));
      expect(requestNFrame.initialRequestN, equals(n));
    });
  });

  group('StreamDemandTracker Tests', () {
    late StreamDemandTracker tracker;

    setUp(() {
      tracker = StreamDemandTracker();
    });

    test('initial demand is zero', () {
      expect(tracker.getDemand(1), equals(0));
      expect(tracker.hasDemand(1), isFalse);
    });

    test('addDemand increases demand', () {
      tracker.addDemand(1, 10);
      expect(tracker.getDemand(1), equals(10));
      expect(tracker.hasDemand(1), isTrue);

      tracker.addDemand(1, 5);
      expect(tracker.getDemand(1), equals(15));
    });

    test('consumeDemand decreases demand', () {
      tracker.addDemand(1, 10);

      expect(tracker.consumeDemand(1), isTrue);
      expect(tracker.getDemand(1), equals(9));

      // Consume all demand
      for (var i = 0; i < 9; i++) {
        tracker.consumeDemand(1);
      }

      expect(tracker.getDemand(1), equals(0));
      expect(tracker.consumeDemand(1), isFalse);
    });

    test('addDemand prevents overflow', () {
      tracker.addDemand(1, 0x7FFFFFFF - 10);
      tracker.addDemand(1, 20);

      expect(tracker.getDemand(1), equals(0x7FFFFFFF));
    });

    test('removeStream clears demand', () {
      tracker.addDemand(1, 10);
      tracker.addDemand(2, 20);

      tracker.removeStream(1);

      expect(tracker.getDemand(1), equals(0));
      expect(tracker.getDemand(2), equals(20));
    });

    test('clear removes all demand', () {
      tracker.addDemand(1, 10);
      tracker.addDemand(2, 20);

      tracker.clear();

      expect(tracker.getDemand(1), equals(0));
      expect(tracker.getDemand(2), equals(0));
    });
  });

  group('Flow Control Integration Tests', () {
    test('REQUEST_N frame increases demand', () async {
      // This would be an integration test with actual RSocketRequester
      // For now, we just test the concept

      var tracker = StreamDemandTracker();
      var streamId = 1;

      // Simulate initial request
      tracker.addDemand(streamId, 5);

      // Simulate consuming demand
      for (var i = 0; i < 3; i++) {
        expect(tracker.consumeDemand(streamId), isTrue);
      }

      expect(tracker.getDemand(streamId), equals(2));

      // Simulate REQUEST_N frame
      tracker.addDemand(streamId, 10);
      expect(tracker.getDemand(streamId), equals(12));
    });
  });
}
