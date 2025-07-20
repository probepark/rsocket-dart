import 'dart:async';
import 'dart:typed_data';

import 'package:rsocket/core/rsocket_error.dart';
import 'package:rsocket/core/rsocket_requester.dart';
import 'package:rsocket/frame/frame.dart';
import 'package:rsocket/io/bytes.dart';
import 'package:test/test.dart';

void main() {
  group('Simple CANCEL Frame Tests', () {
    test('encodeCancelFrame creates valid CANCEL frame', () {
      var streamId = 123;
      var cancelFrame = FrameCodec.encodeCancelFrame(streamId);
      
      // Parse the frame back
      var buffer = RSocketByteBuffer.fromArray(cancelFrame);
      var parsed = parseFrame(buffer);
      
      expect(parsed, isNotNull);
      expect(parsed, isA<CancelFrame>());
      expect(parsed!.header.streamId, equals(streamId));
      expect(parsed.header.type, equals(0x09)); // CANCEL frame type
    });
    
    test('CancelFrame can be parsed from buffer', () {
      var streamId = 456;
      
      // Create a CANCEL frame manually
      var frameBuffer = RSocketByteBuffer();
      frameBuffer.writeI24(6); // frame length (minimal CANCEL frame)
      frameBuffer.writeI32(streamId); // stream id
      frameBuffer.writeI8(0x09 << 2); // CANCEL frame type
      frameBuffer.writeI8(0); // flags
      
      // Parse it
      frameBuffer.resetReaderIndex();
      var parsed = parseFrame(frameBuffer);
      
      expect(parsed, isNotNull);
      expect(parsed, isA<CancelFrame>());
      expect(parsed!.header.streamId, equals(streamId));
      expect(parsed.header.type, equals(0x09));
    });
    
    test('CompleterSubscriber handles cancel error', () async {
      var completer = Completer<Payload>();
      var subscriber = CompleterSubscriber(completer);
      
      // Simulate cancel
      subscriber.onError(RSocketException(
          RSocketErrorCode.CANCELED, 'Request cancelled'));
      
      // Verify the future completes with error
      expect(completer.future, throwsA(isA<RSocketException>()
        .having((e) => e.code, 'code', RSocketErrorCode.CANCELED)
        .having((e) => e.message, 'message', contains('cancelled'))));
    });
    
    test('StreamSubscriber completes stream on cancel', () async {
      var receivedData = <Payload?>[];
      var streamCompleted = false;
      
      var subscriber = StreamSubscriber(onCancel: () {
        // This would send CANCEL frame in real scenario
      });
      
      // Listen to the stream
      subscriber.payloadStream().listen(
        (data) => receivedData.add(data),
        onDone: () => streamCompleted = true,
      );
      
      // Send some data
      subscriber.onNext(Payload()..data = Uint8List.fromList('data1'.codeUnits));
      subscriber.onNext(Payload()..data = Uint8List.fromList('data2'.codeUnits));
      
      // Complete the stream (simulating cancel)
      subscriber.onComplete();
      
      // Wait a bit for async operations
      await Future.delayed(Duration(milliseconds: 10));
      
      expect(receivedData.length, equals(2));
      expect(streamCompleted, isTrue);
    });
    
    test('_StreamSubscription can be cancelled', () {
      var cancelled = false;
      var mockSubscription = MockStreamSubscription(() {
        cancelled = true;
      });
      
      var wrapper = _StreamSubscription(mockSubscription);
      wrapper.cancel();
      
      expect(cancelled, isTrue);
    });
  });
}

// Mock StreamSubscription for testing
class MockStreamSubscription implements StreamSubscription {
  final void Function() onCancel;
  
  MockStreamSubscription(this.onCancel);
  
  @override
  Future<void> cancel() async {
    onCancel();
  }
  
  @override
  void onData(void Function(dynamic)? handleData) {}
  
  @override
  void onError(Function? handleError) {}
  
  @override
  void onDone(void Function()? handleDone) {}
  
  @override
  void pause([Future<void>? resumeSignal]) {}
  
  @override
  void resume() {}
  
  @override
  bool get isPaused => false;
  
  @override
  Future<E> asFuture<E>([E? futureValue]) => Future.value(futureValue);
}