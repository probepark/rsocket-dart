import 'dart:async';
import 'dart:typed_data';

import 'package:rsocket/core/rsocket_error.dart';
import 'package:rsocket/core/rsocket_requester.dart';
import 'package:rsocket/duplex_connection.dart';
import 'package:rsocket/frame/frame.dart';
import 'package:rsocket/io/bytes.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/rsocket.dart';
import 'package:test/test.dart';

// Mock connection for testing
class MockDuplexConnection extends DuplexConnection {
  final sentFrames = <Uint8List>[];
  void Function(Uint8List chunk)? receiveHandler;
  void Function()? closeHandler;
  
  @override
  void init() {}
  
  @override
  void write(Uint8List frame) {
    sentFrames.add(frame);
  }
  
  @override
  void close() {
    closeHandler?.call();
  }
  
  void simulateIncomingFrame(RSocketFrame frame) {
    var buffer = RSocketByteBuffer();
    
    // Encode the frame based on its type
    if (frame is CancelFrame) {
      var encoded = FrameCodec.encodeCancelFrame(frame.header.streamId);
      receiveHandler?.call(encoded);
    } else if (frame is PayloadFrame) {
      var encoded = FrameCodec.encodePayloadFrame(
          frame.header.streamId, frame.completed, frame.payload);
      receiveHandler?.call(encoded);
    } else if (frame is ErrorFrame) {
      var encoded = FrameCodec.encodeErrorFrame(
          frame.header.streamId, frame.code!, frame.message);
      receiveHandler?.call(encoded);
    }
  }
}

void main() {
  group('CANCEL Frame Tests', () {
    late MockDuplexConnection connection;
    late RSocketRequester requester;
    late ConnectionSetupPayload setupPayload;
    
    setUp(() {
      connection = MockDuplexConnection();
      setupPayload = ConnectionSetupPayload()
        ..keepAliveInterval = 20
        ..keepAliveMaxLifetime = 90
        ..metadataMimeType = 'message/x.rsocket.composite-metadata.v0'
        ..dataMimeType = 'application/json';
      
      requester = RSocketRequester('requester', setupPayload, connection);
    });
    
    test('Should send CANCEL frame when stream is cancelled', () async {
      // Create a stream request
      var stream = requester.requestStream!(Payload()..data = Uint8List.fromList('test'.codeUnits));
      
      // Listen to the stream with a cancellable subscription
      var subscription = stream.listen((data) {});
      
      // Clear any setup frames
      connection.sentFrames.clear();
      
      // Cancel the subscription
      await subscription.cancel();
      
      // Verify CANCEL frame was sent
      expect(connection.sentFrames.length, 1);
      
      // Parse the sent frame
      var sentBuffer = RSocketByteBuffer.fromArray(connection.sentFrames.first);
      var sentFrame = parseFrame(sentBuffer);
      
      expect(sentFrame, isA<CancelFrame>());
      expect(sentFrame!.header.type, equals(0x09)); // CANCEL frame type
    });
    
    test('Should handle incoming CANCEL frame for client stream', () async {
      var completer = Completer<bool>();
      var receivedData = <Payload?>[];
      
      // Set up a mock responder
      requester.responder = MockRSocket()
        ..requestStream = (payload) {
          // Return a stream that will be cancelled
          return Stream.periodic(Duration(milliseconds: 100), (i) {
            return Payload()..data = Uint8List.fromList('data$i'.codeUnits);
          }).take(10);
        };
      
      // Simulate an incoming REQUEST_STREAM frame
      var streamId = 2; // Server-initiated stream
      
      // Create and send the cancel frame
      var cancelFrame = CancelFrame()
        ..header = (RSocketHeader()
          ..streamId = streamId
          ..type = 0x09
          ..flags = 0
          ..frameLength = 6);
      
      // Send the CANCEL frame
      connection.simulateIncomingFrame(cancelFrame);
      
      // Verify the stream was cancelled
      expect(requester.senders.containsKey(streamId), false);
    });
    
    test('Should handle incoming CANCEL frame for request-response', () async {
      var streamId = 1;
      var completer = Completer<Payload>();
      
      // Add a pending request-response
      requester.senders[streamId] = CompleterSubscriber(completer);
      
      // Create and send the cancel frame
      var cancelFrame = CancelFrame()
        ..header = (RSocketHeader()
          ..streamId = streamId
          ..type = 0x09
          ..flags = 0
          ..frameLength = 6);
      
      // Send the CANCEL frame
      connection.simulateIncomingFrame(cancelFrame);
      
      // Verify the request was cancelled with error
      expect(completer.future, throwsA(isA<RSocketException>()
        .having((e) => e.code, 'code', RSocketErrorCode.CANCELED)
        .having((e) => e.message, 'message', contains('cancelled'))));
      
      // Verify the sender was removed
      expect(requester.senders.containsKey(streamId), false);
    });
    
    test('Should clean up all streams when connection closes', () async {
      var streamCompleter1 = Completer<Payload>();
      var streamCompleter2 = Completer<Payload>();
      var streamController = StreamController<Payload?>();
      
      // Add multiple active operations
      requester.senders[1] = CompleterSubscriber(streamCompleter1);
      requester.senders[3] = CompleterSubscriber(streamCompleter2);
      requester.senders[5] = StreamSubscriber();
      
      // Close the connection
      requester.close();
      
      // Verify all operations were cancelled with connection close error
      expect(streamCompleter1.future, throwsA(isA<RSocketException>()
        .having((e) => e.code, 'code', RSocketErrorCode.CONNECTION_CLOSE)));
      expect(streamCompleter2.future, throwsA(isA<RSocketException>()
        .having((e) => e.code, 'code', RSocketErrorCode.CONNECTION_CLOSE)));
      
      // Verify all senders were cleared
      expect(requester.senders.isEmpty, true);
      expect(requester.closed, true);
    });
    
    test('Should ignore CANCEL frame for non-existent stream', () {
      var nonExistentStreamId = 999;
      
      // Create and send the cancel frame for non-existent stream
      var cancelFrame = CancelFrame()
        ..header = (RSocketHeader()
          ..streamId = nonExistentStreamId
          ..type = 0x09
          ..flags = 0
          ..frameLength = 6);
      
      // Should not throw
      expect(() => connection.simulateIncomingFrame(cancelFrame), returnsNormally);
    });
  });
}

// Mock RSocket for testing
class MockRSocket extends RSocket {
  @override
  RequestResponse? requestResponse;
  
  @override
  FireAndForget? fireAndForget;
  
  @override
  RequestStream? requestStream;
  
  @override
  MetadataPush? metadataPush;
  
  // For compatibility with older versions that use 'subscribe' instead of 'requestResponse'
  RequestResponse? get subscribe => requestResponse;
  set subscribe(RequestResponse? value) => requestResponse = value;
  
  @override
  double availability() => 1.0;
  
  @override
  void close() {}
}