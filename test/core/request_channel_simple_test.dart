import 'dart:async';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/payload.dart';

void main() {
  test('REQUEST_CHANNEL basic implementation test', () async {
    // Test that requestChannel is properly initialized
    var rsocket = RSocket();
    
    // The default implementation should return an error stream
    var errorStream = rsocket.requestChannel!(Stream.empty());
    expect(
      errorStream,
      emitsError(isA<Exception>()),
    );
    
    // Create an RSocket with requestChannel handler
    var channelHandler = (Stream<Payload> payloads) {
      return payloads.map((payload) {
        var message = String.fromCharCodes(payload.data!);
        return Payload()
          ..data = Uint8List.fromList('Echo: $message'.codeUnits);
      });
    };
    
    var rsocketWithChannel = RSocket.requestChannel(channelHandler);
    
    // Test that requestChannel is set
    expect(rsocketWithChannel.requestChannel, isNotNull);
    
    // Test the channel functionality
    var inputController = StreamController<Payload>();
    var resultStream = rsocketWithChannel.requestChannel!(inputController.stream);
    
    var results = <String>[];
    resultStream.listen((payload) {
      results.add(String.fromCharCodes(payload.data!));
    });
    
    // Send some data
    inputController.add(Payload()..data = Uint8List.fromList('Hello'.codeUnits));
    inputController.add(Payload()..data = Uint8List.fromList('World'.codeUnits));
    await inputController.close();
    
    // Give time for processing
    await Future.delayed(Duration(milliseconds: 100));
    
    // Verify results
    expect(results, ['Echo: Hello', 'Echo: World']);
  });
}