import 'dart:async';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/core/rsocket_error.dart';

void main() {
  group('REQUEST_CHANNEL Integration Tests', () {
    test('bidirectional streaming should work with real server', () async {
      // Create server
      var serverReceived = <String>[];
      var serverSocket = RSocket()
        ..requestChannel = (Stream<Payload> payloads) {
          return payloads.map((payload) {
            var message = String.fromCharCodes(payload.data!);
            serverReceived.add(message);
            return Payload()
              ..data = Uint8List.fromList('Echo: $message'.codeUnits);
          });
        };

      var server =
          await RSocketServer.create((setup, sendingSocket) => serverSocket)
              .bind('tcp://127.0.0.1:9001');

      // Create client
      var client =
          await RSocketConnector.create().connect('tcp://127.0.0.1:9001');

      var clientReceived = <String>[];
      var inputController = StreamController<Payload>();

      // Start channel
      var resultStream = client.requestChannel!(inputController.stream);
      var subscription = resultStream.listen(
        (payload) {
          clientReceived.add(String.fromCharCodes(payload.data!));
        },
      );

      // Send messages
      inputController
          .add(Payload()..data = Uint8List.fromList('Hello'.codeUnits));
      await Future.delayed(Duration(milliseconds: 50));

      inputController
          .add(Payload()..data = Uint8List.fromList('World'.codeUnits));
      await Future.delayed(Duration(milliseconds: 50));

      inputController
          .add(Payload()..data = Uint8List.fromList('Channel'.codeUnits));
      await Future.delayed(Duration(milliseconds: 50));

      // Close the input stream
      await inputController.close();
      await Future.delayed(Duration(milliseconds: 100));

      // Verify results
      expect(serverReceived, ['Hello', 'World', 'Channel']);
      expect(clientReceived, ['Echo: Hello', 'Echo: World', 'Echo: Channel']);

      // Cleanup
      await subscription.cancel();
      client.close();
      server.close();
    });

    test('channel error handling should work', () async {
      // Create server
      var serverSocket = RSocket()
        ..requestChannel = (Stream<Payload> payloads) {
          return payloads.map((payload) {
            var message = String.fromCharCodes(payload.data!);
            if (message == 'ERROR') {
              throw RSocketException(
                  RSocketErrorCode.APPLICATION_ERROR, 'Test error');
            }
            return Payload()
              ..data = Uint8List.fromList('Echo: $message'.codeUnits);
          });
        };

      var server =
          await RSocketServer.create((setup, sendingSocket) => serverSocket)
              .bind('tcp://127.0.0.1:9002');

      // Create client
      var client =
          await RSocketConnector.create().connect('tcp://127.0.0.1:9002');

      var errorReceived = false;
      var inputController = StreamController<Payload>();

      // Start channel
      var resultStream = client.requestChannel!(inputController.stream);
      var subscription = resultStream.listen(
        (payload) {
          // Should receive first echo
        },
        onError: (error) {
          errorReceived = true;
          expect(error, isA<RSocketException>());
          expect((error as RSocketException).message, 'Test error');
        },
      );

      // Send messages
      inputController
          .add(Payload()..data = Uint8List.fromList('Hello'.codeUnits));
      await Future.delayed(Duration(milliseconds: 50));

      inputController
          .add(Payload()..data = Uint8List.fromList('ERROR'.codeUnits));
      await Future.delayed(Duration(milliseconds: 100));

      // Verify error was received
      expect(errorReceived, isTrue);

      // Cleanup
      await subscription.cancel();
      await inputController.close();
      client.close();
      server.close();
    });
  });
}
