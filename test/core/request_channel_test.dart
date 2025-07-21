import 'dart:async';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/duplex_connection.dart';
import 'package:rsocket/core/rsocket_requester.dart';
import 'package:rsocket/core/rsocket_error.dart';

void main() {
  group('REQUEST_CHANNEL Tests', () {
    test('bidirectional streaming should work', () async {
      // Create a mock duplex connection
      var clientToServer = StreamController<List<int>>();
      var serverToClient = StreamController<List<int>>();

      var clientConnection = MockDuplexConnection(
        clientToServer.stream,
        (data) => serverToClient.add(data),
      );

      var serverConnection = MockDuplexConnection(
        serverToClient.stream,
        (data) => clientToServer.add(data),
      );

      // Setup server with channel handler
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

      // Setup client
      var setupPayload = ConnectionSetupPayload()
        ..keepAliveInterval = 60
        ..keepAliveMaxLifetime = 180
        ..metadataMimeType = 'message/x.rsocket.composite-metadata.v0'
        ..dataMimeType = 'application/json';

      var clientRequester =
          RSocketRequester('requester', setupPayload, clientConnection);
      var serverRequester =
          RSocketRequester('responder', setupPayload, serverConnection);
      serverRequester.responder = serverSocket;

      // Start connections (RSocketRequester constructor already sets up receiveHandler)
      clientRequester.sendSetupPayload();

      // Give time for setup frame exchange
      await Future.delayed(Duration(milliseconds: 50));

      // Create input stream for client
      var inputController = StreamController<Payload>();
      var clientReceived = <String>[];

      // Start channel
      var resultStream =
          clientRequester.requestChannel!(inputController.stream);
      var subscription = resultStream.listen(
        (payload) {
          clientReceived.add(String.fromCharCodes(payload.data!));
        },
        onError: (error) {
          print('Channel error: $error');
        },
      );

      // Send some messages
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
      clientRequester.close();
      serverRequester.close();
    });

    test('channel error handling should work', () async {
      // Create a mock duplex connection
      var clientToServer = StreamController<List<int>>();
      var serverToClient = StreamController<List<int>>();

      var clientConnection = MockDuplexConnection(
        clientToServer.stream,
        (data) => serverToClient.add(data),
      );

      var serverConnection = MockDuplexConnection(
        serverToClient.stream,
        (data) => clientToServer.add(data),
      );

      // Setup server with channel handler that throws error
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

      // Setup client
      var setupPayload = ConnectionSetupPayload()
        ..keepAliveInterval = 60
        ..keepAliveMaxLifetime = 180
        ..metadataMimeType = 'message/x.rsocket.composite-metadata.v0'
        ..dataMimeType = 'application/json';

      var clientRequester =
          RSocketRequester('requester', setupPayload, clientConnection);
      var serverRequester =
          RSocketRequester('responder', setupPayload, serverConnection);
      serverRequester.responder = serverSocket;

      // Start connections (RSocketRequester constructor already sets up receiveHandler)
      clientRequester.sendSetupPayload();

      // Give time for setup frame exchange
      await Future.delayed(Duration(milliseconds: 50));

      // Create input stream for client
      var inputController = StreamController<Payload>();
      var errorReceived = false;

      // Start channel
      var resultStream =
          clientRequester.requestChannel!(inputController.stream);
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

      // Send some messages
      inputController
          .add(Payload()..data = Uint8List.fromList('Hello'.codeUnits));
      await Future.delayed(Duration(milliseconds: 10));

      inputController
          .add(Payload()..data = Uint8List.fromList('ERROR'.codeUnits));
      await Future.delayed(Duration(milliseconds: 50));

      // Verify error was received
      expect(errorReceived, isTrue);

      // Cleanup
      await subscription.cancel();
      clientRequester.close();
      serverRequester.close();
    });
  });
}

// Mock duplex connection for testing
class MockDuplexConnection extends DuplexConnection {
  final Stream<List<int>> _input;
  final void Function(List<int>) _output;
  StreamSubscription? _subscription;

  MockDuplexConnection(this._input, this._output);

  @override
  void init() {
    _subscription?.cancel();
    _subscription = _input.listen((data) {
      if (receiveHandler != null) {
        receiveHandler!(Uint8List.fromList(data));
      }
    });
  }

  @override
  void write(Uint8List frame) {
    _output(frame);
  }

  @override
  void close() {
    _subscription?.cancel();
    dispose();
  }

  Stream dataStream() {
    return _input;
  }
}
