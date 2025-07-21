import 'dart:async';
import 'dart:typed_data';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';

void main() async {
  // Setup server acceptor for REQUEST_CHANNEL
  var acceptor = requestChannelAcceptor((Stream<Payload> payloads) {
    print('Server: Channel started');

    // Transform incoming payloads and send responses
    return payloads.map((payload) {
      var message = String.fromCharCodes(payload.data!);
      print('Server received: $message');

      // Echo back with modification
      var response = 'Echo: $message';
      return Payload()..data = Uint8List.fromList(response.codeUnits);
    });
  });

  // Start server
  var server =
      await RSocketServer.create(acceptor).bind('tcp://127.0.0.1:9000');

  print('Server listening on port 9000');

  // Give server time to start
  await Future.delayed(Duration(seconds: 1));

  // Create client
  var client = await RSocketConnector.create().connect('tcp://127.0.0.1:9000');

  print('Client connected');

  // Create input stream for the channel
  var inputController = StreamController<Payload>();

  // Start the channel
  var responseStream = client.requestChannel!(inputController.stream);

  // Listen to responses
  var subscription = responseStream.listen(
    (payload) {
      var response = String.fromCharCodes(payload.data!);
      print('Client received: $response');
    },
    onError: (error) {
      print('Client error: $error');
    },
    onDone: () {
      print('Client: Channel completed');
    },
  );

  // Send some messages
  print('Sending messages...');

  inputController.add(Payload()..data = Uint8List.fromList('Hello'.codeUnits));
  await Future.delayed(Duration(milliseconds: 100));

  inputController.add(Payload()..data = Uint8List.fromList('World'.codeUnits));
  await Future.delayed(Duration(milliseconds: 100));

  inputController
      .add(Payload()..data = Uint8List.fromList('From Channel'.codeUnits));
  await Future.delayed(Duration(milliseconds: 100));

  // Close the input stream to signal completion
  await inputController.close();

  // Wait for responses
  await Future.delayed(Duration(seconds: 1));

  // Cleanup
  await subscription.cancel();
  client.close();
  server.close();

  print('Example completed');
}
