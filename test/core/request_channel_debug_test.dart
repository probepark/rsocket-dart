import 'dart:async';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';

void main() {
  test('REQUEST_CHANNEL debug test', () async {
    print('Starting server...');
    
    // Create server with debug output
    var serverSocket = RSocket()
      ..requestChannel = (Stream<Payload> payloads) {
        print('Server: requestChannel called');
        return payloads.map((payload) {
          var message = String.fromCharCodes(payload.data!);
          print('Server received: $message');
          return Payload()
            ..data = Uint8List.fromList('Echo: $message'.codeUnits);
        });
      };
    
    var server = await RSocketServer.create((setup, sendingSocket) {
      print('Server: acceptor called');
      return serverSocket;
    }).bind('tcp://127.0.0.1:9003');
    
    print('Server bound to port 9003');
    
    // Give server time to start
    await Future.delayed(Duration(milliseconds: 100));
    
    print('Creating client...');
    
    // Create client
    var client = await RSocketConnector.create()
        .connect('tcp://127.0.0.1:9003');
    
    print('Client connected');
    print('Client requestChannel: ${client.requestChannel}');
    
    var inputController = StreamController<Payload>();
    
    // Start channel
    print('Starting channel...');
    var resultStream = client.requestChannel!(inputController.stream);
    
    var results = <String>[];
    var subscription = resultStream.listen(
      (payload) {
        var message = String.fromCharCodes(payload.data!);
        print('Client received: $message');
        results.add(message);
      },
      onError: (error) {
        print('Client error: $error');
      },
      onDone: () {
        print('Client stream done');
      },
    );
    
    // Send a message
    print('Sending message...');
    inputController.add(Payload()..data = Uint8List.fromList('Hello'.codeUnits));
    
    // Wait for response
    await Future.delayed(Duration(milliseconds: 500));
    
    print('Results: $results');
    
    // Close everything
    await inputController.close();
    await subscription.cancel();
    client.close();
    server.close();
    
    // Check if we got any results
    expect(results.isNotEmpty, isTrue, reason: 'Should have received at least one response');
  });
}