import 'dart:async';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/core/rsocket_requester.dart';
import 'package:rsocket/core/rsocket_error.dart';

void main() {
  test('Lease mechanism basic functionality', () async {
    // Start server
    var serverStarted = Completer<void>();
    var leaseGranted = Completer<void>();
    
    var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
      var requester = sendingSocket as RSocketRequester;
      
      // Check if client requested lease support
      print('Server: Responder lease flag = ${requester.leaseEnabled}');
      print('Server: Connection setup flags = ${setup.flags}');
      
      // The lease flag should be read from the setup frame, not the connection setup payload
      // Let's grant lease if the requester has lease enabled
      
      // Grant lease after a delay
      if (requester.leaseEnabled) {
        Timer(Duration(milliseconds: 200), () {
          print('Server: Granting lease (5 requests, 10 seconds)');
          requester.sendLease(5, 10000); // 5 requests, 10 second TTL
          leaseGranted.complete();
        });
      }
      
      serverStarted.complete();
      
      return RSocket()
        ..requestResponse = (payload) async {
          print('Server: Received ${payload?.getDataUtf8()}');
          var response = Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          print('Server: Sending response: ${response.getDataUtf8()}');
          return response;
        };
    };
    
    await RSocketServer.create(acceptor).bind('tcp://localhost:42261');
    
    // Connect client with lease
    var client = await RSocketConnector.create()
        .lease()
        .connect('tcp://localhost:42261');
    
    await serverStarted.future;
    var clientRequester = client as RSocketRequester;
    print('Client: Connected with lease support, leaseEnabled=${clientRequester.leaseEnabled}');
    
    // Try request before lease - should fail
    print('Client: Attempting request before lease is granted...');
    try {
      await client.requestResponse!(Payload.fromText('', 'Too early'));
      fail('Request should have failed without lease');
    } catch (e) {
      print('Client: Expected error before lease: $e');
      expect(e, isA<RSocketException>());
      expect((e as RSocketException).message, contains('Lease'));
    }
    
    // Wait for lease with timeout
    await leaseGranted.future.timeout(Duration(seconds: 1), 
        onTimeout: () => throw Exception('Lease was not granted in time'));
    await Future.delayed(Duration(milliseconds: 100)); // Let lease propagate
    
    // Now requests should work
    print('Client: Starting to make requests after lease grant...');
    for (var i = 1; i <= 5; i++) {
      print('Client: Sending request $i');
      var response = await client.requestResponse!(
        Payload.fromText('', 'Request $i')
      );
      print('Client: Got response: ${response.getDataUtf8()}');
      expect(response.getDataUtf8(), equals('Echo: Request $i'));
    }
    print('Client: All 5 requests completed successfully');
    
    // 6th request should fail
    try {
      await client.requestResponse!(Payload.fromText('', 'Request 6'));
      fail('6th request should have failed');
    } catch (e) {
      print('Client: Expected error after lease exhausted: $e');
      expect(e, isA<RSocketException>());
    }
    
    client.close();
  }, timeout: Timeout(Duration(seconds: 10)));
}