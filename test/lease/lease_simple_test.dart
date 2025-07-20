import 'dart:async';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/core/rsocket_requester.dart';
import 'package:rsocket/core/rsocket_error.dart';

void main() {
  group('Lease Basic Tests', () {
    Closeable? server;
    RSocket? client;
    
    tearDown(() async {
      client?.close();
      server?.close();
      await Future.delayed(Duration(milliseconds: 100));
    });
    test('should reject requests without lease', () async {
      // Start server that doesn't grant lease
      server = await RSocketServer.create((setup, sendingSocket) {
        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      }).bind('tcp://localhost:42270');
      
      // Connect client with lease
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:42270');
      
      // Request should fail
      expect(
        () => client!.requestResponse!(Payload.fromText('', 'Test')),
        throwsA(isA<RSocketException>()),
      );
    });

    test('should allow requests with lease', () async {
      // Start server that grants lease immediately
      server = await RSocketServer.create((setup, sendingSocket) {
        var requester = sendingSocket as RSocketRequester;
        if (requester.leaseEnabled) {
          // Grant lease immediately
          Timer.run(() => requester.sendLease(10, 60000));
        }
        
        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      }).bind('tcp://localhost:42271');
      
      // Connect client with lease
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:42271');
      
      // Wait for lease
      await Future.delayed(Duration(milliseconds: 100));
      
      // Request should succeed
      var response = await client!.requestResponse!(
        Payload.fromText('', 'Test')
      );
      expect(response.getDataUtf8(), equals('Echo: Test'));
    });

    test('should enforce lease limits', () async {
      // Start server that grants limited lease
      server = await RSocketServer.create((setup, sendingSocket) {
        var requester = sendingSocket as RSocketRequester;
        if (requester.leaseEnabled) {
          // Grant lease for only 2 requests
          Timer.run(() => requester.sendLease(2, 60000));
        }
        
        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      }).bind('tcp://localhost:42272');
      
      // Connect client with lease
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:42272');
      
      // Wait for lease
      await Future.delayed(Duration(milliseconds: 100));
      
      // First two requests should succeed
      await client!.requestResponse!(Payload.fromText('', 'Request 1'));
      await client!.requestResponse!(Payload.fromText('', 'Request 2'));
      
      // Third request should fail
      expect(
        () => client!.requestResponse!(Payload.fromText('', 'Request 3')),
        throwsA(isA<RSocketException>()),
      );
    });
  });
}