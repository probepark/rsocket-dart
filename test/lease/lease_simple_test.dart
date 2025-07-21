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
        print('Server: Responder lease flag = ${requester.leaseEnabled}');
        print('Server: Connection setup flags = ${setup.flags}');

        if (requester.leaseEnabled) {
          // Grant lease immediately
          Timer.run(() {
            print('Server: Granting lease (10 requests, 60 seconds)');
            requester.sendLease(10, 60000);
          });
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

      print('Client connected, waiting for lease...');

      // Wait for lease
      await Future.delayed(Duration(milliseconds: 200));

      print('Sending request...');
      // Request should succeed
      var response =
          await client!.requestResponse!(Payload.fromText('', 'Test'));
      expect(response.getDataUtf8(), equals('Echo: Test'));
      print('Request successful!');
    });

    test('should enforce lease limits', () async {
      // Start server that grants limited lease
      server = await RSocketServer.create((setup, sendingSocket) {
        var requester = sendingSocket as RSocketRequester;
        print('Server: Lease enabled = ${requester.leaseEnabled}');

        if (requester.leaseEnabled) {
          // Grant lease for only 3 requests
          Timer(Duration(milliseconds: 200), () {
            print('Server: Granting lease (3 requests, 10 seconds)');
            requester.sendLease(3, 10000);
          });
        }

        return RSocket()
          ..requestResponse = (payload) async {
            print('Server: Processing request: ${payload?.getDataUtf8()}');
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      }).bind('tcp://localhost:42272');

      // Connect client with lease
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:42272');

      print('Client connected, waiting for lease...');
      // Wait for lease
      await Future.delayed(Duration(milliseconds: 300));

      // First three requests should succeed
      print('Making request 1...');
      await client!.requestResponse!(Payload.fromText('', 'Request 1'));
      print('Request 1 successful');

      print('Making request 2...');
      await client!.requestResponse!(Payload.fromText('', 'Request 2'));
      print('Request 2 successful');

      print('Making request 3...');
      await client!.requestResponse!(Payload.fromText('', 'Request 3'));
      print('Request 3 successful');

      // Fourth request should fail due to lease exhaustion
      print('Making request 4 (should fail)...');
      expect(
        () => client!.requestResponse!(Payload.fromText('', 'Request 4')),
        throwsA(isA<RSocketException>()
            .having((e) => e.code, 'code', RSocketErrorCode.REJECTED)),
      );
      print('Request 4 correctly rejected due to lease exhaustion');
    });
  });
}
