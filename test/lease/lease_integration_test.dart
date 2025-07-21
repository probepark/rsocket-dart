import 'dart:async';
import 'package:test/test.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/core/rsocket_requester.dart';
import 'package:rsocket/core/rsocket_error.dart';

void main() {
  group('Lease Integration Tests', () {
    RSocket? server;
    RSocket? client;
    var port = 42253;

    setUp(() {
      // Use a different port for each test to avoid binding issues
      port++;
    });

    tearDown(() async {
      client?.close();
      server?.close();
      await Future.delayed(Duration(milliseconds: 200));
    });

    test('should reject requests when lease is not granted', () async {
      // Start server without granting lease
      var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
        server = sendingSocket;
        // Don't grant lease immediately
        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      };

      await RSocketServer.create(acceptor).bind('tcp://localhost:$port');

      // Connect client with lease enabled
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:$port');

      // Wait a bit for connection setup
      await Future.delayed(Duration(milliseconds: 100));

      // Try to make request - should fail
      expect(
        () => client!.requestResponse!(Payload.fromText('', 'Hello')),
        throwsA(isA<RSocketException>().having(
          (e) => e.message,
          'message',
          contains('Lease exhausted or expired'),
        )),
      );
    });

    test('should allow requests after lease is granted', () async {
      var completer = Completer<void>();

      // Start server that grants lease after connection
      var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
        server = sendingSocket;
        var requester = sendingSocket as RSocketRequester;

        // Grant lease after setup
        Timer(Duration(milliseconds: 100), () {
          requester.sendLease(10, 30000); // 10 requests, 30 second TTL
          completer.complete();
        });

        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      };

      await RSocketServer.create(acceptor).bind('tcp://localhost:$port');

      // Connect client with lease enabled
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:$port');

      // Wait for lease to be granted
      await completer.future;
      await Future.delayed(Duration(milliseconds: 50));

      // Now request should succeed
      var response = await client!
          .requestResponse!(Payload.fromText('', 'Hello with lease'));

      expect(response.getDataUtf8(), equals('Echo: Hello with lease'));
    });

    test('should enforce lease limits', () async {
      var leaseGranted = Completer<void>();

      // Start server that grants limited lease
      var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
        server = sendingSocket;
        var requester = sendingSocket as RSocketRequester;

        // Grant lease with only 3 requests
        Timer(Duration(milliseconds: 100), () {
          requester.sendLease(3, 30000); // 3 requests, 30 second TTL
          leaseGranted.complete();
        });

        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      };

      await RSocketServer.create(acceptor).bind('tcp://localhost:$port');

      // Connect client with lease enabled
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:$port');

      // Wait for lease to be granted
      await leaseGranted.future;
      await Future.delayed(Duration(milliseconds: 50));

      // Make 3 requests - should succeed
      for (var i = 1; i <= 3; i++) {
        var response =
            await client!.requestResponse!(Payload.fromText('', 'Request $i'));
        expect(response.getDataUtf8(), equals('Echo: Request $i'));
      }

      // 4th request should fail
      expect(
        () => client!.requestResponse!(Payload.fromText('', 'Request 4')),
        throwsA(isA<RSocketException>()),
      );
    });

    test('should handle lease expiration', () async {
      var leaseGranted = Completer<void>();

      // Start server that grants short-lived lease
      var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
        server = sendingSocket;
        var requester = sendingSocket as RSocketRequester;

        // Grant lease with very short TTL
        Timer(Duration(milliseconds: 100), () {
          requester.sendLease(10, 200); // 10 requests, 200ms TTL
          leaseGranted.complete();
        });

        return RSocket()
          ..requestResponse = (payload) async {
            return Payload.fromText('', 'Echo: ${payload?.getDataUtf8()}');
          };
      };

      await RSocketServer.create(acceptor).bind('tcp://localhost:$port');

      // Connect client with lease enabled
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:$port');

      // Wait for lease to be granted
      await leaseGranted.future;
      await Future.delayed(Duration(milliseconds: 50));

      // First request should succeed
      var response =
          await client!.requestResponse!(Payload.fromText('', 'Before expiry'));
      expect(response.getDataUtf8(), equals('Echo: Before expiry'));

      // Wait for lease to expire
      await Future.delayed(Duration(milliseconds: 200));

      // Request after expiry should fail
      expect(
        () => client!.requestResponse!(Payload.fromText('', 'After expiry')),
        throwsA(isA<RSocketException>()),
      );
    });

    test('should work with streaming', () async {
      var leaseGranted = Completer<void>();

      // Start server with streaming support
      var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
        server = sendingSocket;
        var requester = sendingSocket as RSocketRequester;

        // Grant lease
        Timer(Duration(milliseconds: 100), () {
          requester.sendLease(5, 30000);
          leaseGranted.complete();
        });

        return RSocket()
          ..requestStream = (payload) {
            return Stream.fromIterable([1, 2, 3])
                .map((i) => Payload.fromText('', 'Item $i'));
          };
      };

      await RSocketServer.create(acceptor).bind('tcp://localhost:$port');

      // Connect client with lease enabled
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:$port');

      // Wait for lease to be granted
      await leaseGranted.future;
      await Future.delayed(Duration(milliseconds: 50));

      // Stream request should consume one lease permit
      var items = <String>[];
      await for (var item
          in client!.requestStream!(Payload.fromText('', 'Stream'))) {
        items.add(item?.getDataUtf8() ?? '');
      }

      expect(items, equals(['Item 1', 'Item 2', 'Item 3']));
    });

    test('should work with fire-and-forget', () async {
      var leaseGranted = Completer<void>();
      var received = <String>[];

      // Start server
      var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
        server = sendingSocket;
        var requester = sendingSocket as RSocketRequester;

        // Grant lease
        Timer(Duration(milliseconds: 100), () {
          requester.sendLease(2, 30000);
          leaseGranted.complete();
        });

        return RSocket()
          ..fireAndForget = (payload) async {
            received.add(payload?.getDataUtf8() ?? '');
          };
      };

      await RSocketServer.create(acceptor).bind('tcp://localhost:$port');

      // Connect client with lease enabled
      client = await RSocketConnector.create()
          .lease()
          .connect('tcp://localhost:$port');

      // Wait for lease to be granted
      await leaseGranted.future;
      await Future.delayed(Duration(milliseconds: 50));

      // Fire and forget should consume lease
      await client!.fireAndForget!(Payload.fromText('', 'Message 1'));
      await client!.fireAndForget!(Payload.fromText('', 'Message 2'));

      await Future.delayed(Duration(milliseconds: 100));
      expect(received, equals(['Message 1', 'Message 2']));

      // Third should fail
      expect(
        () => client!.fireAndForget!(Payload.fromText('', 'Message 3')),
        throwsA(isA<RSocketException>()),
      );
    });
  });
}
