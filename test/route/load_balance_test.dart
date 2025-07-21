import 'package:rsocket/payload.dart';
import 'package:rsocket/route/load_balance.dart';
import 'package:test/test.dart';

void main() {
  group('LoadBalanceRSocket Integration Tests', () {
    test('load-balance with single server', () async {
      // NOTE: This is an integration test that requires a running RSocket server
      // at tcp://127.0.0.1:42252. Run with `just server` to start the test server.

      var rsocket = LoadBalanceRSocket();
      await rsocket.refreshUrl(['tcp://127.0.0.1:42252']);

      // This test will fail if no server is running
      var result = await rsocket.requestResponse!(Payload.fromText('Ping', ''));
      print('Load balance response: ${result.getDataUtf8()}');

      expect(result.getDataUtf8(), isNotEmpty);
    },
        skip:
            'Integration test - requires running server at tcp://127.0.0.1:42252');

    test('LoadBalanceRSocket initialization', () {
      // Unit test that doesn't require external dependencies
      var rsocket = LoadBalanceRSocket();
      expect(rsocket, isNotNull);
      expect(rsocket.availability(), equals(0.0)); // No servers configured yet
    });

    test('refreshUrl accepts server list', () async {
      var rsocket = LoadBalanceRSocket();

      // Should not throw when setting URLs
      expect(
          () => rsocket.refreshUrl(['tcp://localhost:42252']), returnsNormally);
      expect(
          () => rsocket
              .refreshUrl(['tcp://localhost:42252', 'tcp://localhost:42253']),
          returnsNormally);
    });
  });
}
