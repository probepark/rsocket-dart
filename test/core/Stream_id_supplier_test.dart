import 'package:rsocket/core/stream_id_supplier.dart';
import 'package:test/test.dart';

void main() {
  group('StreamIdSupplier Tests', () {
    test('Client supplier generates odd stream IDs', () {
      var streamIds = <int, dynamic>{1: 'demo', 3: 'demo'};
      var streamIdSupplier = StreamIdSupplier.clientSupplier();
      var nextStreamId = streamIdSupplier.nextStreamId(streamIds);
      expect(nextStreamId, equals(5));
      expect(nextStreamId! % 2, equals(1)); // Should be odd for client
    });

    test('Server supplier generates even stream IDs', () {
      var streamIds = <int, dynamic>{2: 'demo', 4: 'demo'};
      var streamIdSupplier = StreamIdSupplier.serverSupplier();
      var nextStreamId = streamIdSupplier.nextStreamId(streamIds);
      expect(nextStreamId, equals(6));
      expect(nextStreamId! % 2, equals(0)); // Should be even for server
    });

    test('Client supplier skips used IDs', () {
      var streamIds = <int, dynamic>{1: 'demo', 3: 'demo', 5: 'demo'};
      var streamIdSupplier = StreamIdSupplier.clientSupplier();
      var nextStreamId = streamIdSupplier.nextStreamId(streamIds);
      expect(nextStreamId, equals(7));
    });

    test('Server supplier skips used IDs', () {
      var streamIds = <int, dynamic>{2: 'demo', 4: 'demo', 6: 'demo'};
      var streamIdSupplier = StreamIdSupplier.serverSupplier();
      var nextStreamId = streamIdSupplier.nextStreamId(streamIds);
      expect(nextStreamId, equals(8));
    });

    test('Empty stream IDs map returns first ID', () {
      var streamIds = <int, dynamic>{};

      var clientSupplier = StreamIdSupplier.clientSupplier();
      var clientId = clientSupplier.nextStreamId(streamIds);
      expect(clientId, equals(1)); // First client ID

      var serverSupplier = StreamIdSupplier.serverSupplier();
      var serverId = serverSupplier.nextStreamId(streamIds);
      expect(serverId, equals(2)); // First server ID
    });
  });
}
