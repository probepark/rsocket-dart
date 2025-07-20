import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:rsocket/payload.dart';
import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';

void main() async {
  // Start server with flow-controlled stream
  var server = await RSocketServer.bind('tcp://127.0.0.1:7000');

  server.acceptor = (setup, sendingSocket) {
    print('[Server] Client connected with setup: ${setup.getDataUtf8()}');

    return RSocket()
      ..requestStream = (payload) {
        print('[Server] Received stream request: ${payload?.getDataUtf8()}');

        // Create a stream that generates data continuously
        // The server will respect backpressure from REQUEST_N frames
        return _generateInfiniteStream();
      };
  };

  print('[Server] Listening on port 7000...');

  // Connect client with flow control
  var connector = RSocketConnector.create()
    ..setupPayload(Payload.fromText('Client', 'Flow Control Demo'))
    ..keepAlive(20, 90);

  var rsocket = await connector.connect('tcp://127.0.0.1:7000');

  print('[Client] Connected to server');

  // Request stream with small initial request N (demonstrates flow control)
  var streamRequest = Payload.fromText('', 'Start streaming with flow control');
  var stream = rsocket.requestStream!(streamRequest);

  print('[Client] Requesting stream with flow control...');

  // Process stream items with artificial delay to demonstrate backpressure
  var itemCount = 0;
  var subscription = stream.listen(
    (payload) async {
      itemCount++;
      var data = payload?.getDataUtf8();
      print('[Client] Received item #$itemCount: $data');

      // Simulate slow processing
      await Future.delayed(Duration(milliseconds: 500));
    },
    onDone: () {
      print('[Client] Stream completed after $itemCount items');
    },
    onError: (error) {
      print('[Client] Stream error: $error');
    },
  );

  // Let it run for a while
  await Future.delayed(Duration(seconds: 10));

  print('[Client] Cancelling stream...');
  await subscription.cancel();

  // Cleanup
  rsocket.close();
  server.close();

  print('Flow control example completed!');
}

// Generate an infinite stream of data
Stream<Payload?> _generateInfiniteStream() async* {
  var counter = 0;
  while (true) {
    counter++;
    var data = 'Item #$counter - Generated at ${DateTime.now()}';
    yield Payload()..data = Uint8List.fromList(utf8.encode(data));

    // Small delay to make output readable
    await Future.delayed(Duration(milliseconds: 100));
  }
}

