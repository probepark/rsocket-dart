import 'package:rsocket/rsocket.dart';
import 'package:rsocket/rsocket_connector.dart';
import 'package:rsocket/rsocket_server.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/core/rsocket_requester.dart';

void main() async {
  // Start server with lease enabled
  await startServerWithLease();

  // Wait for server to start
  await Future.delayed(Duration(seconds: 1));

  // Start client
  await startClientWithLease();
}

Future<void> startServerWithLease() async {
  print('Starting RSocket server with lease enabled...');

  var acceptor = (ConnectionSetupPayload setup, RSocket sendingSocket) {
    // Cast to access lease methods
    var requester = sendingSocket as RSocketRequester;

    // Server grants additional leases periodically
    Stream.periodic(Duration(seconds: 10), (i) => i).listen((_) {
      print('Server: Granting new lease (100 requests, 30s TTL)');
      requester.sendLease(100, 30000); // 100 requests, 30 second TTL
    });

    return RSocket()
      ..requestResponse = (payload) async {
        var request = payload?.getDataUtf8() ?? '';
        print('Server received: $request');

        return Payload.fromText('', 'Echo: $request');
      }
      ..requestStream = (payload) {
        var request = payload?.getDataUtf8() ?? '';
        print('Server streaming for: $request');

        return Stream.periodic(Duration(seconds: 1), (i) => i)
            .take(5)
            .map((i) => Payload.fromText('', 'Stream item $i for: $request'));
      };
  };

  await RSocketServer.create(acceptor).bind('tcp://localhost:42252');

  print('Server listening on port 42252 with lease enabled');
}

Future<void> startClientWithLease() async {
  print('Starting RSocket client...');

  var connector = RSocketConnector.create()
      .lease() // Enable lease on client
      .acceptor((setup, sendingSocket) {
    // Client can also act as responder if needed
    return RSocket();
  });

  var rsocket = await connector.connect('tcp://localhost:42252');

  print('Client connected with lease support');

  // Try to make requests - they will fail until lease is granted
  for (var i = 0; i < 15; i++) {
    try {
      var response =
          await rsocket.requestResponse!(Payload.fromText('', 'Request #$i'));
      print('Client received: ${response.getDataUtf8()}');
    } catch (e) {
      print('Client request #$i failed: $e');
    }

    await Future.delayed(Duration(seconds: 2));
  }

  // Try streaming
  try {
    print('\nClient: Starting stream request...');
    var stream = rsocket.requestStream!(Payload.fromText('', 'Stream request'));

    await for (var item in stream) {
      print('Client stream received: ${item?.getDataUtf8()}');
    }
  } catch (e) {
    print('Client stream failed: $e');
  }

  rsocket.close();
  print('Client closed');
}
