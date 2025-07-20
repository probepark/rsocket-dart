import 'package:rsocket/shelf.dart';

void main() async {
  const listenUrl = 'tcp://0.0.0.0:42252';
  await RSocketServer.create(requestResponseAcceptor((payload) {
    final dataText = payload?.getDataUtf8() ?? 'null';
    return Future.value(
        Payload.fromText('text/plain', 'Hello ' + dataText));
  })).bind(listenUrl);
  print('RSocket Server started on ${listenUrl}');
}
