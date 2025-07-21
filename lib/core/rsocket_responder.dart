import 'package:universal_io/io.dart';
import 'package:web_socket_channel/io.dart';

import '../core/rsocket_requester.dart';
import '../duplex_connection.dart';
import '../frame/frame.dart';
import '../frame/frame_types.dart' as frame_types;
import '../logging.dart';
import '../payload.dart';
import '../rsocket.dart';

class BaseResponder {
  late SocketAcceptor socketAcceptor;
  late Uri uri;

  Future<void> receiveConnection(DuplexConnection duplexConn) async {
    RSocketRequester? rsocketRequester;
    duplexConn.receiveHandler = (chunk) {
      for (var frame in parseFrames(chunk)) {
        var header = frame.header;
        if (header.type == frame_types.SETUP) {
          var setupFrame = frame as SetupFrame;
          var connectionSetupPayload = ConnectionSetupPayload()
            ..keepAliveInterval = setupFrame.keepAliveInterval
            ..keepAliveMaxLifetime = setupFrame.keepAliveMaxLifetime
            ..metadataMimeType = setupFrame.metadataMimeType
            ..dataMimeType = setupFrame.dataMimeType
            ..data = setupFrame.payload?.data
            ..metadata = setupFrame.payload?.data;
          var temp = RSocketRequester(
              'responder', connectionSetupPayload, duplexConn,
              enableLease: setupFrame.leaseEnable);
          var responder = socketAcceptor(connectionSetupPayload, temp);
          if (responder == null) {
            duplexConn.close();
            break;
          } else {
            temp.responder = responder;
            rsocketRequester = temp;
            // Initial lease grant is handled by the acceptor or server configuration
            // Do not automatically grant lease on connection
          }
        } else {
          rsocketRequester?.receiveFrame(frame);
        }
      }
    };
    duplexConn.init();
  }
}

class TcpRSocketResponder extends BaseResponder implements Closeable {
  late ServerSocket serverSocket;
  final Duration healthCheckInterval;
  final int maxMissedHeartbeats;

  TcpRSocketResponder(
    Uri uri,
    ServerSocket serverSocket,
    SocketAcceptor socketAcceptor, {
    this.healthCheckInterval = const Duration(seconds: 5),
    this.maxMissedHeartbeats = 3,
  }) {
    this.uri = uri;
    this.socketAcceptor = socketAcceptor;
    this.serverSocket = serverSocket;
  }

  void accept() {
    serverSocket.listen((socket) {
      receiveConnection(TcpDuplexConnection(
        socket,
        healthCheckInterval: healthCheckInterval,
        maxMissedHeartbeats: maxMissedHeartbeats,
      )).then((value) => {});
    });
  }

  @override
  void close() {
    serverSocket.close();
  }
}

class WebSocketRSocketResponder extends BaseResponder implements Closeable {
  late HttpServer httpServer;
  final Duration healthCheckInterval;
  final int maxMissedHeartbeats;

  WebSocketRSocketResponder(
    Uri uri,
    HttpServer httpServer,
    SocketAcceptor socketAcceptor, {
    this.healthCheckInterval = const Duration(seconds: 5),
    this.maxMissedHeartbeats = 3,
  }) {
    this.uri = uri;
    this.socketAcceptor = socketAcceptor;
    this.httpServer = httpServer;
  }

  void accept() {
    httpServer.listen((HttpRequest req) async {
      if (req.uri.path == uri.path) {
        try {
          final webSocket = await WebSocketTransformer.upgrade(req);
          await receiveConnection(WebSocketDuplexConnection(
            IOWebSocketChannel(webSocket),
            healthCheckInterval: healthCheckInterval,
            maxMissedHeartbeats: maxMissedHeartbeats,
          ));
        } catch (e) {
          RSocketLogger.error('Error handling WebSocket connection', e);
        }
      }
    });
  }

  @override
  void close() {
    httpServer.close();
  }
}
