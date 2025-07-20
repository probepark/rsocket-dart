import 'dart:async';
import 'dart:typed_data';

import 'package:universal_io/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:rxdart/rxdart.dart';

import 'io/bytes.dart';
import 'retry_config.dart';
import 'rsocket.dart';
import 'health_monitoring_mixin.dart';
import 'logging.dart';

abstract class DuplexConnection implements Closeable, Availability {
  double _availability = 1.0;
  TcpChunkHandler? receiveHandler;
  CloseHandler? closeHandler;
  ConnectionHealthHandler? healthHandler;
  
  final BehaviorSubject<ConnectionHealth> _healthController = 
      BehaviorSubject<ConnectionHealth>.seeded(ConnectionHealth(
        isHealthy: true, 
        lastHeartbeat: DateTime.now()
      ));
  
  Stream<ConnectionHealth> get healthStream => _healthController.stream;
  ConnectionHealth get currentHealth => _healthController.value;

  void init();

  void write(Uint8List chunk);
  
  void updateHealth(ConnectionHealth health) {
    if (!_healthController.isClosed) {
      _healthController.add(health);
      healthHandler?.call(health);
    }
  }

  @override
  double availability() {
    return _availability;
  }
  
  void dispose() {
    _healthController.close();
  }
}

typedef TcpChunkHandler = void Function(Uint8List chunk);
typedef CloseHandler = void Function();
typedef ConnectionHealthHandler = void Function(ConnectionHealth health);

class TcpDuplexConnection extends DuplexConnection with HealthMonitoringMixin {
  Socket socket;
  bool closed = false;

  TcpDuplexConnection(this.socket, {
    Duration healthCheckInterval = const Duration(seconds: 5),
    int maxMissedHeartbeats = 3,
  }) {
    initializeHealthMonitoring(
      healthCheckInterval: healthCheckInterval,
      maxMissedHeartbeats: maxMissedHeartbeats,
    );
  }

  @override
  void init() {
    startHealthMonitoring(connectionType: 'TCP');
    
    socket.listen((data) {
      recordActivity();
      receiveHandler!(data);
    }, onDone: () {
      recordConnectionClosed('Connection closed by remote');
      close();
    }, onError: (e) {
      RSocketLogger.error('TCP Socket error', e);
      recordConnectionError(e.toString());
      close();
    });
  }

  @override
  void close() {
    if (!closed) {
      closed = true;
      _availability = 0.0;
      stopHealthMonitoring();
      socket.close();
      recordFinalHealth('Connection closed');
      closeHandler?.call();
      dispose();
    }
  }

  @override
  void write(Uint8List chunk) {
    if (!closed) {
      recordActivity();
      socket.add(chunk);
    }
  }
}

class WebSocketDuplexConnection extends DuplexConnection with HealthMonitoringMixin {
  WebSocketChannel webSocket;
  bool closed = true;

  WebSocketDuplexConnection(this.webSocket, {
    Duration healthCheckInterval = const Duration(seconds: 5),
    int maxMissedHeartbeats = 3,
  }) {
    initializeHealthMonitoring(
      healthCheckInterval: healthCheckInterval,
      maxMissedHeartbeats: maxMissedHeartbeats,
    );
  }

  @override
  void init() {
    closed = false;
    startHealthMonitoring(connectionType: 'WebSocket');

    webSocket.stream.listen((message) {
      recordActivity();
      var data = message as List<int>;
      var frameLenBytes = i24ToBytes(data.length);
      receiveHandler!(Uint8List.fromList(frameLenBytes + data));
    }, onDone: () {
      recordConnectionClosed('WebSocket connection closed by remote');
      close();
    }, onError: (e) {
      RSocketLogger.error('WebSocket error', e);
      recordConnectionError(e.toString());
      close();
    });
  }

  @override
  void close() {
    if (!closed) {
      closed = true;
      _availability = 0.0;
      stopHealthMonitoring();
      webSocket.sink.close();
      recordFinalHealth('WebSocket connection closed');
      closeHandler?.call();
      dispose();
    }
  }

  @override
  void write(Uint8List chunk) {
    if (!closed) {
      recordActivity();
      //remove frame length: 3 bytes
      webSocket.sink.add(chunk.sublist(3));
    }
  }
}

Future<DuplexConnection> connectRSocket(
  String url, 
  TcpChunkHandler handler, {
  Duration healthCheckInterval = const Duration(seconds: 5),
  int maxMissedHeartbeats = 3,
}) {
  var uri = Uri.parse(url);
  var scheme = uri.scheme;
  if (scheme == 'tcp') {
    var socketFuture = Socket.connect(uri.host, uri.port);
    return socketFuture.then((socket) => TcpDuplexConnection(
      socket,
      healthCheckInterval: healthCheckInterval,
      maxMissedHeartbeats: maxMissedHeartbeats,
    ));
  }if (scheme == 'ws' || scheme == 'wss') {
    final websocket = WebSocketChannel.connect(
      Uri.parse(url),
    );
    return Future.value(WebSocketDuplexConnection(
      websocket,
      healthCheckInterval: healthCheckInterval,
      maxMissedHeartbeats: maxMissedHeartbeats,
    ));
  } else {
    return Future.error('${scheme} unsupported');
  }
}

