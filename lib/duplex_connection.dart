import 'dart:async';
import 'dart:typed_data';

import 'package:universal_io/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:rxdart/rxdart.dart';

import 'io/bytes.dart';
import 'retry_config.dart';
import 'rsocket.dart';

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

class TcpDuplexConnection extends DuplexConnection {
  Socket socket;
  bool closed = false;
  Timer? _healthCheckTimer;
  DateTime _lastActivity = DateTime.now();
  int _missedHeartbeats = 0;
  final Duration _healthCheckInterval = Duration(seconds: 5);
  final int _maxMissedHeartbeats = 3;

  TcpDuplexConnection(this.socket);

  @override
  void init() {
    _startHealthMonitoring();
    
    socket.listen((data) {
      _lastActivity = DateTime.now();
      _missedHeartbeats = 0;
      updateHealth(ConnectionHealth(
        isHealthy: true,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: 0,
      ));
      receiveHandler!(data);
    }, onDone: () {
      updateHealth(ConnectionHealth(
        isHealthy: false,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: _missedHeartbeats,
        errorMessage: 'Connection closed by remote',
      ));
      close();
    }, onError: (e) {
      updateHealth(ConnectionHealth(
        isHealthy: false,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: _missedHeartbeats,
        errorMessage: e.toString(),
      ));
      close();
    });
  }

  void _startHealthMonitoring() {
    _healthCheckTimer = Timer.periodic(_healthCheckInterval, (timer) {
      if (closed) {
        timer.cancel();
        return;
      }
      
      final now = DateTime.now();
      final timeSinceLastActivity = now.difference(_lastActivity);
      
      if (timeSinceLastActivity > _healthCheckInterval) {
        _missedHeartbeats++;
        
        final isHealthy = _missedHeartbeats < _maxMissedHeartbeats;
        updateHealth(ConnectionHealth(
          isHealthy: isHealthy,
          lastHeartbeat: _lastActivity,
          missedHeartbeats: _missedHeartbeats,
          errorMessage: isHealthy ? null : 'Connection appears to be stale',
        ));
        
        if (!isHealthy) {
          close();
        }
      }
    });
  }

  @override
  void close() {
    if (!closed) {
      closed = true;
      _availability = 0.0;
      _healthCheckTimer?.cancel();
      socket.close();
      updateHealth(ConnectionHealth(
        isHealthy: false,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: _missedHeartbeats,
        errorMessage: 'Connection closed',
      ));
      closeHandler?.call();
      dispose();
    }
  }

  @override
  void write(Uint8List chunk) {
    if (!closed) {
      _lastActivity = DateTime.now();
      socket.add(chunk);
    }
  }
}

class WebSocketDuplexConnection extends DuplexConnection {
  WebSocketChannel webSocket;
  bool closed = true;
  Timer? _healthCheckTimer;
  DateTime _lastActivity = DateTime.now();
  int _missedHeartbeats = 0;
  final Duration _healthCheckInterval = Duration(seconds: 5);
  final int _maxMissedHeartbeats = 3;

  WebSocketDuplexConnection(this.webSocket);

  @override
  void init() {
    closed = false;
    _startHealthMonitoring();

    webSocket.stream.listen((message) {
      _lastActivity = DateTime.now();
      _missedHeartbeats = 0;
      updateHealth(ConnectionHealth(
        isHealthy: true,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: 0,
      ));
      var data = message as List<int>;
      var frameLenBytes = i24ToBytes(data.length);
      receiveHandler!(Uint8List.fromList(frameLenBytes + data));
    }, onDone: () {
      updateHealth(ConnectionHealth(
        isHealthy: false,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: _missedHeartbeats,
        errorMessage: 'WebSocket connection closed by remote',
      ));
      close();
    }, onError: (e) {
      updateHealth(ConnectionHealth(
        isHealthy: false,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: _missedHeartbeats,
        errorMessage: e.toString(),
      ));
      close();
    });
  }

  void _startHealthMonitoring() {
    _healthCheckTimer = Timer.periodic(_healthCheckInterval, (timer) {
      if (closed) {
        timer.cancel();
        return;
      }
      
      final now = DateTime.now();
      final timeSinceLastActivity = now.difference(_lastActivity);
      
      if (timeSinceLastActivity > _healthCheckInterval) {
        _missedHeartbeats++;
        
        final isHealthy = _missedHeartbeats < _maxMissedHeartbeats;
        updateHealth(ConnectionHealth(
          isHealthy: isHealthy,
          lastHeartbeat: _lastActivity,
          missedHeartbeats: _missedHeartbeats,
          errorMessage: isHealthy ? null : 'WebSocket connection appears to be stale',
        ));
        
        if (!isHealthy) {
          close();
        }
      }
    });
  }

  @override
  void close() {
    if (!closed) {
      closed = true;
      _availability = 0.0;
      _healthCheckTimer?.cancel();
      webSocket.sink.close();
      updateHealth(ConnectionHealth(
        isHealthy: false,
        lastHeartbeat: _lastActivity,
        missedHeartbeats: _missedHeartbeats,
        errorMessage: 'WebSocket connection closed',
      ));
      closeHandler?.call();
      dispose();
    }
  }

  @override
  void write(Uint8List chunk) {
    if (!closed) {
      _lastActivity = DateTime.now();
      //remove frame length: 3 bytes
      webSocket.sink.add(chunk.sublist(3));
    }
  }
}

Future<DuplexConnection> connectRSocket(String url, TcpChunkHandler handler) {
  var uri = Uri.parse(url);
  var scheme = uri.scheme;
  if (scheme == 'tcp') {
    var socketFuture = Socket.connect(uri.host, uri.port);
    return socketFuture.then((socket) => TcpDuplexConnection(socket));
  }if (scheme == 'ws' || scheme == 'wss') {
    final websocket = WebSocketChannel.connect(
      Uri.parse(url),
    );
    return Future.value(WebSocketDuplexConnection(websocket));
  } else {
    return Future.error('${scheme} unsupported');
  }
}

