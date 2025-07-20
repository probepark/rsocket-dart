import 'dart:async';
import 'dart:typed_data';

import 'package:rxdart/rxdart.dart';

import 'core/rsocket_error.dart';
import 'core/rsocket_requester.dart';
import 'duplex_connection.dart';
import 'payload.dart';
import 'retry_config.dart';
import 'rsocket.dart';

class _ReconnectingRSocket extends RSocket {
  RSocket? _activeRSocket;
  final RSocketConnector _connector;
  final String _url;
  
  _ReconnectingRSocket(this._connector, this._url) {
    requestResponse = (payload) async {
      final active = _activeRSocket;
      if (active == null) throw RSocketException(RSocketErrorCode.CONNECTION_ERROR, 'No active connection');
      return active.requestResponse!(payload);
    };
    
    fireAndForget = (payload) async {
      final active = _activeRSocket;
      if (active == null) throw RSocketException(RSocketErrorCode.CONNECTION_ERROR, 'No active connection');
      return active.fireAndForget!(payload);
    };
    
    requestStream = (payload) {
      final active = _activeRSocket;
      if (active == null) return Stream.error(RSocketException(RSocketErrorCode.CONNECTION_ERROR, 'No active connection'));
      return active.requestStream!(payload);
    };
    
    requestChannel = (payloads) {
      final active = _activeRSocket;
      if (active == null) return Stream.error(RSocketException(RSocketErrorCode.CONNECTION_ERROR, 'No active connection'));
      return active.requestChannel!(payloads);
    };
    
    metadataPush = (payload) async {
      final active = _activeRSocket;
      if (active == null) throw RSocketException(RSocketErrorCode.CONNECTION_ERROR, 'No active connection');
      return active.metadataPush!(payload);
    };
  }
  
  void _updateActiveRSocket(RSocket newRSocket) {
    _activeRSocket?.close();
    _activeRSocket = newRSocket;
  }
  
  @override
  void close() {
    _activeRSocket?.close();
    _activeRSocket = null;
  }
  
  @override
  double availability() {
    return _activeRSocket?.availability() ?? 0.0;
  }
}

class RSocketConnector {
  Payload? payload;
  int keepAliveInterval = 20;
  int keepAliveMaxLifeTime = 90;
  String _dataMimeType = 'application/json';
  String _metadataMimeType = 'message/x.rsocket.composite-metadata.v0';
  ErrorConsumer? _errorConsumer;
  SocketAcceptor? _acceptor;
  bool _leaseEnabled = false;
  RetryConfig _retryConfig = RetryConfig.defaultConfig;
  bool _autoReconnect = false;
  String? _lastConnectedUrl;
  _ReconnectingRSocket? _reconnectingProxy;
  
  final BehaviorSubject<ConnectionEvent> _connectionStateController = 
      BehaviorSubject<ConnectionEvent>.seeded(ConnectionEvent(ConnectionState.disconnected));
  final BehaviorSubject<ConnectionHealth> _healthController = 
      BehaviorSubject<ConnectionHealth>.seeded(ConnectionHealth(
        isHealthy: false, 
        lastHeartbeat: DateTime.now()
      ));
  
  Stream<ConnectionEvent> get connectionStateStream => _connectionStateController.stream;
  Stream<ConnectionHealth> get healthStream => _healthController.stream;

  RSocketConnector.create();

  RSocketConnector acceptor(SocketAcceptor socketAcceptor) {
    this._acceptor = socketAcceptor;
    return this;
  }

  RSocketConnector setupPayload(Payload payload) {
    this.payload = payload;
    return this;
  }

  RSocketConnector dataMimeType(String dataMimeType) {
    _dataMimeType = dataMimeType;
    return this;
  }

  RSocketConnector metadataMimeType(String metadataMimeType) {
    _metadataMimeType = metadataMimeType;
    return this;
  }

  // set the keep alive, and unit is second
  RSocketConnector keepAlive(int interval, int maxLifeTime) {
    this.keepAliveInterval = interval;
    this.keepAliveMaxLifeTime = maxLifeTime;
    return this;
  }
  
  // enable lease support
  RSocketConnector lease() {
    _leaseEnabled = true;
    return this;
  }
  
  RSocketConnector retryConfig(RetryConfig config) {
    _retryConfig = config;
    return this;
  }
  
  RSocketConnector autoReconnect([bool enabled = true]) {
    _autoReconnect = enabled;
    return this;
  }

  Future<RSocket> connect(String url) async {
    _lastConnectedUrl = url;
    
    if (_autoReconnect) {
      _reconnectingProxy = _ReconnectingRSocket(this, url);
      final initialConnection = await _connectWithRetry(url);
      _reconnectingProxy!._updateActiveRSocket(initialConnection);
      return _reconnectingProxy!;
    } else {
      return _connectOnce(url);
    }
  }
  
  Future<RSocket> _connectOnce(String url) async {
    _connectionStateController.add(ConnectionEvent(ConnectionState.connecting));
    
    try {
      TcpChunkHandler handler = (Uint8List chunk) {};
      var connectionSetupPayload = ConnectionSetupPayload()
        ..keepAliveInterval = keepAliveInterval
        ..keepAliveMaxLifetime = keepAliveMaxLifeTime
        ..metadataMimeType = _metadataMimeType
        ..dataMimeType = _dataMimeType
        ..data = payload?.data
        ..metadata = payload?.metadata;
        
      final conn = await connectRSocket(url, handler);
      final rsocketRequester = RSocketRequester(
        'requester', 
        connectionSetupPayload, 
        conn, 
        enableLease: _leaseEnabled
      );
      
      if (_acceptor != null) {
        rsocketRequester.responder = _acceptor!(connectionSetupPayload, rsocketRequester);
        if (rsocketRequester.responder == null) {
          rsocketRequester.close();
          _connectionStateController.add(ConnectionEvent(ConnectionState.failed, 
              error: 'Connection refused, please check setup and security!'));
          throw RSocketException(RSocketErrorCode.REJECTED_SETUP, 'Connection refused, please check setup and security!');
        }
      } else {
        rsocketRequester.responder = RSocket();
      }
      
      rsocketRequester.errorConsumer = _errorConsumer;
      
      // Monitor connection health and auto-reconnect if enabled
      rsocketRequester.onConnectionHealthChanged = (health) {
        if (!_healthController.isClosed) {
          _healthController.add(health);
        }
        if (!health.isHealthy && _autoReconnect && _lastConnectedUrl != null) {
          // Connection lost, trigger reconnection
          _triggerReconnection();
        }
      };
      
      // Monitor connection close events
      final originalCloseHandler = rsocketRequester.connection.closeHandler;
      rsocketRequester.connection.closeHandler = () {
        if (_autoReconnect && _lastConnectedUrl != null && !rsocketRequester.closed) {
          _triggerReconnection();
        }
        originalCloseHandler?.call();
      };
      
      rsocketRequester.sendSetupPayload();
      
      _connectionStateController.add(ConnectionEvent(ConnectionState.connected));
      if (!_healthController.isClosed) {
        _healthController.add(ConnectionHealth(isHealthy: true, lastHeartbeat: DateTime.now()));
      }
      
      return rsocketRequester;
    } catch (error) {
      _connectionStateController.add(ConnectionEvent(ConnectionState.failed, error: error));
      rethrow;
    }
  }
  
  Future<RSocket> _connectWithRetry(String url) async {
    int attemptNumber = 0;
    
    while (true) {
      try {
        return await _connectOnce(url);
      } catch (error) {
        if (!_retryConfig.shouldRetry(error, attemptNumber)) {
          rethrow;
        }
        
        _connectionStateController.add(ConnectionEvent(
          ConnectionState.reconnecting, 
          error: error, 
          attemptNumber: attemptNumber + 1
        ));
        
        final delay = _retryConfig.calculateDelay(attemptNumber);
        await Future.delayed(delay);
        
        attemptNumber++;
      }
    }
  }
  
  void _triggerReconnection() {
    if (_lastConnectedUrl != null && _autoReconnect) {
      Timer(Duration(milliseconds: 100), () {
        _connectWithRetry(_lastConnectedUrl!).then((newConnection) {
          _reconnectingProxy?._updateActiveRSocket(newConnection);
        }).catchError((error) {
          _connectionStateController.add(ConnectionEvent(ConnectionState.failed, error: error));
        });
      });
    }
  }
  
  void dispose() {
    _connectionStateController.close();
    _healthController.close();
  }
}
