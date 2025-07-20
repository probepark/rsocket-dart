import 'dart:async';

import 'retry_config.dart';

mixin HealthMonitoringMixin {
  Timer? _healthCheckTimer;
  DateTime _lastActivity = DateTime.now();
  int _missedHeartbeats = 0;
  final Duration _healthCheckInterval = Duration(seconds: 5);
  final int _maxMissedHeartbeats = 3;
  
  bool get closed;
  void updateHealth(ConnectionHealth health);
  void close();
  
  void startHealthMonitoring({String? connectionType}) {
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
        final connectionTypeMsg = connectionType != null ? '$connectionType ' : '';
        updateHealth(ConnectionHealth(
          isHealthy: isHealthy,
          lastHeartbeat: _lastActivity,
          missedHeartbeats: _missedHeartbeats,
          errorMessage: isHealthy ? null : '${connectionTypeMsg}connection appears to be stale',
        ));
        
        if (!isHealthy) {
          close();
        }
      }
    });
  }
  
  void recordActivity() {
    _lastActivity = DateTime.now();
    _missedHeartbeats = 0;
    updateHealth(ConnectionHealth(
      isHealthy: true,
      lastHeartbeat: _lastActivity,
      missedHeartbeats: 0,
    ));
  }
  
  void recordConnectionClosed(String reason) {
    updateHealth(ConnectionHealth(
      isHealthy: false,
      lastHeartbeat: _lastActivity,
      missedHeartbeats: _missedHeartbeats,
      errorMessage: reason,
    ));
  }
  
  void recordConnectionError(String error) {
    updateHealth(ConnectionHealth(
      isHealthy: false,
      lastHeartbeat: _lastActivity,
      missedHeartbeats: _missedHeartbeats,
      errorMessage: error,
    ));
  }
  
  void stopHealthMonitoring() {
    _healthCheckTimer?.cancel();
  }
  
  void recordFinalHealth(String reason) {
    updateHealth(ConnectionHealth(
      isHealthy: false,
      lastHeartbeat: _lastActivity,
      missedHeartbeats: _missedHeartbeats,
      errorMessage: reason,
    ));
  }
}