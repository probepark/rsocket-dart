import 'dart:math' as math;

class RetryConfig {
  final int maxRetryAttempts;
  final Duration initialDelay;
  final Duration maxDelay;
  final double backoffMultiplier;
  final double jitter;
  final bool Function(dynamic error)? retryWhen;

  const RetryConfig({
    this.maxRetryAttempts = 3,
    this.initialDelay = const Duration(seconds: 1),
    this.maxDelay = const Duration(seconds: 30),
    this.backoffMultiplier = 2.0,
    this.jitter = 0.1,
    this.retryWhen,
  });

  Duration calculateDelay(int attemptNumber) {
    final baseDelay = initialDelay.inMilliseconds * 
        math.pow(backoffMultiplier, attemptNumber);
    
    final delayWithJitter = baseDelay + 
        (baseDelay * jitter * (math.Random().nextDouble() - 0.5));
    
    final finalDelay = math.min(delayWithJitter, maxDelay.inMilliseconds);
    
    return Duration(milliseconds: finalDelay.round());
  }

  bool shouldRetry(dynamic error, int attemptNumber) {
    if (attemptNumber >= maxRetryAttempts) return false;
    if (retryWhen != null) return retryWhen!(error);
    return true;
  }

  static const RetryConfig defaultConfig = RetryConfig();
  
  static const RetryConfig aggressive = RetryConfig(
    maxRetryAttempts: 10,
    initialDelay: Duration(milliseconds: 500),
    maxDelay: Duration(seconds: 60),
    backoffMultiplier: 1.5,
    jitter: 0.2,
  );
  
  static const RetryConfig conservative = RetryConfig(
    maxRetryAttempts: 3,
    initialDelay: Duration(seconds: 2),
    maxDelay: Duration(minutes: 2),
    backoffMultiplier: 3.0,
    jitter: 0.05,
  );
}

enum ConnectionState {
  disconnected,
  connecting,
  connected,
  reconnecting,
  failed
}

class ConnectionHealth {
  final bool isHealthy;
  final DateTime lastHeartbeat;
  final int missedHeartbeats;
  final Duration? lastResponseTime;
  final String? errorMessage;

  ConnectionHealth({
    required this.isHealthy,
    required this.lastHeartbeat,
    this.missedHeartbeats = 0,
    this.lastResponseTime,
    this.errorMessage,
  });

  @override
  String toString() {
    return 'ConnectionHealth{isHealthy: $isHealthy, lastHeartbeat: $lastHeartbeat, missedHeartbeats: $missedHeartbeats, lastResponseTime: $lastResponseTime, error: $errorMessage}';
  }
}

class ConnectionEvent {
  final ConnectionState state;
  final DateTime timestamp;
  final dynamic error;
  final int? attemptNumber;

  ConnectionEvent(this.state, {this.error, this.attemptNumber}) 
      : timestamp = DateTime.now();

  @override
  String toString() {
    return 'ConnectionEvent{state: $state, timestamp: $timestamp, error: $error, attemptNumber: $attemptNumber}';
  }
}