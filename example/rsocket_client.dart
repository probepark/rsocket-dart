import 'package:rsocket/shelf.dart';

void main() async {
  // Configure logging
  RSocketLogger.setLogger(ConsoleLogger(minLevel: LogLevel.info));

  // Example 1: Basic connection without retry
  RSocketLogger.info('=== Basic Connection ===');
  try {
    var rsocket =
        await RSocketConnector.create().connect('tcp://127.0.0.1:42252');
    var result =
        await rsocket.requestResponse!(Payload.fromText('text/plain', 'Ping'));
    RSocketLogger.info('Response: ${result.getDataUtf8()}');
  } catch (e) {
    RSocketLogger.error('Connection failed', e);
  }

  // Example 2: Connection with auto-reconnect and custom retry config
  RSocketLogger.info('=== Auto-Reconnect Connection ===');
  final connector = RSocketConnector.create()
      .autoReconnect(true)
      .retryConfig(RetryConfig.aggressive);

  // Listen to connection state changes
  connector.connectionStateStream.listen((event) {
    RSocketLogger.info(
        'Connection state: ${event.state}${event.attemptNumber != null ? ' (attempt ${event.attemptNumber})' : ''}');
    if (event.error != null) {
      RSocketLogger.warning('Connection error', event.error);
    }
  });

  // Listen to connection health changes
  connector.healthStream.listen((health) {
    final status = health.isHealthy ? "Healthy" : "Unhealthy";
    RSocketLogger.info(
        'Connection health: $status (missed heartbeats: ${health.missedHeartbeats})');
    if (health.errorMessage != null) {
      RSocketLogger.warning('Health check error: ${health.errorMessage}');
    }
  });

  try {
    var rsocket = await connector.connect('tcp://127.0.0.1:42252');
    var result = await rsocket
        .requestResponse!(Payload.fromText('text/plain', 'Ping with retry'));
    RSocketLogger.info('Response: ${result.getDataUtf8()}');
  } catch (e) {
    RSocketLogger.error('Final connection failure', e);
  }

  // Example 3: Custom retry configuration
  RSocketLogger.info('=== Custom Retry Configuration ===');
  final customRetryConfig = RetryConfig(
    maxRetryAttempts: 5,
    initialDelay: Duration(milliseconds: 500),
    maxDelay: Duration(seconds: 10),
    backoffMultiplier: 1.5,
    jitter: 0.2,
    retryWhen: (error) {
      // Only retry on specific errors
      return error.toString().contains('connection') ||
          error.toString().contains('timeout');
    },
  );

  final customConnector =
      RSocketConnector.create().autoReconnect().retryConfig(customRetryConfig);

  try {
    await customConnector.connect('tcp://127.0.0.1:42252');
    RSocketLogger.info('Connected with custom retry config');
  } catch (e) {
    RSocketLogger.error('Custom retry failed', e);
  }

  // Example 4: Silent mode (no logging)
  RSocketLogger.info('=== Silent Mode ===');
  RSocketLogger.setLogger(NoOpLogger());

  try {
    var silentConnector = RSocketConnector.create();
    var rsocket = await silentConnector.connect('tcp://127.0.0.1:42252');
    // This won't log anything
    await rsocket
        .requestResponse!(Payload.fromText('text/plain', 'Silent ping'));
  } catch (e) {
    // Silent failure
  }
}
