enum LogLevel {
  debug,
  info,
  warning,
  error,
}

abstract class Logger {
  void log(LogLevel level, String message,
      [dynamic error, StackTrace? stackTrace]);

  void debug(String message) => log(LogLevel.debug, message);
  void info(String message) => log(LogLevel.info, message);
  void warning(String message, [dynamic error]) =>
      log(LogLevel.warning, message, error);
  void error(String message, [dynamic error, StackTrace? stackTrace]) =>
      log(LogLevel.error, message, error, stackTrace);
}

class NoOpLogger implements Logger {
  const NoOpLogger();

  @override
  void log(LogLevel level, String message,
      [dynamic error, StackTrace? stackTrace]) {
    // Do nothing
  }

  @override
  void debug(String message) {}

  @override
  void error(String message, [dynamic error, StackTrace? stackTrace]) {}

  @override
  void info(String message) {}

  @override
  void warning(String message, [dynamic error]) {}
}

class ConsoleLogger implements Logger {
  final LogLevel minLevel;

  const ConsoleLogger({this.minLevel = LogLevel.info});

  @override
  void log(LogLevel level, String message,
      [dynamic error, StackTrace? stackTrace]) {
    if (level.index < minLevel.index) return;

    final timestamp = DateTime.now().toIso8601String();
    final levelStr = _getLevelString(level);

    print('[$timestamp] $levelStr $message');

    if (error != null) {
      print('  Error: $error');
    }

    if (stackTrace != null) {
      print('  Stack trace: $stackTrace');
    }
  }

  @override
  void debug(String message) => log(LogLevel.debug, message);

  @override
  void error(String message, [dynamic error, StackTrace? stackTrace]) =>
      log(LogLevel.error, message, error, stackTrace);

  @override
  void info(String message) => log(LogLevel.info, message);

  @override
  void warning(String message, [dynamic error]) =>
      log(LogLevel.warning, message, error);

  String _getLevelString(LogLevel level) {
    switch (level) {
      case LogLevel.debug:
        return 'DEBUG  ';
      case LogLevel.info:
        return 'INFO   ';
      case LogLevel.warning:
        return 'WARNING';
      case LogLevel.error:
        return 'ERROR  ';
    }
  }
}

class RSocketLogger {
  static Logger _instance = const NoOpLogger();

  static Logger get instance => _instance;

  static void setLogger(Logger logger) {
    _instance = logger;
  }

  static void debug(String message) => _instance.debug(message);
  static void info(String message) => _instance.info(message);
  static void warning(String message, [dynamic error]) =>
      _instance.warning(message, error);
  static void error(String message, [dynamic error, StackTrace? stackTrace]) =>
      _instance.error(message, error, stackTrace);
}
