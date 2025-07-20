# RSocket Dart SDK Development Guidelines

## Build/Configuration Instructions

### Prerequisites
- Dart SDK >=2.12.0 <3.0.0 (null safety enabled)
- Dependencies are minimal: `web_socket_channel`, `universal_io`

### Setup
```bash
# Install dependencies
dart pub get

# Format code (recommended before commits)
dart format lib test
# Or use the justfile command:
just format
```

### Project Structure
- `lib/` - Main library code organized by functionality
  - `core/` - Core RSocket implementation (requester, responder, errors)
  - `frame/` - Frame handling and types
  - `io/` - I/O utilities (bytes handling)
  - `metadata/` - Metadata handling and MIME types
  - `route/` - Service routing and load balancing
- `test/` - Unit tests mirroring lib structure
- `rsocket_client.dart` & `rsocket_server.dart` - Example implementations

## Testing Information

### Running Tests
```bash
# Run all tests
dart test

# Run specific test file
dart test test/io/bytes_test.dart

# Run with detailed stack traces for debugging
dart test --chain-stack-traces
```

### Test Structure and Patterns
Tests use the standard Dart `test` package with these conventions:

```dart
import 'package:test/test.dart';
import 'package:rsocket/payload.dart';

void main() {
  group('Feature Tests', () {
    test('specific functionality', () {
      // Arrange
      var payload = Payload.fromText('text/plain', 'Hello World');
      
      // Act & Assert
      expect(payload.getDataUtf8(), equals('Hello World'));
      expect(payload.getDataUtf8(), contains('Hello'));
      expect(payload.getMetadataUtf8(), isNotNull);
    });
  });
}
```

### Adding New Tests
1. Create test files in `test/` directory mirroring the `lib/` structure
2. Use `group()` to organize related tests
3. Import project modules using `package:rsocket/...` syntax
4. Use descriptive test names and organize with arrange/act/assert pattern
5. Common assertions: `equals()`, `contains()`, `isNull`, `isNotNull`

### Test Example
A working test example is available in `test/example_test.dart` demonstrating:
- Payload creation and manipulation
- Different assertion types
- Test organization with groups

### Integration Testing
The project includes `justfile` commands for integration testing with `rsocket-cli`:
```bash
# Start test server
just server

# Send test request
just request

# Test streaming
just stream
```

## Development Information

### Code Style and Conventions

#### Naming Conventions
- **Classes**: PascalCase (`RSocketConnector`, `ConnectionSetupPayload`)
- **Methods/Variables**: camelCase (`requestResponse`, `keepAliveInterval`)
- **Private fields**: Prefix with underscore (`_dataMimeType`, `_metadataMimeType`)
- **Constants**: camelCase with descriptive names

#### Architectural Patterns

**1. Fluent API Pattern**
```dart
var rsocket = await RSocketConnector.create()
    .dataMimeType('application/json')
    .keepAlive(30, 120)
    .connect('tcp://127.0.0.1:42252');
```

**2. Typedef for Function Signatures**
```dart
typedef RequestResponse = Future<Payload> Function(Payload? payload);
typedef FireAndForget = Future<void> Function(Payload? payload);
```

**3. Factory Functions for Acceptors**
```dart
SocketAcceptor requestResponseAcceptor(RequestResponse requestResponse) {
  return (setup, sendingSocket) {
    return RSocket()..requestResponse = requestResponse;
  };
}
```

**4. Cascade Operator Usage**
```dart
var connectionSetupPayload = ConnectionSetupPayload()
  ..keepAliveInterval = keepAliveInterval
  ..keepAliveMaxLifetime = keepAliveMaxLifeTime
  ..metadataMimeType = _metadataMimeType;
```

#### Error Handling
- Use descriptive error messages with RSocket error codes
- Default implementations throw exceptions for unsupported operations
- Custom error types in `core/rsocket_error.dart`

#### Null Safety
- Project uses null safety (Dart >=2.12.0)
- Use nullable types (`String?`, `Payload?`) appropriately
- Handle null cases explicitly in getters (e.g., `getDataUtf8()` returns `String?`)

#### Async Patterns
- Use `Future<T>` for single async operations
- Use `Stream<T>` for streaming data
- Prefer `async/await` over `.then()` chains
- Handle errors with try/catch or Future.error()

### Key Classes and Interfaces

**Core Abstractions:**
- `RSocket` - Main interface with operation methods
- `Closeable` - Resource cleanup interface
- `Availability` - Connection availability interface

**Client/Server:**
- `RSocketConnector` - Fluent API for client connections
- `RSocketServer` - Server setup and binding
- `RSocketRequester` - Client-side implementation

**Data Handling:**
- `Payload` - Data and metadata container
- `RSocketByteBuffer` - Low-level byte operations
- Frame types for protocol implementation

### Development Tools
- **Formatting**: Use `dart format lib test` or `just format`
- **Analysis**: Project uses default Dart linting rules (no custom `analysis_options.yaml`)
- **Integration Testing**: `rsocket-cli` for protocol-level testing

### Common Pitfalls
1. **Payload API**: Use `Payload()` constructor for empty payloads, not `Payload.empty()`
2. **Null Handling**: `getDataUtf8()` returns `null` for empty payloads, not empty string
3. **Integration Tests**: Some tests may fail without running RSocket server (expected behavior)
4. **Error Messages**: Include RSocket error codes in custom error messages

### Performance Considerations
- Use `Uint8List` for binary data to avoid unnecessary conversions
- Stream operations are lazy - consider buffering for high-throughput scenarios
- Connection pooling available through load balancing utilities in `route/` package
