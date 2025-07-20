# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RSocket-Dart is a Dart implementation of the RSocket protocol, supporting reactive streams communication patterns. The library provides request/response, fire-and-forget, request stream, and metadata push operations.

## Commands

### Development Commands

```bash
# Install dependencies
dart pub get

# Run all tests
dart test

# Run specific test
dart test test/path/to/specific_test.dart

# Format code (required before commits)
dart format lib test
# or use justfile:
just format

# Analyze code for issues
dart analyze
```

### Integration Testing with rsocket-cli

```bash
# Start test server
just server

# Test request-response
just request

# Test streaming
just stream

# Test server streaming
just server-stream
```

## Architecture

### Core Module Structure

- **`lib/core/`** - Core protocol implementation
  - `rsocket_requester.dart` - Client-side request handling
  - `rsocket_responder.dart` - Server-side response handling
  - `rsocket_error.dart` - Error definitions and handling
  - `stream_id_manager.dart` - Stream ID allocation

- **`lib/frame/`** - RSocket frame protocol
  - Frame encoding/decoding for all RSocket frame types
  - Binary protocol implementation

- **`lib/route/`** - Service routing and RPC
  - `rsocket_rpc_proxy.dart` - Reflection-based RPC proxy
  - `load_balance.dart` - Client-side load balancing
  - Service registration and discovery

- **`lib/metadata/`** - Metadata handling
  - Composite metadata support
  - MIME type handling

### Key Interfaces

**RSocket Operations** (defined via typedefs):
```dart
typedef RequestResponse = Future<Payload> Function(Payload? payload);
typedef FireAndForget = Future<void> Function(Payload? payload);
typedef RequestStream = Stream<Payload?> Function(Payload? payload);
typedef MetadataPush = Future<void> Function(Payload metadata);
```

**Transport Abstraction**: All transports implement `DuplexConnection` for unified handling of TCP and WebSocket connections.

### Design Patterns

1. **Builder Pattern**: `RSocketConnector` uses builder pattern for client configuration
2. **Reflection-Based RPC**: Service routing uses Dart mirrors for dynamic invocation
3. **Stream-Based**: Heavy use of Dart streams for reactive programming
4. **Null Safety**: Fully migrated to Dart null safety

## Development Notes

- No custom linting rules - project uses Dart defaults
- Test files mirror the library structure in `test/` directory
- Example implementations in root: `rsocket_client.dart` and `rsocket_server.dart`
- REQUEST_CHANNEL operation is not yet implemented

## Testing Strategy

1. Unit tests cover individual components
2. Integration tests use rsocket-cli via justfile commands
3. No automated CI/CD detected - run tests manually before commits