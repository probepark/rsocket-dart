# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RSocket-Dart is a complete Dart implementation of the RSocket protocol, supporting all core operations including reactive streams communication patterns. The library provides request/response, fire-and-forget, request stream, request channel, and metadata push operations with full protocol compliance.

## Commands

### Development Commands

```bash
# Install dependencies
dart pub get

# Run all tests
dart test

# Run specific test file
dart test test/core/rsocket_requester_test.dart

# Run tests matching pattern
dart test --name "REQUEST_CHANNEL"

# Format code (required before commits)
dart format lib test example
# or use justfile:
just format

# Analyze code for issues  
dart analyze --fatal-infos
```

### Integration Testing with rsocket-cli

```bash
# Start test server (requires rsocket-cli installed)
just server

# Test request-response
just request

# Test streaming
just stream

# Test server streaming  
just server-stream
```

## Architecture

### Core Communication Flow

The RSocket protocol implementation follows a layered architecture:

1. **Transport Layer** (`duplex_connection.dart`) - Handles TCP/WebSocket connections
2. **Frame Layer** (`frame/`) - Binary protocol frame encoding/decoding
3. **Protocol Layer** (`core/rsocket_requester.dart`) - RSocket protocol implementation
4. **Application Layer** (`rsocket_connector.dart`, `rsocket_server.dart`) - High-level APIs

### Key Components

**Connection Management**:
- `DuplexConnection` - Abstract transport interface supporting TCP and WebSocket
- `RSocketConnector` - Client connection builder with auto-reconnect and retry capabilities
- `RSocketServer` - Server binding and acceptor pattern implementation

**Protocol Implementation**:
- `RSocketRequester` - Core protocol engine handling all frame types and operations
- Stream ID management with client/server allocation (odd/even)
- Flow control via `StreamDemandTracker` implementing backpressure
- Lease management for rate limiting and QoS

**Frame Processing**:
- Complete binary frame codec in `frame/frame.dart`
- Support for all RSocket frame types (SETUP, PAYLOAD, ERROR, CANCEL, etc.)
- Efficient parsing with `RSocketByteBuffer` for zero-copy operations

### Advanced Features

**Lease Management** (`lease/lease_manager.dart`):
- Server-side rate limiting with TTL-based lease grants
- Client-side lease consumption and tracking
- Configurable lease policies for QoS control

**Health Monitoring** (`health_monitoring_mixin.dart`):
- Connection health tracking with heartbeat monitoring
- Automatic reconnection on connection failures
- Configurable health check intervals and thresholds

**Flow Control** (`core/stream_demand_tracker.dart`):
- REQUEST_N based backpressure implementation
- Per-stream demand tracking
- Automatic demand management for smooth streaming

### RSocket Operations

All operations are implemented as function typedefs:
```dart
typedef RequestResponse = Future<Payload> Function(Payload? payload);
typedef FireAndForget = Future<void> Function(Payload? payload);  
typedef RequestStream = Stream<Payload?> Function(Payload? payload);
typedef RequestChannel = Stream<Payload> Function(Stream<Payload> payloads);
typedef MetadataPush = Future<void> Function(Payload? payload);
```

**Key Implementation Details**:
- REQUEST_CHANNEL supports full bidirectional streaming
- Flow control prevents memory exhaustion under high load
- Automatic stream cleanup on connection termination
- Error propagation with proper RSocket error codes

### Service Architecture

**Routing & RPC** (`route/`):
- Service proxy generation for type-safe RPC
- Load balancing across multiple RSocket endpoints
- Service discovery integration
- Reflection-based method invocation

**Metadata System** (`metadata/`):
- Composite metadata encoding/decoding
- Well-known MIME type registry
- Custom metadata extension support

## Development Notes

### Protocol Compliance
- Full RSocket protocol v1.0 implementation
- All frame types supported including LEASE, CANCEL, REQUEST_N
- Proper keep-alive and connection lifecycle management
- Binary compatibility with other RSocket implementations

### Performance Considerations
- Zero-copy frame parsing where possible
- Configurable stream limits (MAX_CONCURRENT_STREAMS = 256)
- Efficient byte buffer management
- RxDart integration for reactive stream processing

### Testing Architecture
- Unit tests for all frame types and protocol operations
- Integration tests with external rsocket-cli tool
- Mock connection infrastructure for isolated testing
- Example implementations in `example/` directory

### Code Organization
- Examples moved to `example/` directory (not in root)
- Test structure mirrors library organization
- Null safety throughout with defensive programming
- No custom linting - uses Dart analyzer defaults