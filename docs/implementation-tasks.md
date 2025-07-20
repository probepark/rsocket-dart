# RSocket-Dart Implementation Tasks

This document tracks the implementation of missing features in RSocket-Dart.

## Task Overview

### High Priority
1. **REQUEST_CHANNEL Operation** - Bidirectional streaming support
2. **Cancel Operation** - Frame-based stream cancellation

### Medium Priority  
3. **RequestN (QoS)** - Flow control implementation
4. **Lease (QoS)** - Resource management and rate limiting

## Implementation Details

### 1. REQUEST_CHANNEL Operation
- **Description**: Implement bidirectional streaming where both client and server can send multiple messages
- **Files to modify**:
  - `lib/core/rsocket_requester.dart`
  - `lib/core/rsocket_responder.dart`
  - `lib/frame/frame.dart`
  - `lib/rsocket.dart`
- **Key requirements**:
  - Support Stream<Payload> input and output
  - Handle backpressure
  - Manage stream lifecycle

### 2. Cancel Operation
- **Description**: Implement CANCEL frame to terminate active streams
- **Files to modify**:
  - `lib/frame/frame.dart`
  - `lib/core/rsocket_requester.dart`
  - `lib/core/rsocket_responder.dart`
- **Key requirements**:
  - Send CANCEL frame when stream is disposed
  - Handle incoming CANCEL frames
  - Clean up resources properly

### 3. RequestN (QoS) ✅ COMPLETED
- **Description**: Implement flow control with REQUEST_N frames
- **Status**: Implemented in this commit
- **Files modified**:
  - `lib/frame/frame.dart` - Added `encodeRequestNFrame` method
  - `lib/core/rsocket_requester.dart` - Added demand tracking and REQUEST_N handling
  - `lib/core/stream_demand_tracker.dart` - New file for demand tracking
  - `lib/core/flow_control_rsocket.dart` - New wrapper for flow control configuration
- **Key features implemented**:
  - Track demand for each stream using StreamDemandTracker
  - Send REQUEST_N frames automatically when consuming stream data
  - Respect backpressure signals by checking demand before sending
  - Configurable initial request N value
  - Automatic REQUEST_N generation when 75% of items consumed
- **Documentation**: See `docs/flow_control.md`
- **Tests**: `test/core/request_n_flow_control_test.dart`
- **Example**: `example/request_n_flow_control_example.dart`

### 4. Lease (QoS)
- **Description**: Implement lease-based rate limiting
- **Files to modify**:
  - `lib/frame/frame.dart`
  - `lib/core/rsocket_requester.dart`
  - `lib/core/rsocket_responder.dart`
  - Create new `lib/lease/lease_manager.dart`
- **Key requirements**:
  - Send and receive LEASE frames
  - Track available requests
  - Enforce lease limits

## Testing Strategy
- Unit tests for each feature
- Integration tests with rsocket-cli
- Example code demonstrating usage