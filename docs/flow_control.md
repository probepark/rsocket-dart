# RSocket-Dart Flow Control (REQUEST_N)

## Overview

RSocket-Dart now supports REQUEST_N flow control, allowing consumers to control the rate at which they receive data from streams. This is a Quality of Service (QoS) feature that implements backpressure to prevent overwhelming consumers with data.

## How It Works

1. **Initial Request**: When a client requests a stream, it specifies an initial `requestN` value indicating how many items it's ready to receive.

2. **Demand Tracking**: The server tracks demand for each stream and only sends data when there is outstanding demand.

3. **REQUEST_N Frames**: As the client consumes data, it sends REQUEST_N frames to signal demand for more items.

4. **Backpressure**: If the client doesn't request more items, the server will stop sending data (respecting backpressure).

## Implementation Details

### Key Components

1. **StreamDemandTracker**: Tracks demand per stream ID
   - `addDemand(streamId, n)`: Adds demand when REQUEST_N frames arrive
   - `consumeDemand(streamId)`: Consumes one unit of demand when sending data
   - `hasDemand(streamId)`: Checks if there's demand without consuming

2. **REQUEST_N Frame**: 
   - Frame type 0x08
   - Contains stream ID and requested count (32-bit integer)

3. **Flow Control in RSocketRequester**:
   - Incoming streams respect demand from REQUEST_N frames
   - Outgoing streams automatically send REQUEST_N frames as data is consumed

### Default Behavior

- **MAX_REQUEST_N_SIZE (0x7FFFFFFF)**: Default value for unlimited demand
- **Configurable Request N**: Clients can specify initial request N value
- **Automatic REQUEST_N**: Framework sends REQUEST_N frames when 75% of requested items are consumed

## Usage Example

```dart
// Server respects backpressure automatically
server.acceptor = (setup, sendingSocket) {
  return RSocket()
    ..requestStream = (payload) {
      // This stream will respect REQUEST_N flow control
      return generateDataStream();
    };
};

// Client with default flow control (unlimited)
var stream1 = rsocket.requestStream!(payload);

// Client with custom initial request N
// Note: This requires using the RSocketRequester directly
// as the public API uses the typedef which doesn't support optional parameters
```

## Current Limitations

1. The public `RequestStream` typedef doesn't support optional parameters, so configuring initial request N requires direct use of `RSocketRequester`.

2. When demand is exhausted, frames are currently dropped with a warning. Future improvements could implement buffering strategies.

3. REQUEST_CHANNEL operation doesn't yet integrate with flow control (not implemented).

## Testing

Run the flow control tests:
```bash
dart test test/core/request_n_flow_control_test.dart
```

Run the example:
```bash
dart run example/request_n_flow_control_example.dart
```

## Future Enhancements

1. **Buffering Strategy**: Add configurable buffering when demand is exhausted
2. **Flow Control Policies**: Support different policies (drop, buffer, error)
3. **REQUEST_CHANNEL Integration**: Extend flow control to bidirectional streams
4. **Metrics**: Add demand tracking metrics for monitoring