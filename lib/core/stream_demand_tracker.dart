import 'dart:collection';

import 'rsocket_requester.dart' show MAX_REQUEST_N_SIZE;

/// Tracks demand for each stream to implement flow control
class StreamDemandTracker {
  final Map<int, int> _demandByStreamId = HashMap();

  /// Get current demand for a stream
  int getDemand(int streamId) {
    return _demandByStreamId[streamId] ?? 0;
  }

  /// Add demand for a stream (from REQUEST_N frames)
  void addDemand(int streamId, int n) {
    if (n <= 0) return;

    var currentDemand = _demandByStreamId[streamId] ?? 0;
    // Prevent overflow
    if (currentDemand > 0 && n > (MAX_REQUEST_N_SIZE - currentDemand)) {
      _demandByStreamId[streamId] = MAX_REQUEST_N_SIZE;
    } else {
      _demandByStreamId[streamId] = currentDemand + n;
    }
  }

  /// Consume demand when sending a frame
  bool consumeDemand(int streamId) {
    var demand = _demandByStreamId[streamId] ?? 0;
    if (demand > 0) {
      _demandByStreamId[streamId] = demand - 1;
      return true;
    }
    return false;
  }

  /// Check if there is demand without consuming
  bool hasDemand(int streamId) {
    return (_demandByStreamId[streamId] ?? 0) > 0;
  }

  /// Remove tracking for a stream (when stream completes or errors)
  void removeStream(int streamId) {
    _demandByStreamId.remove(streamId);
  }

  /// Clear all demand tracking
  void clear() {
    _demandByStreamId.clear();
  }
}

