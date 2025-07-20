import 'dart:async';
import 'dart:typed_data';

import '../frame/frame.dart';
import '../frame/frame_types.dart' as frame_types;
import '../io/bytes.dart';

/// Represents a lease granted by the server to control request rates
class Lease {
  final int numberOfRequests;
  final int timeToLive; // in milliseconds
  final DateTime grantedAt;

  Lease({
    required this.numberOfRequests,
    required this.timeToLive,
    DateTime? grantedAt,
  }) : grantedAt = grantedAt ?? DateTime.now();

  /// Check if the lease is still valid based on TTL
  bool isValid() {
    final elapsed = DateTime.now().difference(grantedAt).inMilliseconds;
    return elapsed < timeToLive;
  }

  /// Get remaining time to live in milliseconds
  int remainingTtl() {
    final elapsed = DateTime.now().difference(grantedAt).inMilliseconds;
    final remaining = timeToLive - elapsed;
    return remaining > 0 ? remaining : 0;
  }

  /// Create a copy with updated request count
  Lease withUpdatedRequests(int newRequestCount) {
    return Lease(
      numberOfRequests: newRequestCount,
      timeToLive: remainingTtl(),
      grantedAt: grantedAt,
    );
  }
}

/// Manages lease state and enforcement for RSocket connections
class LeaseManager {
  Lease? _currentLease;
  int _availableRequests = 0;
  Timer? _expirationTimer;
  final StreamController<Lease> _leaseUpdateController = StreamController.broadcast();
  
  /// Callback for when lease expires
  void Function()? onLeaseExpired;
  
  /// Callback for when available requests reaches zero
  void Function()? onRequestsExhausted;

  /// Stream of lease updates
  Stream<Lease> get leaseUpdates => _leaseUpdateController.stream;

  /// Current number of available requests
  int get availableRequests => _availableRequests;

  /// Check if we have available requests under the current lease
  bool get hasAvailableRequests => _availableRequests > 0 && isLeaseValid();

  /// Check if the current lease is valid
  bool isLeaseValid() {
    return _currentLease != null && _currentLease!.isValid();
  }

  /// Update the lease with a new grant
  void updateLease(int numberOfRequests, int timeToLive) {
    _expirationTimer?.cancel();
    
    _currentLease = Lease(
      numberOfRequests: numberOfRequests,
      timeToLive: timeToLive,
    );
    _availableRequests = numberOfRequests;
    
    // Notify listeners if controller is not closed
    if (!_leaseUpdateController.isClosed) {
      _leaseUpdateController.add(_currentLease!);
    }
    
    // Set up expiration timer
    _expirationTimer = Timer(Duration(milliseconds: timeToLive), () {
      _handleLeaseExpiration();
    });
  }

  /// Consume a request if available
  bool consumeRequest() {
    if (!hasAvailableRequests) {
      return false;
    }
    
    _availableRequests--;
    
    if (_availableRequests == 0 && onRequestsExhausted != null) {
      onRequestsExhausted!();
    }
    
    return true;
  }

  /// Try to consume multiple requests
  bool consumeRequests(int count) {
    if (!isLeaseValid() || _availableRequests < count) {
      return false;
    }
    
    _availableRequests -= count;
    
    if (_availableRequests == 0 && onRequestsExhausted != null) {
      onRequestsExhausted!();
    }
    
    return true;
  }

  /// Handle lease expiration
  void _handleLeaseExpiration() {
    _currentLease = null;
    _availableRequests = 0;
    
    if (onLeaseExpired != null) {
      onLeaseExpired!();
    }
  }

  /// Create a lease frame from current lease state
  Uint8List? createLeaseFrame({int? numberOfRequests, int? timeToLive}) {
    final requests = numberOfRequests ?? _availableRequests;
    final ttl = timeToLive ?? (_currentLease?.remainingTtl() ?? 0);
    
    if (requests <= 0 || ttl <= 0) {
      return null;
    }
    
    return FrameCodec.encodeLeaseFrame(ttl, requests);
  }

  /// Dispose of resources
  void dispose() {
    _expirationTimer?.cancel();
    _leaseUpdateController.close();
  }
  
  /// Encode a LEASE frame to send to peer
  static Uint8List encodeLeaseFrame(int numberOfRequests, int timeToLive, {Uint8List? metadata}) {
    return FrameCodec.encodeLeaseFrame(timeToLive, numberOfRequests, metadata: metadata);
  }
}

/// Manages lease state for server-side (granting leases)
class ServerLeaseManager extends LeaseManager {
  /// Default lease parameters
  int defaultNumberOfRequests;
  int defaultTimeToLive;
  
  /// Rate limiting parameters
  final int maxRequestsPerSecond;
  final Duration refillInterval;
  Timer? _refillTimer;
  
  ServerLeaseManager({
    this.defaultNumberOfRequests = 128,
    this.defaultTimeToLive = 60000, // 60 seconds
    this.maxRequestsPerSecond = 100,
    this.refillInterval = const Duration(seconds: 1),
  });
  
  /// Start the lease manager (begins refilling)
  void start() {
    _refillTimer?.cancel();
    _refillTimer = Timer.periodic(refillInterval, (_) {
      _refillRequests();
    });
    
    // Grant initial lease
    updateLease(defaultNumberOfRequests, defaultTimeToLive);
  }
  
  /// Stop the lease manager
  void stop() {
    _refillTimer?.cancel();
    _refillTimer = null;
  }
  
  /// Refill available requests based on rate limit
  void _refillRequests() {
    if (isLeaseValid()) {
      // Calculate refill amount based on the refill interval in milliseconds
      final refillAmount = (maxRequestsPerSecond * refillInterval.inMilliseconds / 1000).round();
      final newTotal = _availableRequests + refillAmount;
      
      // Cap at default maximum
      _availableRequests = newTotal > defaultNumberOfRequests 
          ? defaultNumberOfRequests 
          : newTotal;
    }
  }
  
  /// Grant a new lease to the client
  Uint8List grantLease({int? numberOfRequests, int? timeToLive}) {
    final requests = numberOfRequests ?? defaultNumberOfRequests;
    final ttl = timeToLive ?? defaultTimeToLive;
    
    updateLease(requests, ttl);
    return FrameCodec.encodeLeaseFrame(ttl, requests);
  }
  
  @override
  void dispose() {
    stop();
    super.dispose();
  }
}