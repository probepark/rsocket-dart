import 'package:test/test.dart';
import 'package:rsocket/lease/lease_manager.dart';
import 'package:rsocket/frame/frame.dart';

void main() {
  group('LeaseManager Tests', () {
    late LeaseManager leaseManager;

    setUp(() {
      leaseManager = LeaseManager();
    });

    tearDown(() {
      leaseManager.dispose();
    });

    test('should create lease with correct parameters', () {
      final lease = Lease(
        numberOfRequests: 100,
        timeToLive: 5000, // 5 seconds
      );

      expect(lease.numberOfRequests, equals(100));
      expect(lease.timeToLive, equals(5000));
      expect(lease.isValid(), isTrue);
    });

    test('should expire lease after TTL', () async {
      final lease = Lease(
        numberOfRequests: 100,
        timeToLive: 100, // 100ms
      );

      expect(lease.isValid(), isTrue);
      
      await Future.delayed(Duration(milliseconds: 150));
      
      expect(lease.isValid(), isFalse);
      expect(lease.remainingTtl(), equals(0));
    });

    test('should update lease in manager', () {
      leaseManager.updateLease(50, 10000); // 50 requests, 10 seconds

      expect(leaseManager.availableRequests, equals(50));
      expect(leaseManager.hasAvailableRequests, isTrue);
      expect(leaseManager.isLeaseValid(), isTrue);
    });

    test('should consume single request', () {
      leaseManager.updateLease(5, 10000);

      expect(leaseManager.consumeRequest(), isTrue);
      expect(leaseManager.availableRequests, equals(4));

      expect(leaseManager.consumeRequest(), isTrue);
      expect(leaseManager.availableRequests, equals(3));
    });

    test('should fail to consume when no requests available', () {
      leaseManager.updateLease(1, 10000);

      expect(leaseManager.consumeRequest(), isTrue);
      expect(leaseManager.availableRequests, equals(0));
      expect(leaseManager.consumeRequest(), isFalse);
    });

    test('should consume multiple requests', () {
      leaseManager.updateLease(10, 10000);

      expect(leaseManager.consumeRequests(5), isTrue);
      expect(leaseManager.availableRequests, equals(5));

      expect(leaseManager.consumeRequests(6), isFalse); // Not enough
      expect(leaseManager.availableRequests, equals(5)); // Unchanged

      expect(leaseManager.consumeRequests(5), isTrue);
      expect(leaseManager.availableRequests, equals(0));
    });

    test('should trigger onRequestsExhausted callback', () {
      var callbackTriggered = false;
      leaseManager.onRequestsExhausted = () {
        callbackTriggered = true;
      };

      leaseManager.updateLease(1, 10000);
      leaseManager.consumeRequest();

      expect(callbackTriggered, isTrue);
    });

    test('should trigger onLeaseExpired callback', () async {
      var callbackTriggered = false;
      leaseManager.onLeaseExpired = () {
        callbackTriggered = true;
      };

      leaseManager.updateLease(10, 100); // 100ms TTL

      await Future.delayed(Duration(milliseconds: 150));

      expect(callbackTriggered, isTrue);
      expect(leaseManager.isLeaseValid(), isFalse);
    });

    test('should encode lease frame correctly', () {
      final frame = FrameCodec.encodeLeaseFrame(60000, 100);
      
      expect(frame.length, greaterThan(0));
      // Frame should start with 3-byte length, 4-byte stream ID (0), 
      // 1-byte frame type, 1-byte flags, 4-byte TTL, 4-byte requests
      expect(frame.length, greaterThanOrEqualTo(17));
    });

    test('should broadcast lease updates', () async {
      final updates = <Lease>[];
      leaseManager.leaseUpdates.listen((lease) {
        updates.add(lease);
      });

      leaseManager.updateLease(100, 10000);
      leaseManager.updateLease(200, 20000);

      await Future.delayed(Duration(milliseconds: 10));

      expect(updates.length, equals(2));
      expect(updates[0].numberOfRequests, equals(100));
      expect(updates[1].numberOfRequests, equals(200));
    });
  });

  group('ServerLeaseManager Tests', () {
    late ServerLeaseManager serverLeaseManager;

    setUp(() {
      serverLeaseManager = ServerLeaseManager(
        defaultNumberOfRequests: 100,
        defaultTimeToLive: 10000,
        maxRequestsPerSecond: 10,
        refillInterval: Duration(milliseconds: 100),
      );
    });

    tearDown(() {
      serverLeaseManager.dispose();
    });

    test('should start with default parameters', () {
      serverLeaseManager.start();
      
      expect(serverLeaseManager.availableRequests, equals(100));
      expect(serverLeaseManager.isLeaseValid(), isTrue);
    });

    test('should refill requests periodically', () async {
      serverLeaseManager = ServerLeaseManager(
        defaultNumberOfRequests: 100,
        defaultTimeToLive: 10000,
        maxRequestsPerSecond: 10,
        refillInterval: Duration(milliseconds: 100),
      );
      serverLeaseManager.start();
      
      // Initial lease should be granted
      expect(serverLeaseManager.availableRequests, equals(100));
      
      // Consume some requests
      serverLeaseManager.consumeRequests(50);
      expect(serverLeaseManager.availableRequests, equals(50));
      
      // Wait for multiple refill intervals to ensure at least one refill happens
      await Future.delayed(Duration(milliseconds: 250));
      
      // Should have refilled at least 1 request (10 req/sec * 0.1 sec = 1 request per interval)
      // But we waited for 2.5 intervals, so should have at least 2 requests refilled
      expect(serverLeaseManager.availableRequests, greaterThanOrEqualTo(52));
    });

    test('should cap refill at maximum', () async {
      serverLeaseManager = ServerLeaseManager(
        defaultNumberOfRequests: 10,
        defaultTimeToLive: 10000,
        maxRequestsPerSecond: 100,
        refillInterval: Duration(milliseconds: 100),
      );
      
      serverLeaseManager.start();
      
      // Consume all
      serverLeaseManager.consumeRequests(10);
      expect(serverLeaseManager.availableRequests, equals(0));
      
      // Wait for multiple refills
      await Future.delayed(Duration(milliseconds: 250));
      
      // Should be capped at defaultNumberOfRequests
      expect(serverLeaseManager.availableRequests, lessThanOrEqualTo(10));
    });

    test('should grant lease with custom parameters', () {
      final frame = serverLeaseManager.grantLease(
        numberOfRequests: 200,
        timeToLive: 30000,
      );
      
      expect(frame.length, greaterThan(0));
      expect(serverLeaseManager.availableRequests, equals(200));
    });

    test('should stop refilling when stopped', () async {
      serverLeaseManager.start();
      serverLeaseManager.consumeRequests(50);
      
      serverLeaseManager.stop();
      
      var requestsAfterStop = serverLeaseManager.availableRequests;
      await Future.delayed(Duration(milliseconds: 200));
      
      // Should not have refilled after stop
      expect(serverLeaseManager.availableRequests, equals(requestsAfterStop));
    });
  });
}