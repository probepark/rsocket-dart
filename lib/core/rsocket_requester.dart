import 'dart:async';
import 'dart:typed_data';

import '../core/rsocket_error.dart';
import '../frame/frame_types.dart' as frame_types;
import '../duplex_connection.dart';
import '../payload.dart';
import '../rsocket.dart';
import '../frame/frame.dart';
import '../io/bytes.dart';
import '../lease/lease_manager.dart';
import 'stream_id_supplier.dart';
import 'stream_demand_tracker.dart';

Future<void> voidFuture() async {}

const MAX_REQUEST_N_SIZE = 0x7FFFFFFF;

abstract class Subscriber {
  void onNext(Payload? value);

  void onError(dynamic error);

  void onComplete();
}

class CompleterSubscriber implements Subscriber {
  Completer completer;
  Payload? payload;

  CompleterSubscriber(this.completer);

  @override
  void onNext(Payload? payload) {
    this.payload = payload;
  }

  @override
  void onError(dynamic error) {
    completer.completeError(error);
  }

  @override
  void onComplete() {
    completer.complete(payload);
  }
}

class StreamSubscriber implements Subscriber {
  final StreamController controller;
  final int streamId;
  final DuplexConnection connection;
  final StreamDemandTracker? demandTracker;
  final int requestN;
  int _requested = 0;
  int _received = 0;

  StreamSubscriber({
    required this.streamId,
    required this.connection,
    this.demandTracker,
    this.requestN = 32, // Default request N value
    FutureOr<void> Function()? onCancel,
  }) : controller = StreamController(onCancel: onCancel);

  @override
  void onNext(Payload? value) {
    controller.add(value);
    _received++;

    // Request more items when we've consumed 75% of requested items
    if (requestN != MAX_REQUEST_N_SIZE &&
        _received >= (_requested * 0.75).floor()) {
      _requestMore();
    }
  }

  @override
  void onError(dynamic error) {
    controller.addError(error);
  }

  @override
  void onComplete() {
    controller.close().then((value) => {});
  }

  Stream<Payload?> payloadStream() {
    // Initial request
    if (requestN != MAX_REQUEST_N_SIZE) {
      _requestMore();
    }
    return controller.stream.map((item) => item as Payload?);
  }

  void _requestMore() {
    var toRequest = requestN;
    _requested += toRequest;
    connection.write(FrameCodec.encodeRequestNFrame(streamId, toRequest));
  }
}

// Wrapper for StreamSubscription to implement Subscriber interface
class _StreamSubscription implements Subscriber {
  final StreamSubscription subscription;

  _StreamSubscription(this.subscription);

  @override
  void onNext(Payload? value) {
    // Not used for responder-side subscriptions
  }

  @override
  void onError(dynamic error) {
    subscription.cancel();
  }

  @override
  void onComplete() {
    subscription.cancel();
  }

  void cancel() {
    subscription.cancel();
  }
}

// Subscriber for REQUEST_CHANNEL that manages bidirectional streaming
class _ChannelSubscriber implements Subscriber {
  final StreamController<Payload> inputController;
  StreamSubscription? subscription;
  
  _ChannelSubscriber(this.inputController);
  
  @override
  void onNext(Payload? value) {
    if (value != null && !inputController.isClosed) {
      inputController.add(value);
    }
  }
  
  @override
  void onError(dynamic error) {
    if (!inputController.isClosed) {
      inputController.addError(error);
      inputController.close();
    }
    subscription?.cancel();
  }
  
  @override
  void onComplete() {
    if (!inputController.isClosed) {
      inputController.close();
    }
    subscription?.cancel();
  }
  
  void cancel() {
    if (!inputController.isClosed) {
      inputController.close();
    }
    subscription?.cancel();
  }
}

class RSocketRequester extends RSocket {
  bool closed = false;
  double _availability = 1.0;
  Timer? keepAliveTimer;
  late StreamIdSupplier streamIdSupplier;
  ConnectionSetupPayload? connectionSetupPayload;
  late DuplexConnection connection;

  //buffer for data chunk
  List<int>? chunkBuffer;

  Map<int, Subscriber> senders = {};
  RSocket? responder;
  String mode = 'requester';
  ErrorConsumer? errorConsumer;

  // Lease management
  LeaseManager? leaseManager;
  ServerLeaseManager? serverLeaseManager;
  bool leaseEnabled = false;

  // Demand tracking for flow control
  final StreamDemandTracker _incomingDemandTracker = StreamDemandTracker();
  final StreamDemandTracker _outgoingDemandTracker = StreamDemandTracker();

  RSocketRequester(String mode, ConnectionSetupPayload connectionSetupPayload,
      DuplexConnection connection, {bool enableLease = false}) {
    this.mode = mode;
    if (mode == 'requester') {
      streamIdSupplier = StreamIdSupplier.clientSupplier();
    } else {
      streamIdSupplier = StreamIdSupplier.serverSupplier();
    }
    this.connectionSetupPayload = connectionSetupPayload;
    this.connection = connection;
    this.leaseEnabled = enableLease;
    
    // Initialize lease management
    if (enableLease) {
      if (mode == 'requester') {
        leaseManager = LeaseManager();
        leaseManager!.onLeaseExpired = () {
          _availability = 0.0;
        };
      } else {
        serverLeaseManager = ServerLeaseManager();
        serverLeaseManager!.start();
      }
    }
    
    if (this.connection.receiveHandler == null) {
      this.connection.receiveHandler = (chunk) => receiveChunk(chunk);
    }
    this.connection.closeHandler = () {
      close();
    };
    initRSocketCallStubs();
  }

  void initRSocketCallStubs() {
    //RSocket requestResponse
    requestResponse = (payload) {
      var completer = Completer<Payload>();

      // Check lease if enabled
      if (leaseEnabled && mode == 'requester') {
        if (leaseManager == null || !leaseManager!.consumeRequest()) {
          completer.completeError(RSocketException(
              RSocketErrorCode.REJECTED, 'Lease exhausted or expired'));
          return completer.future;
        }
      }

      var streamId = streamIdSupplier.nextStreamId(senders)!;
      connection
          .write(FrameCodec.encodeRequestResponseFrame(streamId, payload!));
      senders[streamId] = CompleterSubscriber(completer);
      return completer.future;
    };
    //RSocket fireAndForget
    fireAndForget = (payload) {
      // Check lease if enabled
      if (leaseEnabled && mode == 'requester') {
        if (leaseManager == null || !leaseManager!.consumeRequest()) {
          return Future.error(RSocketException(
              RSocketErrorCode.REJECTED, 'Lease exhausted or expired'));
        }
      }

      var streamId = streamIdSupplier.nextStreamId(senders)!;
      connection.write(FrameCodec.encodeFireAndForgetFrame(streamId, payload!));
      return Future.value(() {});
    };
    //RSocket requestStream
    requestStream = (payload, {int initialRequestN = MAX_REQUEST_N_SIZE}) {
      // Check lease if enabled
      if (leaseEnabled && mode == 'requester') {
        if (leaseManager == null || !leaseManager!.consumeRequest()) {
          return Stream.error(RSocketException(
              RSocketErrorCode.REJECTED, 'Lease exhausted or expired'));
        }
      }

      var streamId = streamIdSupplier.nextStreamId(senders)!;
      // Send initial request with the specified or default requestN
      connection.write(FrameCodec.encodeRequestStreamFrame(
          streamId, initialRequestN, payload!));

      // Track initial demand
      _outgoingDemandTracker.addDemand(streamId, initialRequestN);

      var streamSubscriber = StreamSubscriber(
        streamId: streamId,
        connection: connection,
        demandTracker: _outgoingDemandTracker,
        requestN:
            initialRequestN == MAX_REQUEST_N_SIZE ? MAX_REQUEST_N_SIZE : 32,
        onCancel: () {
          connection.write(FrameCodec.encodeCancelFrame(streamId));
          senders.remove(streamId);
          _outgoingDemandTracker.removeStream(streamId);
        },
      );
      senders[streamId] = streamSubscriber;
      return streamSubscriber.payloadStream();
    };
    //RSocket metadataPush
    metadataPush = (payload) {
      connection.write(FrameCodec.encodeMetadataFrame(0, payload!));
      return Future.value(() {});
    };
    //RSocket requestChannel
    requestChannel = (payloads) {
      var streamId = streamIdSupplier.nextStreamId(senders)!;
      var streamSubscriber = StreamSubscriber(
        streamId: streamId,
        connection: connection,
        demandTracker: _outgoingDemandTracker,
        requestN: 32,
        onCancel: () {
          connection.write(FrameCodec.encodeCancelFrame(streamId));
          senders.remove(streamId);
          _outgoingDemandTracker.removeStream(streamId);
        },
      );
      senders[streamId] = streamSubscriber;
      
      // Listen to the input stream and send payloads
      bool firstPayload = true;
      payloads.listen((payload) {
        if (firstPayload) {
          // Send the first payload with REQUEST_CHANNEL frame
          connection.write(FrameCodec.encodeChannelFrame(
              streamId, MAX_REQUEST_N_SIZE, payload));
          firstPayload = false;
        } else {
          // Send subsequent payloads as PAYLOAD frames
          connection.write(FrameCodec.encodePayloadFrame(
              streamId, false, payload));
        }
      }, onDone: () {
        // Send completion signal
        connection.write(FrameCodec.encodePayloadFrame(
            streamId, true, null));
      }, onError: (error) {
        var rsocketError = convertToRSocketException(error);
        connection.write(FrameCodec.encodeErrorFrame(
            streamId, rsocketError.code!, rsocketError.message));
        senders.remove(streamId);
      });
      
      return streamSubscriber.payloadStream()
          .where((payload) => payload != null)
          .cast<Payload>();
    };
  }

  void sendSetupPayload() {
    connection.init();
    connection.write(setupPayloadFrame());
    if (mode == 'requester') {
      keepAliveTimer = Timer.periodic(
          Duration(seconds: connectionSetupPayload!.keepAliveInterval),
          (Timer t) {
        if (!closed) {
          connection.write(FrameCodec.encodeKeepAlive(false, 0));
        } else {
          keepAliveTimer?.cancel();
        }
      });
    }

    // Send initial lease grant if server with lease enabled
    if (mode == 'responder' && leaseEnabled && serverLeaseManager != null) {
      // Send initial lease after a short delay to ensure setup is processed
      Timer(Duration(milliseconds: 100), () {
        if (!closed) {
          grantLease();
        }
      });
    }
  }

  @override
  void close() {
    if (!closed) {
      closed = true;
      _availability = 0.0;
      keepAliveTimer?.cancel();
      leaseManager?.dispose();
      serverLeaseManager?.dispose();

      // Clear demand tracking
      _incomingDemandTracker.clear();
      _outgoingDemandTracker.clear();

      // Cancel all active streams and complete pending requests
      var activeSenders = Map<int, Subscriber>.from(senders);
      senders.clear();

      activeSenders.forEach((streamId, subscriber) {
        if (subscriber is StreamSubscriber) {
          subscriber.onError(RSocketException(
              RSocketErrorCode.CONNECTION_CLOSE, 'Connection closed'));
        } else if (subscriber is _StreamSubscription) {
          subscriber.cancel();
        } else if (subscriber is CompleterSubscriber) {
          subscriber.onError(RSocketException(
              RSocketErrorCode.CONNECTION_CLOSE, 'Connection closed'));
        }
      });

      connection.close();
    }
  }

  @override
  double availability() {
    return _availability;
  }

  /// Send a lease frame to the peer (typically used by server)
  void sendLease(int numberOfRequests, int timeToLive, {Uint8List? metadata}) {
    if (mode == 'responder' && leaseEnabled) {
      connection.write(FrameCodec.encodeLeaseFrame(timeToLive, numberOfRequests,
          metadata: metadata));
    }
  }

  /// Grant a lease using the server lease manager
  void grantLease() {
    if (mode == 'responder' && serverLeaseManager != null) {
      var leaseFrame = serverLeaseManager!.grantLease();
      connection.write(leaseFrame);
    }
  }

  void receiveChunk(Uint8List chunk) {
    if (this.chunkBuffer != null) {
      this.chunkBuffer = this.chunkBuffer! + chunk;
      var chunkDataLength = this.chunkBuffer!.length - 3;
      var bytes = this.chunkBuffer!.sublist(0, 3);
      var rsocketFrameLength = bytesToNumber(bytes)!;
      if (rsocketFrameLength <= chunkDataLength) {
        for (var frame in parseFrames(this.chunkBuffer!)) {
          receiveFrame(frame);
        }
        this.chunkBuffer = null;
      }
      return;
    }
    if (chunk.length > 3) {
      var chunkDataLength = chunk.length - 3;
      var bytes = chunk.sublist(0, 3);
      var rsocketFrameLength = bytesToNumber(bytes)!;
      if (rsocketFrameLength > chunkDataLength) {
        this.chunkBuffer = chunk;
        return;
      }
      for (var frame in parseFrames(chunk)) {
        receiveFrame(frame);
      }
    }
  }

  void receiveFrame(RSocketFrame frame) {
    var header = frame.header;
    var streamId = header.streamId;
    switch (header.type) {
      case frame_types.PAYLOAD:
        var payloadFrame = frame as PayloadFrame;
        if (senders.containsKey(streamId)) {
          var subscriber = senders[streamId];
          var payload = payloadFrame.payload;
          if (payloadFrame.completed) {
            senders.remove(streamId);
            if (payload?.data != null) {
              subscriber!.onNext(payload);
            }
            subscriber!.onComplete();
          } else {
            if (payload?.data != null) {
              subscriber!.onNext(payload);
            }
          }
        }
        break;
      case frame_types.KEEPALIVE:
        var keepAliveFrame = frame as KeepAliveFrame;
        if (keepAliveFrame.respond) {
          connection.write(FrameCodec.encodeKeepAlive(
              false, keepAliveFrame.lastReceivedPosition));
        }
        break;
      case frame_types.ERROR:
        var errorFrame = frame as ErrorFrame;
        var streamId = header.streamId;
        var error = RSocketException(errorFrame.code, errorFrame.message);
        if (streamId == 0 && errorConsumer != null) {
          errorConsumer!(error);
        } else {
          if (senders.containsKey(streamId)) {
            var subscriber = senders[streamId]!;
            senders.remove(streamId);
            subscriber.onError(error);
          }
        }
        break;
      case frame_types.CANCEL:
        var streamId = header.streamId;
        if (senders.containsKey(streamId)) {
          var subscriber = senders[streamId]!;
          senders.remove(streamId);

          // Handle different types of subscribers
          if (subscriber is StreamSubscriber) {
            // For client-side streams, complete the stream
            subscriber.onComplete();
          } else if (subscriber is _StreamSubscription) {
            // For server-side streams, cancel the subscription
            subscriber.cancel();
          } else if (subscriber is CompleterSubscriber) {
            // For request-response, complete with error
            subscriber.onError(RSocketException(
                RSocketErrorCode.CANCELED, 'Request cancelled by remote'));
          }
        }
        break;
      case frame_types.REQUEST_RESPONSE:
        var requestResponseFrame = frame as RequestResponseFrame;
        if (responder != null && requestResponseFrame.payload != null) {
          responder!.requestResponse!(requestResponseFrame.payload).then((payload) {
            connection.write(
                FrameCodec.encodePayloadFrame(header.streamId, true, payload));
          }).catchError((error) {
            var rsocketError = convertToRSocketException(error);
            connection.write(FrameCodec.encodeErrorFrame(
                header.streamId, rsocketError.code!, rsocketError.message));
          });
        }
        break;
      case frame_types.REQUEST_FNF:
        var fireAndForgetFrame = frame as RequestFNFFrame;
        if (responder != null && fireAndForgetFrame.payload != null) {
          responder!.fireAndForget!(fireAndForgetFrame.payload)
              .then((value) => {});
        }
        break;
      case frame_types.METADATA_PUSH:
        var metadataPushFrame = frame as MetadataPushFrame;
        if (responder != null && metadataPushFrame.payload != null) {
          responder!.metadataPush!(metadataPushFrame.payload)
              .then((value) => {});
        }
        break;
      case frame_types.REQUEST_STREAM:
        var requestStreamFrame = frame as RequestStreamFrame;
        var requesterStreamId = header.streamId;

        // Track initial demand from the request
        _incomingDemandTracker.addDemand(requesterStreamId,
            requestStreamFrame.initialRequestN ?? MAX_REQUEST_N_SIZE);

        if (responder != null && requestStreamFrame.payload != null) {
          var subscription = responder!.requestStream!
                  (requestStreamFrame.payload)
              .listen((payload) {
            // Check demand before sending
            if (_incomingDemandTracker.consumeDemand(requesterStreamId)) {
              connection.write(FrameCodec.encodePayloadFrame(
                  requesterStreamId, false, payload));
            } else {
              // TODO: Buffer or drop based on QoS policy
              // For now, we'll drop frames when there's no demand
              print(
                  'Warning: Dropping frame due to lack of demand on stream $requesterStreamId');
            }
          }, onDone: () {
            connection.write(
                FrameCodec.encodePayloadFrame(requesterStreamId, true, null));
            senders.remove(requesterStreamId);
            _incomingDemandTracker.removeStream(requesterStreamId);
          }, onError: (Object error) {
            senders.remove(requesterStreamId);
            _incomingDemandTracker.removeStream(requesterStreamId);
            if (error is RSocketException) {
              var e = error;
              connection.write(FrameCodec.encodeErrorFrame(
                  requesterStreamId, e.code!, e.message));
            } else {
              connection.write(FrameCodec.encodeErrorFrame(requesterStreamId,
                  RSocketErrorCode.APPLICATION_ERROR, error.toString()));
            }
          });
          // Store the subscription so it can be cancelled
          senders[requesterStreamId] = _StreamSubscription(subscription);
        }
        break;
      case frame_types.REQUEST_N:
        var requestNFrame = frame as RequestNFrame;
        var streamId = header.streamId;

        // Add demand for the stream
        if (requestNFrame.initialRequestN != null &&
            requestNFrame.initialRequestN! > 0) {
          _incomingDemandTracker.addDemand(
              streamId, requestNFrame.initialRequestN!);
        }
        break;
      case frame_types.LEASE:
        var leaseFrame = frame as LeaseFrame;
        if (mode == 'requester' && leaseManager != null) {
          // Client receives lease grant from server
          leaseManager!
              .updateLease(leaseFrame.numberOfRequests, leaseFrame.timeToLive);
          _availability = 1.0; // Restore availability when lease is granted
        } else if (mode == 'responder' && serverLeaseManager != null) {
          // Server can receive lease frames in bidirectional scenarios
          // This is less common but supported by the protocol
        }
        break;
      case frame_types.REQUEST_CHANNEL:
        var requestChannelFrame = frame as RequestChannelFrame;
        var requesterStreamId = header.streamId;
        if (responder != null) {
          // Create a stream controller for sending payloads to responder
          var inputController = StreamController<Payload>();
          
          // Add the first payload from the frame
          if (requestChannelFrame.payload != null) {
            inputController.add(requestChannelFrame.payload!);
          }
          
          // Store the controller so we can send more payloads when they arrive
          var channelSubscriber = _ChannelSubscriber(inputController);
          senders[requesterStreamId] = channelSubscriber;
          
          // Call the responder's requestChannel handler
          var subscription = responder!.requestChannel!(inputController.stream).listen(
              (payload) {
            connection.write(FrameCodec.encodePayloadFrame(
                requesterStreamId, false, payload));
          }, onDone: () {
            connection.write(
                FrameCodec.encodePayloadFrame(requesterStreamId, true, null));
            senders.remove(requesterStreamId);
            inputController.close();
          }, onError: (Object error) {
            if (error is RSocketException) {
              var e = error;
              connection.write(FrameCodec.encodeErrorFrame(
                  requesterStreamId, e.code!, e.message));
            } else {
              connection.write(FrameCodec.encodeErrorFrame(requesterStreamId,
                  RSocketErrorCode.APPLICATION_ERROR, error.toString()));
            }
            senders.remove(requesterStreamId);
            inputController.close();
          });
          
          // Store the subscription so it can be cancelled
          channelSubscriber.subscription = subscription;
        }
        break;
      default:
    }
  }

  Uint8List setupPayloadFrame() {
    return FrameCodec.encodeSetupFrame(
        connectionSetupPayload!.keepAliveInterval,
        connectionSetupPayload!.keepAliveMaxLifetime,
        connectionSetupPayload!.metadataMimeType,
        connectionSetupPayload!.dataMimeType,
        connectionSetupPayload,
        leaseEnable: leaseEnabled);
  }
}

RSocketException convertToRSocketException(dynamic e) {
  if (e == null) {
    return RSocketException(RSocketErrorCode.APPLICATION_ERROR, 'Error');
  } else if (e is RSocketException) {
    return e;
  } else {
    return RSocketException(RSocketErrorCode.APPLICATION_ERROR, e.toString());
  }
}

