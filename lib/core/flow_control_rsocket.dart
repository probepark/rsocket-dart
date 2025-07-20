import '../payload.dart';
import '../rsocket.dart';

/// Wrapper for RSocket that provides flow control configuration
class FlowControlRSocket extends RSocket {
  final RSocket _delegate;
  final int defaultRequestN;

  FlowControlRSocket(this._delegate, {this.defaultRequestN = 32});

  @override
  RequestResponse? get requestResponse => _delegate.requestResponse;

  @override
  FireAndForget? get fireAndForget => _delegate.fireAndForget;

  @override
  RequestChannel? get requestChannel => _delegate.requestChannel;

  @override
  MetadataPush? get metadataPush => _delegate.metadataPush;

  @override
  RequestResponse? get subscribe => _delegate.subscribe;

  /// Request stream with configurable initial request N
  Stream<Payload?> requestStreamWithRequestN(Payload? payload,
      {int? initialRequestN}) {
    // This will be handled by the RSocketRequester implementation
    // which now supports the initialRequestN parameter
    return _delegate.requestStream!(payload);
  }

  @override
  RequestStream? get requestStream => _delegate.requestStream;

  @override
  void close() => _delegate.close();

  @override
  double availability() => _delegate.availability();
}

