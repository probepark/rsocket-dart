import '../payload.dart';
import '../rsocket.dart';
import 'rsocket_requester.dart';

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

  /// Request stream with configurable initial request N
  Stream<Payload?> requestStreamWithRequestN(Payload? payload,
      {int? initialRequestN}) {
    // If delegate is RSocketRequester, cast to access the extended method
    if (_delegate is RSocketRequester) {
      final requester = _delegate as RSocketRequester;
      // Call the requester's method directly with the typed parameters
      return (requester.requestStream! as Stream<Payload?> Function(Payload?,
              {int initialRequestN}))(payload,
          initialRequestN: initialRequestN ?? defaultRequestN);
    }
    // Otherwise fall back to regular requestStream
    return _delegate.requestStream!(payload);
  }

  @override
  RequestStream? get requestStream => _delegate.requestStream;

  @override
  void close() => _delegate.close();

  @override
  double availability() => _delegate.availability();
}
