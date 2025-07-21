import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:rsocket/payload.dart';
import 'package:rsocket/frame/frame.dart';

void main() {
  test('REQUEST_CHANNEL frame encoding and decoding', () {
    // Create a payload
    var payload = Payload()..data = Uint8List.fromList('Hello'.codeUnits);

    // Encode the frame
    var encoded = FrameCodec.encodeChannelFrame(1, 100, payload);

    print('Encoded frame length: ${encoded.length}');
    print('Encoded bytes: $encoded');

    // Decode the frame
    var frames = parseFrames(encoded).toList();
    expect(frames.length, 1);

    var frame = frames.first as RequestChannelFrame;
    expect(frame.header.type, 0x07); // REQUEST_CHANNEL
    expect(frame.initialRequestN, 100);
    expect(frame.payload, isNotNull);
    expect(frame.payload!.data, isNotNull);

    var decodedMessage = String.fromCharCodes(frame.payload!.data!);
    print('Decoded message: "$decodedMessage"');
    expect(decodedMessage, 'Hello');
  });
}
