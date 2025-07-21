import 'package:test/test.dart';
import 'package:rsocket/metadata/wellknown_mimetype.dart';

void main() {
  group('WellKnownMimeType Tests', () {
    test('recognizes valid well-known MIME type IDs', () {
      // Test some known valid IDs from the MIME_TYPES map
      expect(
          WellKnownMimeType.isWellKnownTypeId(1), isTrue); // application/cbor
      expect(
          WellKnownMimeType.isWellKnownTypeId(0), isTrue); // application/avro
      expect(
          WellKnownMimeType.isWellKnownTypeId(5), isTrue); // application/json
    });

    test('rejects invalid MIME type IDs', () {
      // Test invalid/out-of-range IDs
      expect(WellKnownMimeType.isWellKnownTypeId(-1), isFalse);
      expect(
          WellKnownMimeType.isWellKnownTypeId(255), isFalse); // Likely too high
      expect(WellKnownMimeType.isWellKnownTypeId(1000),
          isFalse); // Definitely too high
    });

    test('getMimeType returns correct mime type for ID', () {
      // Test known IDs
      expect(WellKnownMimeType.getMimeType(5), equals('application/json'));
      expect(WellKnownMimeType.getMimeType(0), equals('application/avro'));
      expect(WellKnownMimeType.getMimeType(1), equals('application/cbor'));

      // Test unknown ID
      expect(WellKnownMimeType.getMimeType(255), isNull);
    });

    test('getMimeTypeId returns correct ID for mime type', () {
      // Test known mime types
      expect(WellKnownMimeType.getMimeTypeId('application/json'), equals(5));
      expect(WellKnownMimeType.getMimeTypeId('application/avro'), equals(0));
      expect(WellKnownMimeType.getMimeTypeId('application/cbor'), equals(1));

      // Test unknown mime type
      expect(WellKnownMimeType.getMimeTypeId('application/unknown'), isNull);
    });

    test('isWellKnownType recognizes mime type strings', () {
      // Test known mime types
      expect(WellKnownMimeType.isWellKnownType('application/json'), isTrue);
      expect(WellKnownMimeType.isWellKnownType('application/avro'), isTrue);
      expect(WellKnownMimeType.isWellKnownType('application/cbor'), isTrue);

      // Test unknown mime type
      expect(WellKnownMimeType.isWellKnownType('application/unknown'), isFalse);
      expect(WellKnownMimeType.isWellKnownType(null), isFalse);
    });
  });
}
