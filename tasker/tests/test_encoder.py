import unittest

from .. import encoder


class EncoderTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_compressors(self):
        data = b'a' * 1000

        dummy = encoder.compressor.dummy.Compressor()
        compressed = dummy.compress(data)
        self.assertEqual(compressed, data)
        decompressed = dummy.decompress(compressed)
        self.assertEqual(decompressed, data)

        compressors = [
            encoder.compressor.bzip2.Compressor(),
            encoder.compressor.gzip.Compressor(),
            encoder.compressor.lzma.Compressor(),
            encoder.compressor.zlib.Compressor(),
        ]
        for compressor in compressors:
            compressed = compressor.compress(data)
            self.assertLess(
                len(compressed),
                len(data),
            )
            decompressed = compressor.decompress(compressed)
            self.assertEqual(decompressed, data)

    def test_serializers(self):
        data = {
            'a': 1,
            'b': [1, 2, 3, 4],
            'c': {
                '1': {
                    'a': 1,
                    'b': 2,
                }
            }
        }

        serializers = [
            encoder.serializer.msgpack.Serializer(),
            encoder.serializer.pickle.Serializer(),
        ]
        for serializer in serializers:
            serialized = serializer.serialize(data)
            self.assertEqual(type(serialized), bytes)
            self.assertNotEqual(serialized, data)
            deserialized = serializer.unserialize(serialized)
            self.assertEqual(deserialized, data)

    def test_encoder(self):
        data = {
            'a': 1,
            'b': [1, 2, 3, 4],
            'c': {
                '1': {
                    'a': 1,
                    'b': 2,
                }
            }
        }

        compressor_names = [
            compressor_name
            for compressor_name in encoder.compressor.__compressors__
        ]
        serializer_names = [
            serializer_name
            for serializer_name in encoder.serializer.__serializers__
        ]
        for compressor_name in compressor_names:
            for serializer_name in serializer_names:
                encoder_obj = encoder.encoder.Encoder(
                    compressor_name=compressor_name,
                    serializer_name=serializer_name,
                )

                encoded = encoder_obj.encode(data=data)
                decoded = encoder_obj.decode(data=encoded)

                self.assertEqual(decoded, data)
