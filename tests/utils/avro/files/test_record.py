""" tests helper avro reader functions """

import unittest
from avro_to_python.utils.avro.files.record import _prepare_field_for_reference


class UtilsAvroFilesTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_prepare_field_for_reference(self):
        """ tests the _prepare_field_for_reference function """
        test_cases = [
            {'in': {'name': 'ContentGroup', 'type': 'storyblocks.content.records'},
             'expected': {'name': 'ContentGroup', 'type': 'storyblocks.content.records'}},
            {'in': {'name': 'carIdentifiers',
                    'type': 'storyblocks.search.records.CrossAssetRecommendationIdentifiers'},
             'expected': {'name': 'CrossAssetRecommendationIdentifiers',
                          'type': 'storyblocks.search.records'}},
        ]

        for test_case in test_cases:
            self.assertEqual(first=_prepare_field_for_reference(field=test_case['in']),
                             second=test_case['expected'],
                             msg="_prepare_field_for_reference returned the wrong result")
