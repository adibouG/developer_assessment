import django.test

from hotel.models import Stay, Hotel, Guest

from hotel.tests import load_api_fixture
from hotel.tests.factories import HotelFactory


from hotel.pms_systems import CleanedWebhookPayload


class PMS_Apaleotest(django.test.TestCase):
    def setUp(self) -> None:
        self.hotel = HotelFactory(pms=Hotel.PMS.APALEO)
        self.pms = self.hotel.get_pms()

    def test_clean_webhook_payload_faulty(self):
        cleaned_payload = self.pms.clean_webhook_payload(load_api_fixture("webhook_payload_faulty.json"))
        self.assertIsNone(cleaned_payload)


    def test_clean_webhook_payload(self):
        cleaned_payload = self.pms.clean_webhook_payload(load_api_fixture("webhook_payload.json"))
        if not cleaned_payload:
            self.fail("No cleaned payload returned")
        else:
            # MODIFIED: CleanedWebhookPayload to dict in assertIsInstance method arguments 
            # Python3.11 (didn't try with other versions) isInstance seems to not work with TypedDict (seems like the mapping to dict is not done by the interpreter anymore), 
            self.assertIsInstance(cleaned_payload, dict) #CleanedWebhookPayload) 
            # added 2 manual check for correct dict value types, 
            # see NOTE in the end of the file for more details about the error raised  when running the tests
            self.assertIn('hotel_id', cleaned_payload)
            self.assertIn('data', cleaned_payload)
            #_______//END OF ADDITION
            self.assertEqual(cleaned_payload["hotel_id"], self.hotel.id)
            self.assertIsInstance(cleaned_payload["data"], dict)

    def test_handle_webhook(self):
        cleaned_payload = self.pms.clean_webhook_payload(load_api_fixture("webhook_payload.json"))
        success = self.pms.handle_webhook(cleaned_payload)
        self.assertTrue(success)
        stays = Stay.objects.filter(hotel=self.hotel)
        self.assertEqual(stays.count(), 3)
        guests = Guest.objects.all()
        self.assertEqual(guests.count(), 3)


'''
NOTE:

ERROR: test_clean_webhook_payload (hotel.tests.tests.PMS_Apaleotest.test_clean_webhook_payload)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "/home/ad/Public/HDD_ext4/Dev2/Python/developer_assessment/hotel/tests/tests.py", line 25, in test_clean_webhook_payload
    self.assertIsInstance(cleaned_payload, CleanedWebhookPayload)
  File "/usr/lib/python3.11/unittest/case.py", line 1294, in assertIsInstance
    if not isinstance(obj, cls):
           ^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib/python3.11/typing.py", line 2957, in __subclasscheck__
    raise TypeError('TypedDict does not support instance and class checks')
TypeError: TypedDict does not support instance and class checks
'''