from factory import Sequence, Faker
from factory.django import DjangoModelFactory

from hotel import models


class HotelFactory(DjangoModelFactory):
    name = Sequence(lambda n: "Hotel %d" % (n + 1))
    city = Faker("city")
    pms_hotel_id = "851df8c8-90f2-4c4a-8e01-a4fc46b25178"
                    
    class Meta:
        model = models.Hotel
