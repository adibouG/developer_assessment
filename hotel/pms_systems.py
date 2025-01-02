from abc import ABC, abstractmethod
import inspect
import sys
import re
from json import loads, JSONDecodeError
from uuid import UUID
from time import sleep
from datetime import datetime


from typing import Any, Tuple, Callable, Optional, Type, TypedDict

from hotel.external_api import (
    get_reservations_for_given_checkin_date,
    get_reservation_details,
    get_guest_details,
    APIError
)

from hotel.models import Stay, Hotel, Guest, Language


class CleanedWebhookPayload(TypedDict):
    hotel_id: int
    data: dict


class PMS(ABC):
    """
    Abstract class for Property Management Systems.
    """

    def __init__(self, hotel: Hotel):
        assert hotel is not None

        self.hotel = hotel

    @property
    def name(self):
        longname = self.__class__.__name__
        return longname[4:]

    @classmethod
    def clean_webhook_payload(cls, payload: str) -> Optional[CleanedWebhookPayload]:
        """
        This method returns a CleanedWebhookPayload object containing a hotel_id from the payload and the data as a dict in the data field
        It should return None if the payload is invalid or the hotel is not found.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_webhook(self, webhook_data: dict) -> bool:
        """
        This method is called when we receive a webhook from the PMS.
        Handle webhook handles the events and updates relevant models in the database.
        Requirements:
            - Now that the PMS has notified you about an update of a reservation, you need to
                get more details of this reservation. For this, you can use the mock API
                call get_reservation_details(reservation_id).
            - Handle the payload for the correct hotel.
            - Update or create a Stay.
            - Update or create Guest details.
        """
        raise NotImplementedError


class PMS_Apaleo(PMS):
    @classmethod
    def clean_webhook_payload(cls, payload: str) -> Optional[CleanedWebhookPayload]:
        # check for valid payload
        if payload is None or len(payload) <= 2:
            return None
        if "HotelId" not in payload:
            return None
        try:
            # get the hotel
            load = loads(payload)
            pms_hotelId: str = load.get("HotelId")
            pmsHotelUuid = UUID(pms_hotelId) 
            hotelId: int = Hotel.objects.filter(pms_hotel_id=pmsHotelUuid).first().id
            # rebuild the payload as typed data structure
            cleanedPayload: CleanedWebhookPayload = { 'hotel_id': hotelId, 'data': load }
            return cleanedPayload
        except (JSONDecodeError, KeyError, ValueError, Hotel.DoesNotExist, TypeError, Exception): 
            return None
  

    def handle_webhook(self, webhook_data: dict) -> bool:
        # check for valid payload
        if webhook_data is None:
            return False
        #_____________//PARAMETERS SETTINGS //_____________# 
        # API calls RETRY and WAIT parameters settings, 
        RETRY: int = 3
        WAIT: int = 1 
        # Phone number validation check
        DO_PHONE_CHECK: bool = False
        #_____________// 
        try:
            webhook_data = CleanedWebhookPayload(webhook_data)
            hotelId: int = webhook_data.get("hotel_id")
            # check if the hotel is the right one
            if hotelId != self.hotel.id:
                return False
            
            data: dict = webhook_data.get("data")
            pmsHotelId: UUID = UUID(data.get("HotelId"))
            dataEvents: list = data.get("Events")
            
            #_______________//Start of fetch reservations details___________
            # get the reservation updates details as a list
            # get the reservation details, we could setup a specific pms method returning a list to get reservation details, but this is just an example,       
            reservationUpdatesList: list = []
            for event in dataEvents:
                eventValue: dict = event.get("Value")
                if event.get("Name") == "ReservationUpdated":
                    reservationId: str = eventValue.get("ReservationId")  
                    if reservationId is None or len(reservationId) == 0:
                        continue                  
                    # get the reservation details 
                    # the API calls are wrapped in a function named api_call_retry 
                    # it's defined out of the scope of this  class, see below 
                    try:
                        reservationDetails: str =  api_call_retry(get_reservation_details, reservationId, RETRY, WAIT) 
                        if reservationDetails is not None and len(reservationDetails) > 2 :
                        # the data received is from the pms for a specific hotel so this is just a check just in case 
                            if reservationDetails.find(f'"HotelId": "{pmsHotelId}"') != -1:  
                                reservationUpdatesList.append(loads(reservationDetails))
                    except APIError as e:
                        print(f"APIError on reservation: {reservationId}\nError: {e}")
            #_______________//for loop end___________          
                
            # if no reservation updates, we can return
            if len(reservationUpdatesList) == 0:
                return True
            #_______________//End of fetch reservations details___________

            
            #_______________//Start update guests and stays___________
            if DO_PHONE_CHECK : 
                phoneReg = "^\\+?\\d{1,4}?[-.\\s]?\\(?\\d{1,3}?\\)?[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,9}$"
            else:
                phoneReg = "^.{0,200}$"
        
            # now we have the list of reservation to creates or updates, we can get the stay and guest details  
            # loop over the reservation update list and update or create the stays and guests
            for reservationData in reservationUpdatesList:
    
                try:
                    pmsReservationId: str = reservationData.get("ReservationId")
                    # First: get the guest details, we could setup a specific pms method to get and update the guest details, but this is just an example, 
                    # no need to check guestId as it comes directly from the Pms
                    guestDetails: str = api_call_retry(get_guest_details, reservationData.get("GuestId"), RETRY, WAIT)
                    guestObj = loads(guestDetails)
                                    
                    # get the guest phone as this is our system unique identifier and other details
                    pmsGuestId: str = guestObj.get("GuestId")
                    guestPhone: str = guestObj.get("Phone")
                    guestName: str = guestObj.get("Name")
                    guestLang: str = guestObj.get("Country")
                    
                    if guestLang is not None and len(guestLang) > 0 and \
                        guestLang.lower() != "null" and  \
                            guestLang.lower() in Language.values:
                        guestLang = Language(guestLang.lower())
                    else:
                        guestLang = None
                    
                    if guestPhone is None or re.fullmatch(phoneReg, guestPhone) is None:
                        guestPhone = ""
                    
                    if guestName is None :
                        guestName = ""
                    
                    # update or create the guest, Note that storing an email in addition to the phone would help to identify the guest 
                    # check if the phone already exists in our db and update or create the guest accordingly
                    guest, _ = check_and_resolve_phone_number(guestPhone, guestName, guestLang)
                             
                except APIError as e:
                    print(f"APIError on guest: {pmsGuestId}\nError: {e}")
                    #continue # No guest details should we move to the next reservation?
                
                # Now we are sure to have the Guest in our db or an updated guest details  
                
                # stay update    
                # we can update or create the stays accordingly:
                #   for now we can assume we are up-to-date with the reservation updates from the pms we just received
                #   and can update our db stay details accordingly , 
                
                # we could setup a specific pms method to update the stay details, but this is just an example, 
                # update or create the stay details, no need to check stayId as it comes directly from the Pms
                try:     
                    pmsStayStatus: Stay.Status = PMS_Apaleo.pmsStayStatusMap.get(reservationData.get("Status").lower(), Stay.Status.UNKNOWN)  
                    stayStatus: str = Stay.Status(pmsStayStatus)
             
                    stayUpdate: Stay = { 
                        'hotel': self.hotel,
                        'guest': guest, 
                        'pms_reservation_id': pmsReservationId, 
                        'pms_guest_id': pmsGuestId,
                        'status': stayStatus,
                        'checkin': datetime.strptime(reservationData.get("CheckInDate"), "%Y-%m-%d").date(),
                        'checkout': datetime.strptime(reservationData.get("CheckOutDate"), "%Y-%m-%d").date(),
                    } 
             
                    stay, created = Stay.objects.update_or_create(pms_reservation_id=pmsReservationId, defaults=stayUpdate)
                    print(f"stay model with id {stay.id} for reservation {pmsReservationId} processed ") # this will print 'created' or 'updated: {pmsStayId}\nError: {e}")
                except Exception as e:
                    print(f"Error on stay update: {pmsReservationId}\nError: {e}")
                    continue
                #_______________//for loop end___________
            #_______________//End update guests and stays________
            return True

        except Exception as e:
            print(f"Error: {e}")
            return False

    '''
     NOTE:
    - The only use case I see for using get_reservations_for_given_checkin_date is to notify customers about it 
    or for the edge case of getting the reservations that were booked prior to the pms and hotel 
    integration, and reservations that didn't get updated since the integration is working
    As a result, we can get guest request for an existing reservation which is not in our system, 
    This might mostly happen with new integrations,
    Handling these case would mean get and check all the coming pms reservations of the hotels
    and get the missing ones to add these to the db. 
    Not very funny for all hotels / pms , for all the future dates 
    A compromise could be to set up a time offset value like COMING_DAYS_CHECK = 15 
    and every night run the method get_reservations_for_given_checkin_date over the define time offset
    for each integration, compare our db data with the pms and add the missing reservations to our db.
    However, I don' t see how this relates to clean_payload or handle_webhook methods, so I didn't implement it
    '''
                

    # mapping of the pms statuses to our supported statuses
    
    pmsStayStatusMap: dict = { 
       "not_confirmed": Stay.Status.UNKNOWN, 
        "booked": Stay.Status.BEFORE, 
        "in_house": Stay.Status.INSTAY, 
        "checked_out": Stay.Status.AFTER,
        "cancelled": Stay.Status.CANCEL,
        "no_show": Stay.Status.UNKNOWN
    }

# ________// End PMS_Apaleo class

def get_pms(name: str) -> Type[PMS]:
    """
    This function returns the PMS class for the given name.
    This does not return an instance of the class, but the class itself.
    Note, that the name should be the same as the class name without the 'PMS_' prefix.
    """
    fullname = "PMS_" + name.capitalize()
    # find all class names in this module
    # from https://stackoverflow.com/questions/1796180/
    current_module = sys.modules[__name__]
    clsnames = [x[0] for x in inspect.getmembers(current_module, inspect.isclass)]

    # if we have a PMS class for the given name, return an instance of it
    if fullname in clsnames:
        return getattr(current_module, fullname)
    else:
        raise ValueError(f"No PMS class found for {name}")



# Additional function to handle api call with failing case via retry and sleep parameters... could be an async one       
def api_call_retry(func: Callable[[Any], Any], param: Any, retry: int = 1, wait: int = 1) -> dict:
    res = None
    try:
        res =  func(param)
    except APIError as e:
        print(f"APIError: {e}, retry left: {retry}")
        sleep(wait)
        if retry > 0:
            retry -= 1
            res = api_call_retry(func, param, retry, wait)
        else:
            raise e
    return res

# Addtional function to handle the phone number issues in our db
# Return the created Guest and the operation result as a tuple
# Note that the overhead is because the test can fail with the update of a guest a
# so we create a new guest with a modified phone number or update the existing guest with the modified phone number
def check_and_resolve_phone_number(guestPhone: str, guestName: str, guestLang: str, guestId: int = None) ->  Optional[Tuple[Guest, int]]:
    
    # ______________//PARAMETERS SETTINGS //_____________#
    DUPLICATE_PHONE_SUFFIX: str = "_-1"
    # this is the suffix that will be added to the phone number to make it unique if needed
    UPDATE_MODES  = { "create": 1, "update": 2 } 
    # "create" : use the suffixe to create the new guest in our db 
    # "update": use the suffixe to update the existing guest in our db and create a new guest with the original phone number
    MODE = UPDATE_MODES["update"]
    #this is the mode that makes more sense to me, updating older guest details. but this is an opinion 
    # best would be to fine tune it based on the guest last updates, reservation status, and future reservations 
    # as oldest guest details might not be of use or valid, but this is an assumption, the same assert can be true for  newest guest
    RESULTS = { "none": 0, "create": 1, "update": 2 } 
    #_____________//
    
    try:
        # check if the phone already exists in our db
        dbGuest = Guest.objects.filter(phone=guestPhone).first()
        # _____//case: phone number already in our db
        if dbGuest is not None:
            # if we can't confirm that this is the same guest
            # we update a phone number with a suffixe and then create or update one of the two guest with it
            
            # _____//case: different guest 
            if dbGuest.name.strip().lower() != guestName.strip().lower() or \
                dbGuest.language != guestLang:
                # same phone but  name  or country differ, so we can't confirm this is the same guest, an email would be nice
                # modify the number
                duplicatePhone: str = guestPhone + DUPLICATE_PHONE_SUFFIX
                # then, either :
                    # create a new guest  with a phone number modified with the DUPLICATE_PHONE_SUFFIX
                    # or update the  guest already in our db with the modified phone number
                    # however we must check that the modified phone is not already in our db too
                if MODE == UPDATE_MODES["create"]: 
                    return check_and_resolve_phone_number(duplicatePhone, guestName, guestLang) # check and create new guest with the modified phone number
                else: 
                    return check_and_resolve_phone_number(duplicatePhone, guestName, guestLang, dbGuest.id)  # check andupdate the existing guest in our db with the modified phone number and add a new guest with the original phone number
            #___// case: same guest with same phone number
            else:
                return dbGuest, RESULTS["none"]  # name and phone and country are the same so we do nothing
        #___//end case phone number already in our db
        
        # _____//case: phone number not in our db
            # ______//case Update the old guest with the modified phone number not in our db
        elif  MODE == UPDATE_MODES["update"] and guestId is not None:
            # update the existing guest in our db with the modified phone number
            Guest.objects.filter(id=guestId).update(phone=guestPhone)
            # create a new guest to prevent issues with the original phone number  
            guest = Guest.objects.create(name=guestName, phone=guestPhone.replace(DUPLICATE_PHONE_SUFFIX, ""), language=guestLang)
            print(f"guest model with id {guest.id} processed and  phone duplicate added to Guest with id {guestId}")  
            return guest, RESULTS["update"]
        
            # ______//case phone number not in our db: create a new guest
        else: 
            guest = Guest.objects.create(name=guestName, phone=guestPhone, language=guestLang)
            print(f"guest model with id {guest.id} processed")  
            return guest, RESULTS["create"]

    except Exception as e:
        print(f"Error: {e}")
        return None, RESULTS["none"]