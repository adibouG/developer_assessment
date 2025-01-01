from abc import ABC, abstractmethod
import inspect
import sys
import re
from json import loads, JSONDecodeError
from uuid import UUID
from time import sleep
from datetime import datetime


from typing import Any, Callable, Optional, Type, TypedDict

from hotel.external_api import (
    get_reservations_for_given_checkin_date,
    get_reservation_details,
    get_guest_details,
    APIError,
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
        if payload is None or len(payload) <= 0:
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
        DUPLICATE_PHONE_SUFFIX: str = "_-1_"
        #_____________// 
        try:
            webhook_data = CleanedWebhookPayload(webhook_data)
            hotelId: int = webhook_data.get("hotel_id")
            data: dict = webhook_data.get("data")
            pmsHotelId: UUID = UUID(data.get("HotelId"))
            integrationId: UUID = UUID(data.get("IntegrationId"))
            dataEvents: list = data.get("Events")
            reservationUpdatesList: list = []
            # check if the hotel is the right one
            if hotelId != self.hotel.id:
                return False

            #_______________//Start of fetch reservations details___________
            # get the reservation updates details as a list
            # get the reservation details, we could setup a specific pms method returning a list to get reservation details, but this is just an example,       
            for event in dataEvents:
                reservationEvent: dict = event
                eventType: str = reservationEvent.get("Name")
                eventValue: dict = reservationEvent.get("Value")
                if eventType != "ReservationUpdated":
                    continue
                reservationId: str = eventValue.get("ReservationId")  
                if reservationId is None or len(reservationId) == 0:
                    continue                  
                # get the reservation details 
                # API call use additional wrapper function named api_call_retry 
                # it's defined out of the scope of this PMS class, see below 
                try:
                    reservationDetails: str =  api_call_retry(get_reservation_details, reservationId, RETRY, WAIT) 
                    if reservationDetails is not None and len(reservationDetails) > 2 :
                        print(reservationDetails)
                        # the data received is from the pms for a specific hotel so this is just a check just in case 
                        if reservationDetails.find(f'"HotelId": "{pmsHotelId}"') != -1:  
                            reservationUpdatesList.append(loads(reservationDetails))
                except APIError as e:
                    print(f"APIError on reservation: {reservationId}\nError: {e}")
                        
                continue
            # if no reservation updates, we can return
            if len(reservationUpdatesList) == 0:
                return True
            #_______________//End of fetch reservations details___________

            #_______________//Start update guests and stays___________
            
            # now we have the list of reservation to creates or updates, we can get the stay and guest details  
            # mapping of the pms statuses to our supported statuses
            pmsStayStatusMap: dict = { 
                "not_confirmed": Stay.Status.UNKNOWN, 
                "booked": Stay.Status.BEFORE, 
                "in_house": Stay.Status.INSTAY, 
                "checked_out": Stay.Status.AFTER,
                "cancelled": Stay.Status.CANCEL,
                "no_show": Stay.Status.UNKNOWN
            }
            # loop over the reservation updates list and update or create the stays and guests
            for reservationData in reservationUpdatesList:
                print(reservationData)
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
                    if guestLang is not None and len(guestLang) > 0 and  guestLang.lower() != "null" and \
                      guestLang.lower() in Language.values:
                        guestLang = Language(guestLang.lower())
                    else:
                        guestLang = None
                    
                    if DO_PHONE_CHECK : 
                        phoneReg = "^\\+?\\d{1,4}?[-.\\s]?\\(?\\d{1,3}?\\)?[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,9}$"
                    else:
                        phoneReg = "^.{0,200}$"
                    
                    if guestPhone is None or re.fullmatch(phoneReg, guestPhone) is None:
                        guestPhone = ""
                    
                    if guestName is None :
                        guestName = ""
                    
                    # update or create the guest, Note that storing an email in addition to the phone would help to identify the guest 
                    guestUpdate: dict = { "name": guestName, "phone": guestPhone, "language": guestLang }
                    print (guestUpdate)
                    
                    # if 2 different guest have the same phone number, we should update the guest or the phone number
                    # but updating the guest might fail with the test cases, 
                    # so we update the phone number with a suffixe and create or update this new guest
                    # this could be done in separate function handling the case of phone number update and guest update
                    doGuestUpdate = True
                    guestExist = Guest.objects.filter(phone=guestPhone).first()
                    if guestExist is not None:
                        if (guestExist.name.strip().lower() != guestName.strip().lower() or \
                            guestExist.language is None or guestLang is None or guestExist.language != guestLang):
                            # same phone but  name  or country might differ, so we can't confirm same guest, an email would be nice
                            guestPhone = guestPhone + DUPLICATE_PHONE_SUFFIX
                            # either :
                                # create a new guest to prevent issues with the phone number modified with the suffixe DUPLICATE_PHONE_SUFFIX
                            guestUpdate["phone"] = guestPhone
                                # or update the existing guest with the modified phone number 
                            # guestExist.phone = guestPhone 
                            # guestExist.save()
                                #  we could fine tune this decision by checking guest creation and update dates and reservation status to decide which one to update example
                                # if we have the country we could also decide to add the country code as a PREFIX to the phone number to make it unique 
                        else:
                            # else the name and country are the same so we do nothing
                            doGuestUpdate = False
                            
                    # update the guest details
                    # we still call update_or_create as the modified number might be the same as another guest number,
                    # but we can't go adding duplicate prefixes so just update the guest with the same prefixed duplicate phone number
                    # and otherwise create a new guest with the modified phone number         
                    if doGuestUpdate is True:     
                        guest, _= Guest.objects.update_or_create(phone=guestPhone, defaults=guestUpdate)
                        print(f"guest model with id {guest.id} processed")  
                            
                except APIError as e:
                    print(f"APIError on guest: {pmsGuestId}\nError: {e}")
                    #continue # No guest details should we move to the next reservation?
                
                # Now we are sure to have the Guest in our db or an updated guest details  
                
                # stay update    
                # we can update or create the stays accordingly:
                
                # NOTE:
                #   - The only use case I see for using get_reservations_for_given_checkin_date is to notify customers about it 
                #   or for the edge case of getting the reservations that were booked prior to the pms and hotel 
                #   integration, and reservationss that didn't get updated since the integration is working
                #   As a result, we can get guest request for an existing reservation which is not in our system, 
                #   This might mostly happen with new integrations, but can happen, is it acceptable ? 
                #       Handling these case would mean get and check all the coming pms reservations of the hotels
                #       get the missing ones and add these to the db. 
                #       Not very likely doable in 1 go for all hotels pms for all future dates 
                #       A compromise could be to set up a time offset value like COMING_DAYS_CHECK = 15 
                #       and every night run the method get_reservations_for_given_checkin_date over the define time offset
                #       hotel per hotel or pms per pms, compare our db data with the pms data and add the missing reservations in our db
                #       
                # This could be managed in a cron job to run every night, or another process manager or scheduler 
                # However this has to be implemented in another method or function in order to handle this case
                
                #   for now we can assume we are up-to-date with the reservation updates from the pms we just received
                #   and can update our db stay details accordingly , 
                
                # we could setup a specific pms method to  update the stay details, but this is just an example, 
                # update or create the stay details, no need to check stayId as it comes directly from the Pms
                try:     
                    pmsStayStatus: Stay.Status = pmsStayStatusMap.get(reservationData.get("Status").lower(), Stay.Status.UNKNOWN)  
                    stayStatus: str = Stay.Status(pmsStayStatus)
                    print(stayStatus)

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
            #_______________//End update guests and stays________
            return True

        except Exception as e:
            print(f"Error: {e}")
            return False


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


