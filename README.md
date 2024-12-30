# Runnr.ai integration developer assessment
This repository contains working Django code. Run your code locally, you don't need any external services.

## Prerequisites:
- use Python version 3.11
- install dependencies by running: `pip install -r requirements.txt`

## Migrate database and run server
`python3.11 manage.py migrate`
`python3.11 manage.py runserver 0.0.0.0:8000`
`python3.11 manage.py test`

## Relevant information
- The file `views.py` contains a webhook endpoint to receive updates from the PMS. These updates don't contain any details of the actual reservations. They require you to fetch additional details of any reservation.
- The file `external_api.py` mocks API calls that are available to you to get additional guest and reservation details. Note that the API calls sometimes generate errors, or invalid data. You should deal with those in the way you see fit.
- The file `pms_systems.py` contains an AbstractBaseClass and a ChildClass of a `PMS`. You will find explanations of what all the methods do inside the methods of the ABC.
- The file `models.py` contains your database models. The models should be mostly self-explanatory. Relations are defined and some columns have `help_text`.

## TODO
- Fork the repo into your own Github account. Make the fork public.
- Implement the following methods for the child class `PMS_Apaleo` in the file `pms_systems.py`: `clean_webhook_payload`, `handle_webhook`.
- Webhook calls should use the `clean_webhook_payload`, `handle_webhook` methods. You can test the webhook functionality by making a POST request to the url: `http://localhost:8000/webhook/apaleo/` with the payload:
```
{
    "HotelId": "851df8c8-90f2-4c4a-8e01-a4fc46b25178",
    "IntegrationId": "c8bee838-7fb1-4f4e-8fac-ac87008b2f90",
    "Events": [
        {
            "Name": "ReservationUpdated",
            "Value": {
                "ReservationId": "5a9469b7-f13f-4a8d-b092-afe400fd7721"
            }
        },
        {
            "Name": "ReservationUpdated",
            "Value": {
                "ReservationId": "7c22cb23-c517-48f9-a5d4-da811043bd67"
            }
        },
        {
            "Name": "ReservationUpdated",
            "Value": {
                "ReservationId": "7c22cb23-c517-48f9-a5d4-da811023bd67"
            }
        }
    ]
}
```
