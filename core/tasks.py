# your_app_name/tasks.py
import requests
from celery import shared_task
from core.models import GHLAuthCredentials
from decouple import config
import logging
# from core.services import (
#     # handle_job_created,
#     # handle_job_updated, 
#     # handle_customer_create, 
#     # handle_customer_update, 
#     # handle_customer_delete,
#     # create_ghl_opportunity,
#     # handle_customer_created,
#     # handle_estimate_approved,
#     # handle_estimate_created,
#     # handle_job_started,
#     # handle_job_completed,
#     # handle_estimate_sent,
    
# )

logger = logging.getLogger(__name__)

@shared_task
def make_api_call():
    credentials = GHLAuthCredentials.objects.first()
    
    print("credentials tokenL", credentials)
    refresh_token = credentials.refresh_token

    
    response = requests.post('https://services.leadconnectorhq.com/oauth/token', data={
        'grant_type': 'refresh_token',
        'client_id': config("GHL_CLIENT_ID"),
        'client_secret': config("GHL_CLIENT_SECRET"),
        'refresh_token': refresh_token
    })
    
    new_tokens = response.json()

    print("new tokens: ", new_tokens)

    obj, created = GHLAuthCredentials.objects.update_or_create(
            location_id= new_tokens.get("locationId"),
            defaults={
                "access_token": new_tokens.get("access_token"),
                "refresh_token": new_tokens.get("refresh_token"),
                "expires_in": new_tokens.get("expires_in"),
                "scope": new_tokens.get("scope"),
                "user_type": new_tokens.get("userType"),
                "company_id": new_tokens.get("companyId"),
                "user_id":new_tokens.get("userId"),

            }
        )



# @shared_task(
#     bind=True,
#     max_retries=3,
#     default_retry_delay=60,
#     autoretry_for=(Exception,),
#     retry_backoff=True,
# )
# def handle_webhook_event(self, data):
#     try:
#         # Get access token
#         ghl_credentials = GHLAuthCredentials.objects.first()
#         if not ghl_credentials:
#             logger.error("No GHL credentials found")
#             return
        
#         access_token = ghl_credentials.access_token
#         location_id = ghl_credentials.location_id
        
#         event_type = data.get('event')
#         logger.info(f"Processing event: {event_type}")
        
#         # Route to appropriate handler based on event type
#         if event_type == 'customer.created':
#             handle_customer_created(data, access_token, location_id)
#         elif event_type in ['estimate.created', 'estimate.option.created']:
#             handle_estimate_created(data, access_token, location_id)
#         elif event_type == 'estimate.sent':
#             handle_estimate_sent(data, access_token, location_id)
#         elif event_type == 'estimate.option.approval_status_changed':
#             handle_estimate_approved(data, access_token, location_id)
#         elif event_type == 'job.created':
#             handle_job_created(data, access_token, location_id)
#         elif event_type == 'job.started':
#             handle_job_started(data, access_token, location_id)
#         elif event_type == 'job.completed':
#             handle_job_completed(data, access_token, location_id)
#         else:
#             logger.info(f"Unhandled event type: {event_type}")
            
#     except Exception as e:
#         logger.error(f"Error processing webhook event: {str(e)}")
#         raise