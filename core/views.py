from decouple import config
import requests
from django.http import JsonResponse
import json
from django.shortcuts import redirect
from core.models import GHLAuthCredentials
from django.views.decorators.csrf import csrf_exempt
# from core.tasks import handle_webhook_event
import logging
# from .services import HousecallProGHLService
from django.views import View
from django.utils.decorators import method_decorator






GHL_CLIENT_ID = config("GHL_CLIENT_ID")
GHL_CLIENT_SECRET = config("GHL_CLIENT_SECRET")
GHL_REDIRECTED_URI = config("GHL_REDIRECTED_URI")
TOKEN_URL = "https://services.leadconnectorhq.com/oauth/token"
SCOPE = config("SCOPE")

def auth_connect(request):
    auth_url = ("https://marketplace.leadconnectorhq.com/oauth/chooselocation?response_type=code&"
                f"redirect_uri={GHL_REDIRECTED_URI}&"
                f"client_id={GHL_CLIENT_ID}&"
                f"scope={SCOPE}"
                )
    return redirect(auth_url)



def callback(request):
    
    code = request.GET.get('code')

    if not code:
        return JsonResponse({"error": "Authorization code not received from OAuth"}, status=400)

    return redirect(f'{config("BASE_URI")}/core/auth/tokens?code={code}')


def tokens(request):
    authorization_code = request.GET.get("code")

    if not authorization_code:
        return JsonResponse({"error": "Authorization code not found"}, status=400)

    data = {
        "grant_type": "authorization_code",
        "client_id": GHL_CLIENT_ID,
        "client_secret": GHL_CLIENT_SECRET,
        "redirect_uri": GHL_REDIRECTED_URI,
        "code": authorization_code,
    }

    response = requests.post(TOKEN_URL, data=data)

    try:
        response_data = response.json()
        if not response_data:
            return

        obj, created = GHLAuthCredentials.objects.update_or_create(
            location_id= response_data.get("locationId"),
            defaults={
                "access_token": response_data.get("access_token"),
                "refresh_token": response_data.get("refresh_token"),
                "expires_in": response_data.get("expires_in"),
                "scope": response_data.get("scope"),
                "user_type": response_data.get("userType"),
                "company_id": response_data.get("companyId"),
                "user_id":response_data.get("userId"),

            }
        )
        return JsonResponse({
            "message": "Authentication successful",
            "access_token": response_data.get('access_token'),
            "token_stored": True
        })
        
    except requests.exceptions.JSONDecodeError:
        return JsonResponse({
            "error": "Invalid JSON response from API",
            "status_code": response.status_code,
            "response_text": response.text[:500]
        }, status=500)
    

logger = logging.getLogger(__name__)

# @csrf_exempt
# def webhook(request):
#     if request.method == "POST":
#         try:
#             data = json.loads(request.body)
#             handle_webhook_event.delay(data)
#             logger.info(f"Webhook received: {data.get('event', 'unknown')}")
#             return JsonResponse({"status": "success", "message": "Webhook received"}, status=200)
#         except json.JSONDecodeError:
#             logger.error("Invalid JSON received in webhook")
#             return JsonResponse({"status": "error", "message": "Invalid JSON"}, status=400)
#     else:
#         return JsonResponse({"status": "error", "message": "Only POST method is allowed"}, status=405)
    

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
import json
import logging
from core.services import HousecallProWebhookService

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class HousecallProWebhookView(View):
    
    def post(self, request):
        try:
            # Parse webhook data
            webhook_data = json.loads(request.body)
            
            # Log the received webhook
            logger.info(f"Received webhook: {webhook_data.get('event')} for company {webhook_data.get('company_id')}")
            
            # Process the webhook
            service = HousecallProWebhookService()
            result = service.process_webhook(webhook_data)
            
            return JsonResponse(result, status=200)
            
        except json.JSONDecodeError:
            logger.error("Invalid JSON in webhook request")
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            logger.error(f"Error processing webhook: {str(e)}")
            return JsonResponse({"error": "Internal server error"}, status=500)
