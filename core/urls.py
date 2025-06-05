from django.urls import path
from core.views import auth_connect,tokens,callback

from django.urls import path
from .views import HousecallProWebhookView

urlpatterns = [
    path("auth/connect/", auth_connect, name="oauth_connect"),
    path("auth/tokens/", tokens, name="oauth_tokens"),
    path("auth/callback/", callback, name="oauth_callback"),
    # path("webhook/", webhook),
    path('webhook/', HousecallProWebhookView.as_view(), name='hcp_webhook'),
]
