from django.contrib import admin
from core.models import GHLAuthCredentials, HCPToGHLMapping, ContactMapping, OpportunityMapping, Webhook

admin.site.register(GHLAuthCredentials)
admin.site.register(HCPToGHLMapping)
admin.site.register(ContactMapping)
admin.site.register(OpportunityMapping)
admin.site.register(Webhook)

# Register your models here.
