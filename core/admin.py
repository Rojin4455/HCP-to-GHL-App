from django.contrib import admin
from core.models import GHLAuthCredentials, HCPToGHLMapping, ContactMapping, OpportunityMapping

admin.site.register(GHLAuthCredentials)
admin.site.register(HCPToGHLMapping)
admin.site.register(ContactMapping)
admin.site.register(OpportunityMapping)

# Register your models here.
