# models.py
from django.db import models
from django.utils import timezone
import uuid

class GHLAuthCredentials(models.Model):
    user_id = models.CharField(max_length=255, unique=True)
    access_token = models.TextField()
    refresh_token = models.TextField()
    expires_in = models.IntegerField()
    scope = models.CharField(max_length=500, null=True, blank=True)
    user_type = models.CharField(max_length=50, null=True, blank=True)
    company_id = models.CharField(max_length=255, null=True, blank=True)
    location_id = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user_id} - {self.company_id}"

class HCPToGHLMapping(models.Model):
    """Maps Housecall Pro company_id to GoHighLevel location_id and credentials"""
    hcp_company_id = models.CharField(max_length=255, unique=True)
    ghl_location_id = models.CharField(max_length=255)
    ghl_credentials = models.ForeignKey(GHLAuthCredentials, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"HCP: {self.hcp_company_id} -> GHL: {self.ghl_location_id}"

class ContactMapping(models.Model):
    """Maps Housecall Pro customer IDs to GoHighLevel contact IDs"""
    hcp_customer_id = models.CharField(max_length=255)
    ghl_contact_id = models.CharField(max_length=255)
    hcp_company_id = models.CharField(max_length=255)
    ghl_location_id = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['hcp_customer_id', 'hcp_company_id']

class OpportunityMapping(models.Model):
    """Maps Housecall Pro estimates/jobs to GoHighLevel opportunities"""
    hcp_estimate_id = models.CharField(max_length=255, null=True, blank=True)
    hcp_job_id = models.CharField(max_length=255, null=True, blank=True)
    ghl_opportunity_id = models.CharField(max_length=255)
    hcp_company_id = models.CharField(max_length=255)
    ghl_location_id = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['hcp_estimate_id', 'hcp_company_id']