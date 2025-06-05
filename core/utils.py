import requests
import json
import logging
from django.conf import settings
from .models import GHLAuthCredentials, HCPGHLContactMapping, HCPGHLOpportunityMapping

logger = logging.getLogger(__name__)

class GoHighLevelAPI:
    BASE_URL = "https://services.leadconnectorhq.com"
    
    def __init__(self, location_id=None):
        self.location_id = location_id
        self.access_token = None
        self._get_access_token()
    
    def _get_access_token(self):
        """Get valid access token from database"""
        try:
            # You might want to implement token refresh logic here
            credentials = GHLAuthCredentials.objects.filter(
                location_id=self.location_id
            ).first()
            
            if credentials:
                self.access_token = credentials.access_token
            else:
                logger.error(f"No credentials found for location: {self.location_id}")
        except Exception as e:
            logger.error(f"Error getting access token: {str(e)}")
    
    def _make_request(self, method, endpoint, data=None):
        """Make authenticated request to GHL API"""
        if not self.access_token:
            logger.error("No access token available")
            return None
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Version': '2021-07-28'
        }
        
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=data)
            elif method.upper() == 'PUT':
                response = requests.put(url, headers=headers, json=data)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=headers)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"GHL API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            return None
    
    def create_contact(self, contact_data):
        """Create contact in GHL"""
        endpoint = "/contacts/"
        return self._make_request('POST', endpoint, contact_data)
    
    def update_contact(self, contact_id, contact_data):
        """Update contact in GHL"""
        endpoint = f"/contacts/{contact_id}"
        return self._make_request('PUT', endpoint, contact_data)
    
    def get_contact(self, contact_id):
        """Get contact from GHL"""
        endpoint = f"/contacts/{contact_id}"
        return self._make_request('GET', endpoint)
    
    def create_opportunity(self, opportunity_data):
        """Create opportunity in GHL"""
        endpoint = "/opportunities/"
        return self._make_request('POST', endpoint, opportunity_data)
    
    def update_opportunity(self, opportunity_id, opportunity_data):
        """Update opportunity in GHL"""
        endpoint = f"/opportunities/{opportunity_id}"
        return self._make_request('PUT', endpoint, opportunity_data)
    
    def get_opportunity(self, opportunity_id):
        """Get opportunity from GHL"""
        endpoint = f"/opportunities/{opportunity_id}"
        return self._make_request('GET', endpoint)