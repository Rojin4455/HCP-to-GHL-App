import requests
import json
import logging
from typing import Dict, Any, Optional
from django.conf import settings
from .models import GHLAuthCredentials, HCPToGHLMapping, ContactMapping, OpportunityMapping

logger = logging.getLogger(__name__)

class GoHighLevelService:
    BASE_URL = "https://services.leadconnectorhq.com"
    
    # Pipeline stage mappings
    PIPELINE_STAGES = {
        "new_leads": "d3919a4d-0364-4711-965c-9eb35fa237f4",
        "quote_requested": "5734163e-7160-4aa5-9998-4f6251acaa3a", 
        "send_estimate_now": "c7581484-707f-4b0c-a2d8-b337f3b5a2af",
        "not_booked_follow_up": "7455ac3a-538c-4ece-a6f4-26d4c179b430",
        "booking_requested": "f3b25dda-da79-479e-836d-385fbc28bfc5",
        "booking_confirmed": "17e08482-30e8-4f40-8102-6b57a5ade4a2",
        "service_sold": "5e87a81e-b699-449c-9bbd-b19b0a074917",
    }

    def get_pipeline_stage_id(self,event_type):
        """Map HCP event types to GHL pipeline stage IDs"""
        stage_mapping = {
            # Estimate events
            'estimate.created':    'be6b28f7-b0ce-43c6-a27d-b3862c937573',  # Estimate Created
            'estimate.scheduled':  '4af05417-3d54-4dbf-82c9-ef98367fdf51',  # Estimate Scheduled
            'estimate.on_my_way':  '4ae7824b-92a7-4f25-a4ca-0e65b4ca4c43',  # Estimate On My Way
            'estimate.completed':  '40c97416-7379-43e3-a908-e37f88f923bb',  # Estimate Completed
            'estimate.sent':       'db9f2183-de84-4b60-8c41-3c2177dbc947',  # Estimate Sent

            # Job events
            'job.created':         '6c9e3352-2958-4d59-b93b-9f967274539d',  # Job Created
            'job.scheduled':       '7d17b02f-88af-4e7c-abc4-59ef89f0e189',  # Job Scheduled
            'job.on_my_way':       'e72d3998-b9cf-42bb-bfdc-e0ac9226466d',  # Job On My Way
            'job.started':         '706a1981-db46-4b0d-9543-47270c20193e',  # Job Started
            'job.completed':       '6be00967-b2ad-4e5f-b6a2-7f63d6977a39',  # Job Completed
        }
        return stage_mapping.get(event_type)
    
    PIPELINE_ID = "kHLBjOkrltkMAOOIINvs"

    def __init__(self, access_token: str, event_type):
        self.access_token = access_token
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Version': '2021-07-28'
        }
        self.event = event_type

    def create_contact(self, location_id: str, contact_data: Dict[str, Any]) -> Optional[str]:
        """Create a contact in GoHighLevel"""
        url = f"{self.BASE_URL}/contacts/"
        
        payload = {
            "locationId": location_id,
            "firstName": contact_data.get('first_name', ''),
            "lastName": contact_data.get('last_name', ''),
            "email": contact_data.get('email', ''),
            "phone": contact_data.get('mobile_number', ''),
            "source": contact_data.get('lead_source', ''),
            "tags": contact_data.get('tags', [])
        }
        
        # Add additional phone numbers if available
        if contact_data.get('home_number'):
            payload['customFields'] = payload.get('customFields', [])
            payload['customFields'].append({
                "key": "home_phone",
                "field_value": contact_data['home_number']
            })
        
        if contact_data.get('work_number'):
            payload['customFields'] = payload.get('customFields', [])
            payload['customFields'].append({
                "key": "work_phone", 
                "field_value": contact_data['work_number']
            })

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return result.get('contact', {}).get('id')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating contact in GHL: {e}")
            return None
        
    def delete_contact(self, contact_id: str, contact_data: Dict[str, Any]) -> bool:
        """Delete a contact in GoHighLevel"""
        url = f"{self.BASE_URL}/contacts/{contact_id}"
        

        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error Deleting contact in GHL: {e}")
            return False

    def update_contact(self, contact_id: str, contact_data: Dict[str, Any]) -> bool:
        """Update a contact in GoHighLevel"""
        url = f"{self.BASE_URL}/contacts/{contact_id}"
        
        payload = {
            "firstName": contact_data.get('first_name', ''),
            "lastName": contact_data.get('last_name', ''),
            "email": contact_data.get('email', ''),
            "phone": contact_data.get('mobile_number', ''),
            "source": contact_data.get('lead_source', ''),
            "tags": contact_data.get('tags', [])
        }

        try:
            response = requests.put(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating contact in GHL: {e}")
            return False

    def create_opportunity(self, location_id: str, contact_id: str, opportunity_data: Dict[str, Any]) -> Optional[str]:
        """Create an opportunity in GoHighLevel"""
        url = f"{self.BASE_URL}/opportunities/"
        
        # Determine pipeline stage based on work status
        print("stage_id before, ", self.event)
        # stage_id = self._get_pipeline_stage_from_status(opportunity_data.get('work_status', ''))
        stage_id = self.get_pipeline_stage_id(event_type=self.event)
        print("after: ", stage_id)
        payload = {
            "pipelineId": self.PIPELINE_ID,
            "locationId": location_id,
            "contactId": contact_id,
            "name": f"{opportunity_data["customer"]["first_name"]} {opportunity_data["customer"]["last_name"]} #{opportunity_data.get('estimate_number', 'N/A')}",
            "pipelineStageId": stage_id,
            "source":opportunity_data.get("source", ""),
            "status": "open",
            "monetaryValue": opportunity_data.get('total_amount', 0),
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return result.get('opportunity', {}).get('id')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating opportunity in GHL: {e}")
            return None

    def update_opportunity(self, opportunity_id: str, opportunity_data: Dict[str, Any]) -> bool:
        """Update an opportunity in GoHighLevel"""
        url = f"{self.BASE_URL}/opportunities/{opportunity_id}"
        
        # Determine pipeline stage based on work status
        # stage_id = self._get_pipeline_stage_from_status(opportunity_data.get('work_status', ''))
        print("event type in update opportunity")
        stage_id = self.get_pipeline_stage_id(event_type=self.event)
        
        print("Stage ID: ", stage_id)

        
        payload = {
            "pipelineStageId": stage_id,
            "monetaryValue": opportunity_data.get('total_amount', 0),
        }
        
        # Update name if it's a job
        if opportunity_data.get('invoice_number'):
            payload["name"] = f"{opportunity_data["customer"]["first_name"]} {opportunity_data["customer"]["last_name"]} #{opportunity_data['invoice_number']}"

        try:
            response = requests.put(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating opportunity in GHL: {e}")
            return False

    def _get_pipeline_stage_from_status(self, work_status: str) -> str:
        """Map Housecall Pro work status to GHL pipeline stage"""
        # status_mapping = {
        #     'needs scheduling': self.get_pipeline_stage_id[self.event],
        #     'scheduled': self.get_pipeline_stage_id[self.event], 
        #     'in progress': self.get_pipeline_stage_id[self.event],
        #     'complete unrated': self.get_pipeline_stage_id[self.event],
        #     'complete': self.get_pipeline_stage_id[self.event],
        #     'created job from estimate': self.get_pipeline_stage_id[self.event],
        #     'submitted for signoff': self.get_pipeline_stage_id[self.event],
        # }
        return self.get_pipeline_stage_id[self.event]

class HousecallProWebhookService:
    def __init__(self):
        self.ghl_service = None

    def process_webhook(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main method to process Housecall Pro webhooks"""
        self.event = webhook_data.get('event')
        company_id = webhook_data.get('company_id')
        
        
        if not company_id:
            return {"error": "No company_id in webhook data"}

        # Get GHL mapping for this HCP company
        try:
            mapping = HCPToGHLMapping.objects.get(hcp_company_id=company_id)
            credentials = mapping.ghl_credentials
            self.ghl_service = GoHighLevelService(credentials.access_token, self.event)
        except HCPToGHLMapping.DoesNotExist:
            return {"error": f"No GHL mapping found for HCP company {company_id}"}

        # Route to appropriate handler based on event type
        if self.event == 'customer.created':
            return self._handle_customer_created(webhook_data, mapping)
        if self.event == 'customer.deleted':
            return self._handle_customer_deleted(webhook_data, mapping)
        elif self.event == 'customer.updated':
            return self._handle_customer_updated(webhook_data, mapping)
        elif self.event == 'estimate.created':
            return self._handle_estimate_created(webhook_data, mapping)
        elif self.event == 'estimate.scheduled':
            return self._handle_estimate_updated(webhook_data, mapping)        
        elif self.event == 'estimate.on_my_way':
            return self._handle_estimate_updated(webhook_data, mapping)
        elif self.event == 'estimate.completed':
            return self._handle_estimate_updated(webhook_data, mapping)

        
        # elif self.event == 'estimate.updated':
        #     return self._handle_estimate_updated(webhook_data, mapping)
        elif self.event == 'estimate.sent':
            return self._handle_estimate_sent(webhook_data, mapping)
        # elif self.event == 'estimate.option.created':
        #     return self._handle_estimate_option_created(webhook_data, mapping)
        # elif self.event == 'estimate.option.approval_status_changed':
        #     return self._handle_estimate_approval_changed(webhook_data, mapping)
        # elif self.event == 'estimate.copy_to_job':
        #     return self._handle_estimate_copy_to_job(webhook_data, mapping)
        elif self.event == 'job.created':
            return self._handle_job_created(webhook_data, mapping)
        elif self.event == 'job.scheduled':
            return self._handle_job_created(webhook_data, mapping)
        elif self.event == 'job.on_my_way':
            return self._handle_job_created(webhook_data, mapping)
        elif self.event == 'job.started':
            return self._handle_job_started(webhook_data, mapping)
        elif self.event == 'job.completed':
            return self._handle_job_completed(webhook_data, mapping)
        else:
            return {"message": f"Event {self.event} not handled"}

    def _handle_customer_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle customer.created webhook"""
        customer_data = webhook_data.get('customer', {})
        hcp_customer_id = customer_data.get('id')
        
        if not hcp_customer_id:
            return {"error": "No customer ID in webhook data"}

        # Check if contact already exists
        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if contact_mapping:
            return {"message": "Contact already exists", "ghl_contact_id": contact_mapping.ghl_contact_id}

        # Create contact in GHL
        ghl_contact_id = self.ghl_service.create_contact(mapping.ghl_location_id, customer_data)
        
        if ghl_contact_id:
            # Save mapping
            ContactMapping.objects.create(
                hcp_customer_id=hcp_customer_id,
                ghl_contact_id=ghl_contact_id,
                hcp_company_id=mapping.hcp_company_id,
                ghl_location_id=mapping.ghl_location_id
            )
            return {"message": "Contact created successfully", "ghl_contact_id": ghl_contact_id}
        else:
            return {"error": "Failed to create contact in GHL"}
        

    def _handle_customer_deleted(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle customer.deleted webhook"""
        customer_data = webhook_data.get('customer', {})
        hcp_customer_id = customer_data.get('id')
        
        if not hcp_customer_id:
            return {"error": "No customer ID in webhook data"}

        # Check if contact already exists
        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()

        if not contact_mapping:
            return {"message": "Contact is exists"}
        

        ghl_contact_id = self.ghl_service.delete_contact(contact_mapping.ghl_contact_id, customer_data)
        contact_mapping.delete()
        

        return {"message": "Contact deleted successfully", "ghl_contact_id": ghl_contact_id}


    def _handle_customer_updated(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle customer.updated webhook"""
        customer_data = webhook_data.get('customer', {})
        hcp_customer_id = customer_data.get('id')
        
        # Find existing contact mapping
        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if not contact_mapping:
            # Create new contact if it doesn't exist
            return self._handle_customer_created(webhook_data, mapping)
        
        # Update existing contact
        success = self.ghl_service.update_contact(contact_mapping.ghl_contact_id, customer_data)
        
        if success:
            return {"message": "Contact updated successfully"}
        else:
            return {"error": "Failed to update contact in GHL"}

    def _handle_estimate_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.created webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        options = estimate_data.get("options",[])

        
        return self._create_or_update_opportunity(options, estimate_data, customer_data, mapping, is_estimate=True)

    def _handle_estimate_updated(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.updated webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        options = estimate_data.get("options",[])
        
        return self._create_or_update_opportunity(options, estimate_data, customer_data, mapping, is_estimate=True)

    def _handle_estimate_sent(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.sent webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        options = estimate_data.get("options",[])
        
        return self._create_or_update_opportunity(options, estimate_data, customer_data, mapping, is_estimate=True)

    def _handle_estimate_option_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.option.created webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        options = estimate_data.get("options",[])
        
        return self._create_or_update_opportunity(options, estimate_data, customer_data, mapping, is_estimate=True)

    def _handle_estimate_approval_changed(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.option.approval_status_changed webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        options = estimate_data.get("options",[])
        
        return self._create_or_update_opportunity(options, estimate_data, customer_data, mapping, is_estimate=True)

    def _handle_estimate_copy_to_job(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.copy_to_job webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        options = estimate_data.get("options",[])
        
        return self._create_or_update_opportunity(options, estimate_data, customer_data, mapping, is_estimate=True)

    def _handle_job_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.created webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        return self._create_or_update_opportunity(None, job_data, customer_data, mapping, is_estimate=False)

    def _handle_job_started(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.started webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        return self._create_or_update_opportunity(None, job_data, customer_data, mapping, is_estimate=False)

    def _handle_job_completed(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.completed webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        return self._create_or_update_opportunity(None, job_data, customer_data, mapping, is_estimate=False)

    def _create_or_update_opportunity(self, options, opportunity_data: Dict[str, Any], customer_data: Dict[str, Any], 
                                    mapping: HCPToGHLMapping, is_estimate: bool = True) -> Dict[str, Any]:
        """Create or update opportunity in GHL"""
        
        # Ensure customer exists in GHL first
        hcp_customer_id = customer_data.get('id')
        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if not contact_mapping:
            # Create customer first
            ghl_contact_id = self.ghl_service.create_contact(mapping.ghl_location_id, customer_data)
            if ghl_contact_id:
                ContactMapping.objects.create(
                    hcp_customer_id=hcp_customer_id,
                    ghl_contact_id=ghl_contact_id,
                    hcp_company_id=mapping.hcp_company_id,
                    ghl_location_id=mapping.ghl_location_id
                )
            else:
                return {"error": "Failed to create contact in GHL"}
        else:
            ghl_contact_id = contact_mapping.ghl_contact_id

        # Handle opportunity creation/update
        if is_estimate:
            hcp_estimate_id = options[0].get('id')
            
            # Check if opportunity already exists for this estimate
            opp_mapping = OpportunityMapping.objects.filter(
                hcp_estimate_id=hcp_estimate_id,
                hcp_company_id=mapping.hcp_company_id
            ).first()
            
            if opp_mapping:
                # Update existing opportunity
                success = self.ghl_service.update_opportunity(opp_mapping.ghl_opportunity_id, opportunity_data)
                return {"message": "Opportunity updated" if success else "Failed to update opportunity"}
            else:
                # Create new opportunity
                ghl_opp_id = self.ghl_service.create_opportunity(
                    mapping.ghl_location_id, ghl_contact_id, opportunity_data
                )
                if ghl_opp_id:
                    OpportunityMapping.objects.create(
                        hcp_estimate_id=hcp_estimate_id,
                        ghl_opportunity_id=ghl_opp_id,
                        hcp_company_id=mapping.hcp_company_id,
                        ghl_location_id=mapping.ghl_location_id
                    )
                    return {"message": "Opportunity created", "ghl_opportunity_id": ghl_opp_id}
                else:
                    return {"error": "Failed to create opportunity"}
        else:
            # For jobs, find the corresponding estimate opportunity and update it
            hcp_job_id = opportunity_data.get('id')
            original_estimate_id = opportunity_data.get('original_estimate_id')
            
            if original_estimate_id:
                opp_mapping = OpportunityMapping.objects.filter(
                    hcp_estimate_id=original_estimate_id,
                    hcp_company_id=mapping.hcp_company_id
                ).first()
                
                if opp_mapping:
                    # Update the job_id in mapping
                    opp_mapping.hcp_job_id = hcp_job_id
                    opp_mapping.save()
                    
                    # Update the opportunity with job data
                    success = self.ghl_service.update_opportunity(opp_mapping.ghl_opportunity_id, opportunity_data)
                    return {"message": "Job opportunity updated" if success else "Failed to update job opportunity"}
                else:
                    return {"error": "No corresponding estimate opportunity found"}
            else:
                return {"error": "No original estimate ID found in job data"}
