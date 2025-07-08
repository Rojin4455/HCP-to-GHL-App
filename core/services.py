import requests
import json
import logging
from typing import Dict, Any, Optional
from django.conf import settings
from .models import GHLAuthCredentials, HCPToGHLMapping, ContactMapping, OpportunityMapping

logger = logging.getLogger(__name__)

class GoHighLevelService:
    BASE_URL = "https://services.leadconnectorhq.com"
    
    # Pipeline stage mappings for all HCP events
    PIPELINE_STAGES = {
        # Estimate events
        'estimate.created': 'be6b28f7-b0ce-43c6-a27d-b3862c937573',
        'estimate.updated': 'be6b28f7-b0ce-43c6-a27d-b3862c937573',
        'estimate.scheduled': '4af05417-3d54-4dbf-82c9-ef98367fdf51',
        'estimate.on_my_way': '4ae7824b-92a7-4f25-a4ca-0e65b4ca4c43',
        'estimate.completed': '40c97416-7379-43e3-a908-e37f88f923bb',
        'estimate.sent': 'db9f2183-de84-4b60-8c41-3c2177dbc947',
        'estimate.copy_to_job': '6c9e3352-2958-4d59-b93b-9f967274539d', # Job Created stage
        'estimate.option.created': 'be6b28f7-b0ce-43c6-a27d-b3862c937573',
        'estimate.option.approval_status_changed': 'db9f2183-de84-4b60-8c41-3c2177dbc947', # Estimate Sent stage
        
        # Job events
        'job.created': '6c9e3352-2958-4d59-b93b-9f967274539d',
        'job.updated': '6c9e3352-2958-4d59-b93b-9f967274539d',
        'job.scheduled': '7d17b02f-88af-4e7c-abc4-59ef89f0e189',
        'job.on_my_way': 'e72d3998-b9cf-42bb-bfdc-e0ac9226466d',
        'job.started': '706a1981-db46-4b0d-9543-47270c20193e',
        'job.completed': '6be00967-b2ad-4e5f-b6a2-7f63d6977a39',
        'job.canceled': '6c9e3352-2958-4d59-b93b-9f967274539d', # Job Created stage, then typically moved to 'lost'
        'job.deleted': '6c9e3352-2958-4d59-b93b-9f967274539d', # Job Created stage, then typically moved to 'lost'
        'job.paid': '6be00967-b2ad-4e5f-b6a2-7f63d6977a39',
        
        # Job appointment events
        'job.appointment.scheduled': '7d17b02f-88af-4e7c-abc4-59ef89f0e189',
        'job.appointment.rescheduled': '7d17b02f-88af-4e7c-abc4-59ef89f0e189',
        'job.appointment.appointment_discarded': '6c9e3352-2958-4d59-b93b-9f967274539d', # Job Created stage, then typically moved to 'lost'
        'job.appointment.appointment_pros_assigned': '7d17b02f-88af-4e7c-abc4-59ef89f0e189',
        'job.appointment.appointment_pros_unassigned': '6c9e3352-2958-4d59-b93b-9f967274539d', # Job Created stage, then typically moved to 'lost'
    }
    PIPELINE_ID = "kHLBjOkrltkMAOOIINvs" # This needs to be the actual pipeline ID in GHL

    def __init__(self, access_token: str, event_type: str):
        self.access_token = access_token
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Version': '2021-07-28'
        }
        self.event_type = event_type

    def get_pipeline_stage_id(self, event_type: str) -> str:
        """Get GHL pipeline stage ID for HCP event type"""
        return self.PIPELINE_STAGES.get(event_type, "")

    def create_contact(self, location_id: str, contact_data: Dict[str, Any]) -> Optional[str]:
        """Create a contact in GoHighLevel with housecallpro tag"""
        url = f"{self.BASE_URL}/contacts/"
        
        # Prepare tags - always include housecallpro
        tags = contact_data.get('tags', [])
        if isinstance(tags, list):
            tags = tags.copy()
        else:
            tags = []
        
        if 'housecallpro' not in tags:
            tags.append('housecallpro')
        
        payload = {
            "locationId": location_id,
            "firstName": contact_data.get('first_name', ''),
            "lastName": contact_data.get('last_name', ''),
            "email": contact_data.get('email', ''),
            "phone": contact_data.get('mobile_number', ''),
            "source": contact_data.get('lead_source', 'HousecallPro'),
            "tags": tags
        }
        
        # Add custom fields for additional phone numbers
        custom_fields = []
        if contact_data.get('home_number'):
            custom_fields.append({
                "key": "home_phone",
                "field_value": contact_data['home_number']
            })
        
        if contact_data.get('work_number'):
            custom_fields.append({
                "key": "work_phone", 
                "field_value": contact_data['work_number']
            })
        
        if contact_data.get('company'):
            custom_fields.append({
                "key": "company",
                "field_value": contact_data['company']
            })
        
        if custom_fields:
            payload['customFields'] = custom_fields

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return result.get('contact', {}).get('id')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating contact in GHL: {e}")
            return None

    def update_contact(self, contact_id: str, contact_data: Dict[str, Any]) -> bool:
        """Update a contact in GoHighLevel"""
        url = f"{self.BASE_URL}/contacts/{contact_id}"
        
        # Prepare tags - always include housecallpro
        tags = contact_data.get('tags', [])
        if isinstance(tags, list):
            tags = tags.copy()
        else:
            tags = []
        
        if 'housecallpro' not in tags:
            tags.append('housecallpro')
        
        payload = {
            "firstName": contact_data.get('first_name', ''),
            "lastName": contact_data.get('last_name', ''),
            "email": contact_data.get('email', ''),
            "phone": contact_data.get('mobile_number', ''),
            "source": contact_data.get('lead_source', 'HousecallPro'),
            "tags": tags
        }
        # Add custom fields for additional phone numbers
        custom_fields = []
        if contact_data.get('home_number'):
            custom_fields.append({
                "key": "home_phone",
                "field_value": contact_data['home_number']
            })
        
        if contact_data.get('work_number'):
            custom_fields.append({
                "key": "work_phone", 
                "field_value": contact_data['work_number']
            })
        
        if contact_data.get('company'):
            custom_fields.append({
                "key": "company",
                "field_value": contact_data['company']
            })
        
        if custom_fields:
            payload['customFields'] = custom_fields

        try:
            response = requests.put(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating contact in GHL: {e}")
            return False

    def delete_contact(self, contact_id: str) -> bool:
        """Delete a contact in GoHighLevel"""
        url = f"{self.BASE_URL}/contacts/{contact_id}"
        
        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error deleting contact in GHL: {e}")
            return False

    def create_opportunity(self, location_id: str, contact_id: str, opportunity_data: Dict[str, Any]) -> Optional[str]:
        """Create an opportunity in GoHighLevel"""
        url = f"{self.BASE_URL}/opportunities/"
        
        stage_id = self.get_pipeline_stage_id(self.event_type)
        
        # Determine opportunity name based on type
        customer = opportunity_data.get('customer', {})
        customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
        
        if opportunity_data.get('estimate_number'):
            name = f"{customer_name} - Estimate #{opportunity_data['estimate_number']}"
        elif opportunity_data.get('invoice_number'):
            name = f"{customer_name} - Job #{opportunity_data['invoice_number']}"
        else:
            name = f"{customer_name} - {opportunity_data.get('id', 'Unknown')}"
        
        # Get monetary value
        monetary_value = 0
        if opportunity_data.get('total_amount'):
            try:
                monetary_value = float(opportunity_data['total_amount']) / 100
            except (ValueError, TypeError):
                monetary_value = 0
        
        payload = {
            "pipelineId": self.PIPELINE_ID,
            "locationId": location_id,
            "contactId": contact_id,
            "name": name,
            "source": opportunity_data.get('lead_source', 'HousecallPro'),
            "status": "open",
            "monetaryValue": monetary_value,
        }
        
        if stage_id:
            payload["pipelineStageId"] = stage_id

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return result.get('opportunity', {}).get('id')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating opportunity in GHL: {e}")
            return None

    def update_opportunity(self, opportunity_id: str, opportunity_data: Dict[str, Any], option_data: Dict[str, Any] = None) -> bool:
        """Update an opportunity in GoHighLevel"""
        url = f"{self.BASE_URL}/opportunities/{opportunity_id}"
        
        stage_id = self.get_pipeline_stage_id(self.event_type)
        
        payload = {}
        
        if stage_id:
            payload["pipelineStageId"] = stage_id
        
        # Update monetary value
        if option_data and option_data.get("total_amount"):
            try:
                payload["monetaryValue"] = float(option_data["total_amount"]) / 100
            except (ValueError, TypeError):
                payload["monetaryValue"] = 0
        elif opportunity_data.get('total_amount'):
            try:
                payload["monetaryValue"] = float(opportunity_data['total_amount']) / 100
            except (ValueError, TypeError):
                payload["monetaryValue"] = 0
        
        # Update name if it's a job conversion
        if opportunity_data.get('invoice_number'):
            customer = opportunity_data.get('customer', {})
            customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
            payload["name"] = f"{customer_name} - Job #{opportunity_data['invoice_number']}"
        
        if not payload:
            return True  # Nothing to update

        try:
            response = requests.put(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating opportunity in GHL: {e}")
            return False

    def close_opportunity(self, opportunity_id: str, won: bool = True) -> bool:
        """Close an opportunity as won or lost"""
        url = f"{self.BASE_URL}/opportunities/{opportunity_id}"
        
        payload = {
            "status": "won" if won else "lost"
        }
        try:
            response = requests.put(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error closing opportunity in GHL: {e}")
            return False

class HousecallProWebhookService:
    def __init__(self):
        self.ghl_service = None
        self.event_type = None

    def process_webhook(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main method to process Housecall Pro webhooks"""
        self.event_type = webhook_data.get('event')
        company_id = webhook_data.get('company_id')
        
        if not company_id:
            return {"error": "No company_id in webhook data"}

        # Get GHL mapping for this HCP company
        try:
            mapping = HCPToGHLMapping.objects.get(hcp_company_id=company_id)
            credentials = mapping.ghl_credentials
            self.ghl_service = GoHighLevelService(credentials.access_token, self.event_type)
        except HCPToGHLMapping.DoesNotExist:
            return {"error": f"No GHL mapping found for HCP company {company_id}"}

        # Route to appropriate handler based on event type
        event_handlers = {
            # Customer events
            'customer.created': self._handle_customer_created,
            'customer.updated': self._handle_customer_updated,
            'customer.deleted': self._handle_customer_deleted,
            
            # Estimate events
            'estimate.created': self._handle_estimate_created,
            'estimate.updated': self._handle_estimate_updated,
            'estimate.scheduled': self._handle_estimate_updated,
            'estimate.on_my_way': self._handle_estimate_updated,
            'estimate.completed': self._handle_estimate_updated,
            'estimate.sent': self._handle_estimate_updated,
            'estimate.copy_to_job': self._handle_estimate_copy_to_job,
            'estimate.option.created': self._handle_estimate_option_created,
            'estimate.option.approval_status_changed': self._handle_estimate_option_approval_changed,
            
            # Job events
            'job.created': self._handle_job_created,
            'job.updated': self._handle_job_updated,
            'job.scheduled': self._handle_job_updated,
            'job.on_my_way': self._handle_job_updated,
            'job.started': self._handle_job_updated,
            'job.completed': self._handle_job_completed,
            'job.canceled': self._handle_job_canceled,
            'job.deleted': self._handle_job_deleted,
            'job.paid': self._handle_job_paid,
            
            # Job appointment events
            'job.appointment.scheduled': self._handle_job_appointment_event,
            'job.appointment.rescheduled': self._handle_job_appointment_event,
            'job.appointment.appointment_discarded': self._handle_job_appointment_event,
            'job.appointment.appointment_pros_assigned': self._handle_job_appointment_event,
            'job.appointment.appointment_pros_unassigned': self._handle_job_appointment_event,
        }

        handler = event_handlers.get(self.event_type)
        if handler:
            return handler(webhook_data, mapping)
        else:
            return {"message": f"Event {self.event_type} not handled"}

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
            # If contact exists, ensure it's up-to-date
            logger.info(f"Contact for HCP customer {hcp_customer_id} already exists, attempting to update.")
            success = self.ghl_service.update_contact(contact_mapping.ghl_contact_id, customer_data)
            return {"message": "Contact already exists and updated" if success else "Contact already exists, but failed to update", "ghl_contact_id": contact_mapping.ghl_contact_id}

        # Create contact in GHL
        ghl_contact_id = self.ghl_service.create_contact(mapping.ghl_location_id, customer_data)
        
        if ghl_contact_id:
            ContactMapping.objects.create(
                hcp_customer_id=hcp_customer_id,
                ghl_contact_id=ghl_contact_id,
                hcp_company_id=mapping.hcp_company_id,
                ghl_location_id=mapping.ghl_location_id
            )
            return {"message": "Contact created successfully", "ghl_contact_id": ghl_contact_id}
        else:
            return {"error": "Failed to create contact in GHL"}

    def _handle_customer_updated(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle customer.updated webhook"""
        customer_data = webhook_data.get('customer', {})
        hcp_customer_id = customer_data.get('id')
        
        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if not contact_mapping:
            logger.warning(f"Contact mapping for HCP customer {hcp_customer_id} not found on update, attempting to create.")
            return self._handle_customer_created(webhook_data, mapping)
        
        success = self.ghl_service.update_contact(contact_mapping.ghl_contact_id, customer_data)
        
        if success:
            return {"message": "Contact updated successfully"}
        else:
            return {"error": "Failed to update contact in GHL"}

    def _handle_customer_deleted(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle customer.deleted webhook"""
        customer_data = webhook_data.get('customer', {})
        hcp_customer_id = customer_data.get('id')
        
        if not hcp_customer_id:
            return {"error": "No customer ID in webhook data"}

        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if not contact_mapping:
            return {"message": "Contact does not exist in GHL mapping, nothing to delete."}

        success = self.ghl_service.delete_contact(contact_mapping.ghl_contact_id)
        
        if success:
            contact_mapping.delete()
            return {"message": "Contact deleted successfully"}
        else:
            return {"error": "Failed to delete contact in GHL"}

    def _handle_estimate_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.created webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        
        return self._create_or_update_estimate_opportunity(estimate_data, customer_data, mapping)

    def _handle_estimate_updated(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate update events"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        
        return self._create_or_update_estimate_opportunity(estimate_data, customer_data, mapping)

    def _handle_estimate_copy_to_job(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.copy_to_job webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        
        # When an estimate is copied to a job, we should convert the estimate opportunity to a job opportunity if it exists,
        # or create a new job opportunity if not, and then close the estimate opportunity as 'won'.
        # The job creation webhook will handle the job opportunity creation/update.
        
        # First, ensure the customer exists in GHL
        ghl_contact_id = self._ensure_contact_exists(customer_data, mapping)
        if not ghl_contact_id:
            return {"error": "Failed to create/find contact in GHL for estimate.copy_to_job"}

        hcp_estimate_id = estimate_data.get('id')
        if not hcp_estimate_id:
            return {"error": "No estimate ID in webhook data for estimate.copy_to_job"}

        # Find the existing estimate opportunity
        estimate_opp_mapping = OpportunityMapping.objects.filter(
            hcp_estimate_id=hcp_estimate_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()

        if estimate_opp_mapping:
            # Close the estimate opportunity as won
            success = self.ghl_service.close_opportunity(estimate_opp_mapping.ghl_opportunity_id, won=True)
            if success:
                logger.info(f"Closed estimate opportunity {estimate_opp_mapping.ghl_opportunity_id} as won due to copy_to_job.")
                # Optionally, you might want to update the mapping to reflect it's now a job-related opportunity,
                # or rely on the job.created webhook to create a new one. For simplicity, we'll let the job.created handle the job opportunity.
                # If the business logic dictates keeping only one opportunity, this is where you'd modify the existing mapping.
            else:
                logger.warning(f"Failed to close estimate opportunity {estimate_opp_mapping.ghl_opportunity_id} as won.")
        else:
            logger.info(f"No existing estimate opportunity found for HCP estimate ID {hcp_estimate_id} during copy_to_job.")

        # Let the job.created webhook handle the creation/update of the job opportunity
        return {"message": "Estimate copy to job processed, awaiting job creation webhook for opportunity handling."}

    def _handle_estimate_option_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.option.created webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        
        # This event implies a change in the estimate, potentially affecting its value.
        return self._create_or_update_estimate_opportunity(estimate_data, customer_data, mapping)

    def _handle_estimate_option_approval_changed(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle estimate.option.approval_status_changed webhook"""
        estimate_data = webhook_data.get('estimate', {})
        customer_data = estimate_data.get('customer', {})
        
        # When an estimate option's approval status changes, it might impact the opportunity value or stage.
        # If an option is approved and it leads to a definitive state, this should be reflected.
        # For simplicity, we'll assume a mere update of the opportunity is sufficient, and "job.created" will handle the actual "won" state.
        
        return self._create_or_update_estimate_opportunity(estimate_data, customer_data, mapping)

    def _handle_job_created(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.created webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        return self._create_or_update_job_opportunity(job_data, customer_data, mapping)

    def _handle_job_updated(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job update events"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        return self._create_or_update_job_opportunity(job_data, customer_data, mapping)

    def _handle_job_completed(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.completed webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        result = self._create_or_update_job_opportunity(job_data, customer_data, mapping)
        
        # Also close the opportunity as won
        if result.get('ghl_opportunity_id'):
            self.ghl_service.close_opportunity(result['ghl_opportunity_id'], won=True)
        
        return result

    def _handle_job_canceled(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.canceled webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        result = self._create_or_update_job_opportunity(job_data, customer_data, mapping)
        
        # Close the opportunity as lost
        if result.get('ghl_opportunity_id'):
            self.ghl_service.close_opportunity(result['ghl_opportunity_id'], won=False)
        
        return result

    def _handle_job_deleted(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.deleted webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        result = self._create_or_update_job_opportunity(job_data, customer_data, mapping)
        
        # Close the opportunity as lost
        if result.get('ghl_opportunity_id'):
            self.ghl_service.close_opportunity(result['ghl_opportunity_id'], won=False)
        
        return result

    def _handle_job_paid(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job.paid webhook"""
        job_data = webhook_data.get('job', {})
        customer_data = job_data.get('customer', {})
        
        # When a job is paid, it signifies a successful completion. Update the opportunity.
        result = self._create_or_update_job_opportunity(job_data, customer_data, mapping)
        
        # If it's paid, it should definitively be marked as won.
        if result.get('ghl_opportunity_id'):
            self.ghl_service.close_opportunity(result['ghl_opportunity_id'], won=True)
            
        return result

    def _handle_job_appointment_event(self, webhook_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Handle job appointment events"""
        appointment_data = webhook_data.get('appointment', {})
        job_id = appointment_data.get('job_id')
        
        if not job_id:
            return {"error": "No job_id in appointment data"}
        
        # Find the corresponding opportunity
        opp_mapping = OpportunityMapping.objects.filter(
            hcp_job_id=job_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if opp_mapping:
            # Update the opportunity stage based on the appointment event
            # We need to construct a minimal job_data dictionary for update_opportunity
            # as it expects 'customer' and potentially 'invoice_number' for name updates.
            # Since appointment events don't necessarily provide full job data,
            # we'll just pass a placeholder if not directly relevant to the name.
            fake_job_data = {'id': job_id, 'customer': {'first_name': '', 'last_name': ''}} # Minimal data for update
            success = self.ghl_service.update_opportunity(opp_mapping.ghl_opportunity_id, fake_job_data)
            return {"message": "Appointment event processed" if success else "Failed to update opportunity"}
        else:
            return {"message": "No corresponding opportunity found for job appointment"}

    def _ensure_contact_exists(self, customer_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Optional[str]:
        """Ensure customer exists in GHL and return contact ID"""
        hcp_customer_id = customer_data.get('id')
        
        if not hcp_customer_id:
            logger.warning("No customer ID provided in _ensure_contact_exists.")
            return None
        
        contact_mapping = ContactMapping.objects.filter(
            hcp_customer_id=hcp_customer_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if contact_mapping:
            # Update existing contact as well to ensure data is fresh
            self.ghl_service.update_contact(contact_mapping.ghl_contact_id, customer_data)
            return contact_mapping.ghl_contact_id
        
        # Create new contact
        ghl_contact_id = self.ghl_service.create_contact(mapping.ghl_location_id, customer_data)
        
        if ghl_contact_id:
            ContactMapping.objects.create(
                hcp_customer_id=hcp_customer_id,
                ghl_contact_id=ghl_contact_id,
                hcp_company_id=mapping.hcp_company_id,
                ghl_location_id=mapping.ghl_location_id
            )
            return ghl_contact_id
        
        return None

    def _create_or_update_estimate_opportunity(self, estimate_data: Dict[str, Any], customer_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Create or update opportunity for estimate events"""
        ghl_contact_id = self._ensure_contact_exists(customer_data, mapping)
        
        if not ghl_contact_id:
            return {"error": "Failed to create/find contact in GHL for estimate opportunity."}
        
        hcp_estimate_id = estimate_data.get('id')
        
        if not hcp_estimate_id:
            return {"error": "No estimate ID in webhook data for opportunity creation/update."}
        
        # Check if opportunity already exists
        opp_mapping = OpportunityMapping.objects.filter(
            hcp_estimate_id=hcp_estimate_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if opp_mapping:
            # Update existing opportunity
            option_data = None
            if estimate_data.get('options') and isinstance(estimate_data['options'], list):
                # Assuming the first option is the primary one for total amount if multiple exist
                option_data = estimate_data['options'][0] 
            
            success = self.ghl_service.update_opportunity(opp_mapping.ghl_opportunity_id, estimate_data, option_data)
            return {
                "message": "Estimate opportunity updated" if success else "Failed to update opportunity",
                "ghl_opportunity_id": opp_mapping.ghl_opportunity_id
            }
        else:
            # Create new opportunity
            ghl_opp_id = self.ghl_service.create_opportunity(mapping.ghl_location_id, ghl_contact_id, estimate_data)
            
            if ghl_opp_id:
                OpportunityMapping.objects.create(
                    hcp_estimate_id=hcp_estimate_id,
                    ghl_opportunity_id=ghl_opp_id,
                    hcp_company_id=mapping.hcp_company_id,
                    ghl_location_id=mapping.ghl_location_id
                )
                return {"message": "Estimate opportunity created", "ghl_opportunity_id": ghl_opp_id}
            else:
                return {"error": "Failed to create opportunity"}

    def _create_or_update_job_opportunity(self, job_data: Dict[str, Any], customer_data: Dict[str, Any], mapping: HCPToGHLMapping) -> Dict[str, Any]:
        """Create or update opportunity for job events"""
        ghl_contact_id = self._ensure_contact_exists(customer_data, mapping)
        
        if not ghl_contact_id:
            return {"error": "Failed to create/find contact in GHL for job opportunity."}
        
        hcp_job_id = job_data.get('id')
        original_estimate_id = job_data.get('original_estimate_id')
        
        if not hcp_job_id:
            return {"error": "No job ID in webhook data for job opportunity creation/update."}
        
        # First, try to find existing opportunity by job ID
        opp_mapping = OpportunityMapping.objects.filter(
            hcp_job_id=hcp_job_id,
            hcp_company_id=mapping.hcp_company_id
        ).first()
        
        if opp_mapping:
            # Update existing opportunity
            success = self.ghl_service.update_opportunity(opp_mapping.ghl_opportunity_id, job_data)
            return {
                "message": "Job opportunity updated" if success else "Failed to update opportunity",
                "ghl_opportunity_id": opp_mapping.ghl_opportunity_id
            }
        
        # If no job opportunity exists, check if there's an estimate opportunity to convert
        if original_estimate_id:
            estimate_opp_mapping = OpportunityMapping.objects.filter(
                hcp_estimate_id=original_estimate_id,
                hcp_company_id=mapping.hcp_company_id
            ).first()

            if estimate_opp_mapping:
                # Update the existing estimate opportunity to reflect it's now a job
                # and update its HcpJobId.
                success = self.ghl_service.update_opportunity(estimate_opp_mapping.ghl_opportunity_id, job_data)
                if success:
                    # Update the mapping to link it to the job ID
                    estimate_opp_mapping.hcp_job_id = hcp_job_id
                    estimate_opp_mapping.save()
                    return {
                        "message": "Converted estimate opportunity to job opportunity and updated",
                        "ghl_opportunity_id": estimate_opp_mapping.ghl_opportunity_id
                    }
                else:
                    logger.error(f"Failed to update existing estimate opportunity {estimate_opp_mapping.ghl_opportunity_id} to job.")
        
        # If neither existing job opportunity nor convertible estimate opportunity found, create a new one
        ghl_opp_id = self.ghl_service.create_opportunity(mapping.ghl_location_id, ghl_contact_id, job_data)
        
        if ghl_opp_id:
            OpportunityMapping.objects.create(
                hcp_job_id=hcp_job_id,
                ghl_opportunity_id=ghl_opp_id,
                hcp_company_id=mapping.hcp_company_id,
                ghl_location_id=mapping.ghl_location_id,
                hcp_estimate_id=original_estimate_id # Store original estimate ID if available
            )
            return {"message": "Job opportunity created", "ghl_opportunity_id": ghl_opp_id}
        else:
            return {"error": "Failed to create job opportunity"}

