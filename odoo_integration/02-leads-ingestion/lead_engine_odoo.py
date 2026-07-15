"""
Lead engine for importing leads from data warehouse into Odoo CRM.

Handles:
- Product code resolution
- Multi-country support
- Store key lookups
- UTM tracking
"""

import logging
from typing import List, Union, Optional
from google.cloud import bigquery
from odoo_utils import Connection

logger = logging.getLogger(__name__)


class Odoo:
    """Handles lead creation in Odoo CRM."""
    
    def chunks(self, l: List, n: int):
        """Split list into chunks of size n."""
        for i in range(0, len(l), n):
            yield l[i:i + n]
    
    def _get_product_ids(self, product_ids: str) -> Union[List[int], None]:
        """
        Extract product IDs from comma-separated string.
        
        Args:
            product_ids: Comma-separated product IDs
        
        Returns:
            List of integers or None
        """
        if product_ids:
            return list(map(int, product_ids.split(',')))
        return None
    
    def _get_store_key(self, odoo, text: str, model: str = 'dish_metro.store'):
        """
        Retrieve store ID from code.
        
        Args:
            odoo: Odoo connection
            text: Store code
            model: Odoo model name
        
        Returns:
            Store ID or 0
        """
        try:
            store_key = odoo.env[model].search([('code', '=', text)])
            return store_key[0] if store_key else 0
        except:
            return 0
    
    def odoo_conn(self, odoo_creds):
        """Initialize Odoo connection."""
        conn = Connection()
        odoo = conn.connect(odoo_creds=odoo_creds)
        return odoo
    
    def load_data(self, odoo_creds, project_name: str = 'dwh-project'):
        """
        Load leads from BigQuery into Odoo.
        
        Args:
            odoo_creds: Odoo credentials dictionary
            project_name: BigQuery project ID
        """
        logger.info(f"Connecting to Odoo: {odoo_creds.get('hostname')}")
        odoo = self.odoo_conn(odoo_creds)
        
        # Get establishment type "none" ID
        establishment_type_none_id = odoo.env.ref("dish_crm.establishment_type_none").id
        
        # Build country ID mapping
        country_ids = {
            'DE': odoo.env.ref('base.de').id if odoo.env.ref('base.de') else False,
            'ES': odoo.env.ref('base.es').id if odoo.env.ref('base.es') else False,
            'FR': odoo.env.ref('base.fr').id if odoo.env.ref('base.fr') else False,
            'HR': odoo.env.ref('base.hr').id if odoo.env.ref('base.hr') else False,
            'HU': odoo.env.ref('base.hu').id if odoo.env.ref('base.hu') else False,
            'IT': odoo.env.ref('base.it').id if odoo.env.ref('base.it') else False,
            'NL': odoo.env.ref('base.nl').id if odoo.env.ref('base.nl') else False,
            'PT': odoo.env.ref('base.pt').id if odoo.env.ref('base.pt') else False,
            'RO': odoo.env.ref('base.ro').id if odoo.env.ref('base.ro') else False
        }
        
        # Build language ID mapping
        lang_ids = {
            'de': odoo.env.ref('base.lang_de').id if odoo.env.ref('base.lang_de') else False,
            'es': odoo.env.ref('base.lang_es').id if odoo.env.ref('base.lang_es') else False,
            'fr': odoo.env.ref('base.lang_fr').id if odoo.env.ref('base.lang_fr') else False,
            'hr': odoo.env.ref('base.lang_hr').id if odoo.env.ref('base.lang_hr') else False,
            'hu': odoo.env.ref('base.lang_hu').id if odoo.env.ref('base.lang_hu') else False,
            'it': odoo.env.ref('base.lang_it').id if odoo.env.ref('base.lang_it') else False,
            'nl': odoo.env.ref('base.lang_nl').id if odoo.env.ref('base.lang_nl') else False,
            'pt': odoo.env.ref('base.lang_pt').id if odoo.env.ref('base.lang_pt') else False,
            'ro': odoo.env.ref('base.lang_ro').id if odoo.env.ref('base.lang_ro') else False,
        }
        
        # Build UTM source/medium mapping
        channel_ids = {
            'MCC Salesforce': odoo.env.ref('dish_crm.utm_medium_mcc_salesforce').id if odoo.env.ref('dish_crm.utm_medium_mcc_salesforce') else False,
            'HD': odoo.env.ref('dish_agency.reseller_hd').id if odoo.env.ref('dish_agency.reseller_hd') else False,
            'DISH': odoo.env.ref('dish_agency.reseller_dish_digital').id if odoo.env.ref('dish_agency.reseller_dish_digital') else False,
            'MCCDE': odoo.env.ref('dish_agency.agency_metro_mccde').id if odoo.env.ref('dish_agency.agency_metro_mccde') else False,
            'METRO SAM': odoo.env.ref('dish_crm.utm_source_METRO_SAM').id if odoo.env.ref('dish_crm.utm_source_METRO_SAM') else False,
        }
        
        # Query leads from BigQuery
        client = bigquery.Client(project=project_name)
        query = """
        SELECT DISTINCT 
            first_name,
            last_name,
            email,
            phone,
            mobile_phone,
            metro_id_country,
            company_postal_code,
            company_city,
            company_street,
            establishment_postal_code,
            establishment_city,
            establishment_street,
            store,
            establishment,
            primary_language,
            lead_source,
            channel_v2,
            role,
            merchant,
            reseller,
            vat_id,
            metro_id,
            product_codes,
            last_modified,
            load_date
        FROM `dwh_trusted.int_metro_sam_leads`
        WHERE load_date = CURRENT_DATE()
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        logger.info(f"Total leads retrieved: {results.total_rows}")
        
        tdatafinal = []
        
        for row in results:
            # Process product codes
            product_codes = []
            if row['product_codes']:
                for pc in row['product_codes'].split(';'):
                    pc = pc.strip()
                    if pc and pc.lower() != 'none':
                        if pc.lower().startswith('none') and len(pc) > 4:
                            pc = pc[4:]
                        if pc:
                            product_codes.append(pc)
            
            # Resolve product IDs
            product_ids_list = []
            if product_codes:
                for pc in product_codes:
                    try:
                        if row["metro_id_country"].upper() in ["DE", "IT"] and \
                           pc in ['POS_L_Package', 'POS_L_PackageS']:
                            product_ref = odoo.env.ref(f'dish_product_catalog.{pc}:1:12')
                        else:
                            product_ref = odoo.env.ref(f'dish_product_catalog.{pc}:1:1')
                        
                        if product_ref:
                            product_ids_list.append(product_ref.id)
                    except Exception as e:
                        logger.warning(f"Product code '{pc}' not found: {e}")
                        continue
            
            # Build lead data
            tdata = {
                "contact_name": f"{row['first_name']} {row['last_name']}",
                "email_from": row["email"],
                "phone": row["phone"],
                "mobile": row["mobile_phone"],
                "country_id": country_ids.get(row["metro_id_country"].upper()),
                "zip": row["company_postal_code"],
                "city": row["company_city"],
                "street": row["company_street"],
                "establishment_zip": row["establishment_postal_code"],
                "establishment_city": row["establishment_city"],
                "establishment_street": row["establishment_street"],
                "store_id": self._get_store_key(odoo, row["store"]),
                "establishment_name": row["establishment"],
                "name": row["establishment"],
                "lang_id": lang_ids.get(row["primary_language"].lower()),
                "source_id": channel_ids.get(row["lead_source"]),
                "medium_id": channel_ids.get(row["channel_v2"]),
                "function": row["role"],
                "reseller_id": channel_ids.get(row["merchant"].upper()),
                "partner_assigned_id": channel_ids.get(row["reseller"]),
                "metro_id": row["metro_id"],
                "description": f'Company VAT ID: {row["vat_id"]}, Product codes: {row["product_codes"]}',
                "user_id": False,
                "product_ids": [(6, 0, product_ids_list)] if product_ids_list else None,
                "establishment_type": establishment_type_none_id
            }
            
            # Filter None values
            filtered = {k: v for k, v in tdata.items() if v is not None}
            tdatafinal.append(filtered)
        
        # Insert leads
        chunked = list(self.chunks(tdatafinal, 1))
        for i, chunk in zip(range(1, len(chunked)+1), chunked):
            try:
                lead = odoo.env['crm.lead']
                logger.info(
                    f'Inserting lead #{i} - Country: {chunk[0].get("country_id")}, '
                    f'Metro ID: {chunk[0]["metro_id"]}'
                )
                lead.create(chunk)
            except Exception as e:
                logger.error(f"Failed inserting lead #{i}: {e}")
                continue
        
        odoo.logout()
        logger.info(f"✅ Successfully inserted {len(chunked)} leads")
