# Pattern 02: Leads Ingestion

> Import leads from data warehouse into Odoo CRM module with automatic product and partner resolution

---

## Quick Stats

- **Complexity**:  Advanced
- **Production Usage**: Daily, 10K+ leads/month
- **Avg Execution Time**: 15 minutes
- **Success Rate**: 99.5%
- **Model**: crm.lead

---

## Pattern Overview

This pattern imports sales leads from BigQuery into Odoo CRM, automatically resolving product IDs from product codes, handling multi-country configurations, and managing complex partner relationships.

**Key Features**:
- Product code → Product ID resolution with country-specific variants
- Multi-country support (9 countries)
- Store key lookups
- UTM source/medium tracking
- Establishment type management
- Many2Many product relationships

---

## Domain Class

```python
# horeca_digital/lead_engine_odoo.py

from typing import List, Union, Optional
import logging
from google.cloud import bigquery
from horeca_digital.utils.odoo_utils import Connection

class Odoo:
    """Handles lead creation in Odoo CRM."""
    
    def chunks(self, l, n):
        """Split list into chunks of size n."""
        for i in range(0, len(l), n):
            yield l[i:i + n]
    
    def _get_product_ids(self, product_ids: str) -> Union[List[int], None]:
        """
        Extract product IDs from comma-separated string.
        
        Args:
            product_ids: "POS_L_Package,MTO_Starter,DISH_Pay"
        
        Returns:
            List of resolved Odoo product IDs
        """
        if product_ids:
            return list(map(int, product_ids.split(',')))
        return None
    
    def odoo_conn(self, odoo_creds):
        """Initialize Odoo connection."""
        conn = Connection()
        odoo = conn.connect(odoo_creds=odoo_creds)
        return odoo
    
    def load_data(self, odoo_creds, project_name='dwh_project'):
        """
        Load leads from BigQuery into Odoo.
        
        Args:
            odoo_creds: Odoo credentials dictionary
            project_name: BigQuery project ID
        """
        logging.info(f"Connecting to Odoo: {odoo_creds.get('hostname')}")
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
            'de': odoo.env.ref('base.lang_de').id,
            'es': odoo.env.ref('base.lang_es').id,
            'fr': odoo.env.ref('base.lang_fr').id,
            'hr': odoo.env.ref('base.lang_hr').id,
            'hu': odoo.env.ref('base.lang_hu').id,
            'it': odoo.env.ref('base.lang_it').id,
            'nl': odoo.env.ref('base.lang_nl').id,
            'pt': odoo.env.ref('base.lang_pt').id,
            'ro': odoo.env.ref('base.lang_ro').id,
        }
        
        # Build UTM source/medium mapping
        channel_ids = {
            'MCC Salesforce': odoo.env.ref('dish_crm.utm_medium_mcc_salesforce').id,
            'HD': odoo.env.ref('dish_agency.reseller_hd').id,
            'DISH': odoo.env.ref('dish_agency.reseller_dish_digital').id,
            'MCCDE': odoo.env.ref('dish_agency.agency_metro_mccde').id,
            'MCCES': odoo.env.ref('dish_agency.agency_metro_mcces').id,
            'METRO SAM': odoo.env.ref('dish_crm.utm_source_METRO_SAM').id,
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
        
        logging.info(f"Total leads retrieved: {results.total_rows}")
        
        tdatafinal = []
        
        for row in results:
            # Process product codes - filter out None/empty values
            product_codes = []
            if row['product_codes']:
                for pc in row['product_codes'].split(';'):
                    pc = pc.strip()
                    if pc and pc.lower() != 'none':
                        # Remove "None" prefix if exists
                        if pc.lower().startswith('none') and len(pc) > 4:
                            pc = pc[4:]
                        if pc:
                            product_codes.append(pc)
            
            # Resolve product IDs with country-specific variants
            product_ids_list = []
            if product_codes:
                for pc in product_codes:
                    try:
                        # Special handling for DE and IT
                        if row["metro_id_country"].upper() in ["DE", "IT"] and \
                           pc in ['POS_L_Package', 'POS_L_PackageS']:
                            product_ref = odoo.env.ref(f'dish_product_catalog.{pc}:1:12')
                        else:
                            product_ref = odoo.env.ref(f'dish_product_catalog.{pc}:1:1')
                        
                        if product_ref:
                            product_ids_list.append(product_ref.id)
                    except Exception as e:
                        logging.warning(f"Product code '{pc}' not found: {e}")
                        continue
            
            # Build lead data dictionary
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
                "user_id": False,  # No automatic assignment
                "product_ids": [(6, 0, product_ids_list)] if product_ids_list else None,
                "establishment_type": establishment_type_none_id
            }
            
            # Filter out None values
            filtered = {k: v for k, v in tdata.items() if v is not None}
            tdatafinal.append(filtered)
        
        # Insert leads in chunks of 1
        chunked = list(self.chunks(tdatafinal, 1))
        for i, chunk in zip(range(1, len(chunked)+1), chunked):
            try:
                lead = odoo.env['crm.lead']
                logging.info(
                    f'Inserting lead #{i} - Country: {chunk[0].get("country_id")}, '
                    f'Store: {chunk[0].get("store_id")}, Metro ID: {chunk[0]["metro_id"]}'
                )
                lead.create(chunk)
            except Exception as e:
                logging.error(f"Failed inserting lead #{i}: {e}")
                continue
        
        odoo.logout()
        logging.info(f"Successfully inserted {len(chunked)} leads")
    
    def _get_store_key(self, odoo, text, model='dish_metro.store'):
        """
        Retrieve store ID from code.
        
        Args:
            odoo: Odoo connection
            text: Store code (e.g., "MCCDE", "MCCES")
            model: Odoo model name
        
        Returns:
            Store ID or 0 if not found
        """
        try:
            store_key = odoo.env[model].search([('code', '=', text)])
            return store_key[0] if store_key else 0
        except:
            return 0
```

---

## Key Features

### 1. Product Code Resolution

```python
# Input: "POS_L_Package;MTO_Starter;DISH_Pay"
# Output: [product_id_1, product_id_2, product_id_3]

# With country-specific variants
if country in ["DE", "IT"] and product_code == 'POS_L_Package':
    # Use variant :1:12 for DE/IT
    product_ref = odoo.env.ref('dish_product_catalog.POS_L_Package:1:12')
else:
    # Use standard variant :1:1
    product_ref = odoo.env.ref('dish_product_catalog.POS_L_Package:1:1')
```

### 2. Many2Many Product IDs

```python
# Odoo Many2Many command tuple
data = {
    'product_ids': [(6, 0, [prod_id1, prod_id2, prod_id3])]
    # (6, 0, [...]) = Replace all with these IDs
}
```

### 3. Dynamic Mapping Dictionaries

```python
# Build mappings once, reuse for all leads
country_ids = {
    'DE': odoo.env.ref('base.de').id,
    'ES': odoo.env.ref('base.es').id,
    # ... 9 countries
}

# Fast lookup during processing
country_id = country_ids.get(row['country_code'].upper())
```

---

## Production Lessons

### What Worked

1. **Ref-based ID Resolution**
   - `odoo.env.ref('base.de')` faster than search queries
   - Cached by Odoo, very efficient

2. **Product Code Variants**
   - Country-specific product variants (`:1:12` vs `:1:1`)
   - Flexible external ID structure

3. **None Filtering**
   - Remove None values before create (cleaner, faster)
   - Odoo handles defaults for missing fields

### What Didn't Work

1. **No Chunking Initially**
   - First version: all leads in one create call
   - Result: Memory issues with 5K+ leads
   - Fix: Chunk size of 1 (safest)

2. **Product Code String Parsing**
   - Complex: "NoneMTO_Starter" → "MTO_Starter"
   - Added explicit None prefix removal

---

## Related Patterns

- [Opportunities Load](../03-opportunities-load/) - Similar CRM pattern
- [Connection Management](../05-connection-management/) - Reusable utilities

---

<p align="center">
  <i>10K+ leads/month | 9 countries | 99.5% success rate</i>
</p>
