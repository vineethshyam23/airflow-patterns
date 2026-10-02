#TODO: refactor the constants to be UPPER_CASE
class TableNames: 
    """String definitions for the table names in the database that are
    used in the database query definitions. E.g. 
    f"select * from {TableNames.menuitemsclean_stage}"
    Thereby the table names can be changed in one place for all uses."""

    # HTML 
    MENU_ITEM_RAW = "smartdatadb.menu_item_raw"
    MENU_ITEM_CLEAN_STAGE = "smartdatadb.menu_item_stage"
    MENU_ITEM_CLEAN = "smartdatadb.menu_item"
    
    B2B_HTML = "smartdatadb.b2b_html"
    B2B_URLS = "smartdatadb.b2b_urls"

    # COMMON 
    MENU = "smartdatadb.menu"
    MENU_PROCESSED = "smartdatadb.menuprocessed"
    MENU_ITEM_DEL = "smartdatadb.menuitems_del"
    MENU_ITEM_DEL_URL = "smartdatadb.menuitems_del_url"
    IMPORT_STATUS_HIST = "smartdatadb.importstatushist"
    OBJEKT = "smartdatadb.objekt"
    B2B_URLS_PDF = "smartdatadb.v_b2b_urls_pdf"
    # TODO: once the objectid's are there, use the materialized view instead:
    # b2b_urls_pdf = "smartdatadb.v_b2b_urls_pdf"


class FunctionCalls: 
    """String definitions for the function calls in the database that are
    used in the database query definitions. 
    E.g. f"call {FunctionCalls.insert_menuitems_from_stage}()"
    """
  #  insert_menuitems_from_stage = "smartdatadb.insert_menuitems_nlcopy"
    insert_menuitems_from_stage_html = "smartdatastagdb.insert_menuitems('html')"
    insert_menuitems_from_stage_pdf = "smartdatastagdb.insert_menuitems('pdf')"
    insert_menuitems_from_stage_dishpdf = "smartdatastagdb.insert_menuitems('dishpdf')"