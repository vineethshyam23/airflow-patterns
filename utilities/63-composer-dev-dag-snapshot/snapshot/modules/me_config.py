class MeDb_config:
    """
    Class to contain any use-settings for the me module, such as database connection settings
    """
    PG_PASS_FILE = ".pgpass"

    def get_pg_config(self):
        # TBD: adding support to get db-parameters from env vars?
        with open(self.PG_PASS_FILE) as f:
            user, password, host, port, database = tuple(f.read().strip().split(":"))
        return (user, password, host, port, database)
    

class MeExtractorConfig: 
    def __init__(self):
        self.HTML_EXTRACTOR_STRING = "html_extractor"
        self.HTML_EXTRACTOR_ID = 1
        self.PDF_EXTRACTOR_STRING = "pdf_extractor"
        self.PDF_EXTRACTOR_ID = 2


class MeCleanerConfig: 
    """
    Configuration class for me_cleaner.Cleaner. 
    In this class, the currently supported countries are 
    defined. Undefined countries default to "germany".
    """
    def __init__(self): 

        self.OUTPUT_SKIPPED_ITEMS = None
        self.OUTPUT_RAW_ITEMS = None
        self.OUTPUT_CLEANED_ITEMS = None
        self.REMOVE_NO_PRICE = True
        self.REMOVE_MAX_PRICE = 1000
        self.REMOVE_MIN_PRICE = 0

        # Defines which countries are fully configured
        self.COUNTRY_MAPPER = {
            # 'at':'austria',
            # 'be':'belgium',
            'hr':'croatia',
            # 'cz':'czech republic',
            # 'dk':'denmark',
            'fr':'france',
            'de':'germany',
            # 'hu':'hungary',
            'it':'italy',
            'nl':'netherlands',
            'pl':'poland',
            # 'pt':'portugal',
            # 'ro':'romania',
            # 'sk':'slovakia',
            'es':'spain',
            # 'ch':'switzerland',
            # 'tr':'turkey',
            # 'ua':'ukraine',
            # 'uk':'united kingdom',
        }

        self.COUNTRY_CURRENCY = {
            'at':'€',
            'be':'€',
            'hr':'€',
            'cz':'€',
            'dk':'€',
            'fr':'€',
            'de':'€',
            'hu':'€',
            'it':'€',
            'nl':'€',
            'pl':'zl',
            'pt':'€',
            'ro':'€',
            'sk':'€',
            'es':'€',
            'ch':'€',
            'tr':'€',
            'ua':'€',
            'uk':'€',
        } 
        
        # Defines which languages use capitalization for nouns 
        self.UPPERCASE_LANG = { 
            'at':True,
            'be':False,
            'ch':True,
            'hr':False,
            'cz':False,
            'de':True,
            'dk':False,
            'fr':False,
            'hu':False,
            'it':False,
            'nl':False,
            'pl':False,
            'pt':False,
            'ro':False,
            'sk':False,
            'es':False,
            'tr':False,
            'ua':False,
            'uk':False
        }