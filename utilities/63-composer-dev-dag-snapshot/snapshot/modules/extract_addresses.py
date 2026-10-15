#!/usr/bin/env python3

# Regular Expressions
import re
import json
import time
import logging 

# HTML parser
from selectolax.parser import HTMLParser
from .db_postgres import PostgresDB, testConnect
from .db_connections import initialize_db_connection

# import string
# import sys
# from pathlib import Path

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO, 
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

class CompanyDataBase():
    '''
    Base class for converting HTML to addresses
    creates the parser and provides the basic properties
    '''
    email = ''
    telefon = ''
    firma1 = ''
    firma2 = ''
    firma3 = ''
    adresse = ''
    lkz = ''
    plz = ''
    ort = ''
    com_reg_entry = ''
    vat_id = ''
    manager = ''
    manager_position = ''
    location = {'lat': 0, 'lon': 0} # Geocodes
    _searchelements = {}  # Dictionary with search elements
    _checkerresult = ""   # contains the check results of the associated checker objects

    _htmlparser: HTMLParser = None
    _html_cleaned = ''    # contains the "cleaned" text from the html after the init method

    def __init__(self, ahtml: str) -> None:
        '''
        cleans the passed HTML and stores the text in _html_cleaned
        '''
        # replace br-Tags with real line breaks, otherwise the parser removes them
        ahtml = re.sub(r"<br ?/?>", r"\n", ahtml, flags=re.IGNORECASE)
        # ...the first time it doesn't catch all for some reason
        ahtml = re.sub(r"<br ?/?>", r"\n", ahtml)
        # Add line break in p-Tag so that the lines are preserved
        ahtml = re.sub(r"(</?p>)", r"\n\1", ahtml)
        # Add line break in span-Tag so that the lines are preserved
        ahtml = re.sub(r"</span", r"\n</span", ahtml)
        # Add line break in h-Tag so that the lines are preserved
        ahtml = re.sub(r"</h", r"\n</h", ahtml)
        # Add line break in div-Tag so that the lines are preserved
        ahtml = re.sub(r"</div", r"\n</div", ahtml)
        # Convert nbsp to space
        ahtml = re.sub(r"\xa0", r" ", ahtml)
        # print(ahtml)
        # Save the original parser, further down script and style tags are removed
        self._htmlparser = HTMLParser(ahtml)
        htmlparser = HTMLParser(ahtml)
        # Remove script and style tags
        htmlparser.strip_tags(['script', 'style'])
        try:
            self._html_cleaned = htmlparser.text(True, ' ', False)
        except (AttributeError):
            self._html_cleaned = ''
        # Convert tabs to line breaks
        self._html_cleaned = re.sub(r"\t", r"\n", self._html_cleaned)
        # Convert 4 or more spaces to line breaks
        self._html_cleaned = re.sub(r" {4,}", r"\n", self._html_cleaned)
        # Remove multiple spaces or tabs at the beginning of a line
        self._html_cleaned = re.sub(r"\n( |\t)+", r"\n", self._html_cleaned)
        # print(self._html_cleaned)
            
    def _rclean(self, ahtml: str) -> str:
        '''
        remove non-printable and empty characters at the end of ahtml
        '''
        return re.sub(r"(\n| |\t|\r){1,2}$", r"", ahtml)

    def _getCheckerResult(self, all_results: bool=False) -> str:
        '''
        returns the messages from the associated checker objects
        if all_results=True, also the results of failed attempts are returned
        '''
        if all_results:
            return self._checkerresult
        else:
            result = ""
            for k, v in self._searchelements.items():
                if v.checker:
                    result += v.checker.message
            return result       

    def getSearchtime(self) -> str:
        '''
        returns the calculation times for each SearchElement in ns
        '''
        result = ""
        for k, v in self._searchelements.items():
            result += "\n"+'SearchElement '+k.ljust(25)+' '+'{time:15.3f}'.format(time=v.searchtime/1000/1000)+' s'
        return result       

    def __call__(self) -> str:
        '''
        default method of the class, to return the found content
        '''
        # result = ""                    
        # for k, v in self._searchelements.items():
        #     result = result + k + ": '" + str(v()) + "' "
        result = "Firma1: '"+str(self.firma1)+"', Firma2: '"+str(self.firma2)+"', Firma3: '"+str(self.firma3) + "', Adresse: '"+str(self.adresse)+"', LKZ: '"+\
                    str(self.lkz)+"', PLZ: '"+str(self.plz)+"', Ort: '"+str(self.ort) + "', Email: '"+str(self.email)+"', Telefon: '"+\
                    str(self.telefon)+"', Geocodes: "+str(self.location)+", com_reg_entry:'"+str(self.com_reg_entry)+"', vat_id:'"+str(self.vat_id)+"', manager_position:'"+str(self.manager_position)+"', manager:'"+str(self.manager)+"'"
        return result

class CompanyDataCheckerBase():
    '''
    Base class to check CompanyData
    in derived classes only the check method must be overridden
    error messages can be stored in the associated CompanyData object via _addmessage in the message property
    '''
    _CompanyData: CompanyDataBase #private variable to store the CompanyData object
    message: str = ""

    def __init__(self, aCompanyData: CompanyDataBase) -> None:
        self._CompanyData = aCompanyData
        self.message = ""

    def _addmessage(self, msg: str) -> None: #private method to add a message to the associated CompanyData object and the own message
        '''
        pass the given message to the associated CompanyData object and write it to the own message at the same time
        message is cleared with each call of the check method and therefore only contains the last message
        '''
        self.message += msg + ";"
        self._CompanyData._checkerresult += msg + ";"
     
    def resetresultfield(self, fieldname: str) -> None:
        '''
        reset the field content, can be used within the check function
        '''
        if fieldname in self._CompanyData._searchelements:
            self._CompanyData._searchelements[fieldname].resetresult()

        if fieldname == "plzort":
            self._CompanyData.plz = ""
            self._CompanyData.ort = ""
        elif fieldname == "plzortadresse":
            self._CompanyData.adresse = ""
            self._CompanyData.plz = ""
            self._CompanyData.ort = ""
        elif fieldname == "adresse":
            self._CompanyData.adresse = ""
        elif fieldname == "firma1":
            self._CompanyData.firma1 = ""
        elif fieldname == "firma2":
            self._CompanyData.firma2 = ""
        elif fieldname == "firma3":
            self._CompanyData.firma3 = ""
        elif fieldname == "email":
            self._CompanyData.email = ""
        elif fieldname == "telefon":
            self._CompanyData.telefon = ""
        elif fieldname == "location":
            self._CompanyData.location = {'lat': 0, 'lon': 0}
        elif fieldname == "com_reg_entry":
            self._CompanyData.com_reg_entry = ""
        elif fieldname == "vat_id":
            self._CompanyData.vat_id = ""
        elif fieldname == "manager":
            self._CompanyData.manager = ""
        elif fieldname == "manager_position":
            self._CompanyData.manager_position = ""

    def check(self) -> bool:
        '''
        Base method, is overridden in derived classes
        Here the current results from the searchelements are written to the target variables of the CompanyData object so that
        they can be used for checking in the derived classes
        '''
        # set all results fresh
        if 'plzort' in self._CompanyData._searchelements:
            self._CompanyData.plz = self._CompanyData._searchelements['plzort']()[0]
            self._CompanyData.ort = self._CompanyData._searchelements['plzort']()[1]
        if 'adresse' in self._CompanyData._searchelements:
            self._CompanyData.adresse = self._CompanyData._searchelements['adresse']()
        if 'plzortadresse' in self._CompanyData._searchelements:
            address = self._CompanyData._searchelements['plzortadresse']()      
            if "streetAddress" in address and isinstance(address["streetAddress"], str):
                self._CompanyData.adresse = address["streetAddress"]
            if "addressCountry" in address and isinstance(address["addressCountry"], str):
                self._CompanyData.lkz = address["addressCountry"]
            if "postalCode" in address and isinstance(address["postalCode"], str):
                self._CompanyData.plz = address["postalCode"]
            if "addressLocality" in address and isinstance(address["addressLocality"], str):
                self._CompanyData.ort = address["addressLocality"]
        if 'firma1' in self._CompanyData._searchelements:
            self._CompanyData.firma1 = self._CompanyData._searchelements['firma1']()
        if 'firma2' in self._CompanyData._searchelements:
            self._CompanyData.firma2 = self._CompanyData._searchelements['firma2']()
        if 'firma3' in self._CompanyData._searchelements:
            self._CompanyData.firma3 = self._CompanyData._searchelements['firma3']()
        if 'email' in self._CompanyData._searchelements:
            self._CompanyData.email = self._CompanyData._searchelements['email']()
        if 'telefon' in self._CompanyData._searchelements:
            self._CompanyData.telefon = self._CompanyData._searchelements['telefon']()
        if 'com_reg_entry' in self._CompanyData._searchelements:
            self._CompanyData.com_reg_entry = self._CompanyData._searchelements['com_reg_entry']()
        if 'vat_id' in self._CompanyData._searchelements:
            self._CompanyData.vat_id = self._CompanyData._searchelements['vat_id']()
        if 'location' in self._CompanyData._searchelements:
            self._CompanyData.location.lat = self._CompanyData._searchelements['latitude']()
            self._CompanyData.location.lon = self._CompanyData._searchelements['longitude']()
        # find the correct entry for manager and manager_position
        x=0
        while self._CompanyData.manager_position == '' and self._CompanyData.manager == '' and x<len(self._CompanyData._re_manager):
            self._CompanyData.manager_position = (self._CompanyData._searchelements['manager'+str(x)]()[0]).strip()
            self._CompanyData.manager          = (self._CompanyData._searchelements['manager'+str(x)]()[1]).strip()
            x=x+1

        self.message = ""
        return True

class CompanyDataCheckerAdresse(CompanyDataCheckerBase):
    '''
    Check the address for plausibility
    '''

    def check(self) -> bool:
        result = super().check()
        if not self._CompanyData.plz:
            result = False
            self._addmessage("PLZ leer")
        if not self._CompanyData.ort:
            result = False
            self._addmessage("Ort leer")
        if len(self._CompanyData.ort) > 80:
            result = False
            self._addmessage("Ort > 80 Zeichen")
        if not self._CompanyData.adresse:
            result = False
            self._addmessage("Adresse leer")
        if len(self._CompanyData.adresse) > 80:
            result = False
            self._addmessage("Adresse > 80 Zeichen")
        if len(re.findall(r"[a-z]+", self._CompanyData.adresse)) == 0:
            self._addmessage("Adresse enthält keine Buchstaben")
            result = False        
        if not result:
             self.resetresultfield("plzort")
             self.resetresultfield("adresse")
        return result

class CompanyDataCheckerFirma(CompanyDataCheckerBase):
    '''
    Check the Firma1,2,3 for plausibility
    '''

    def check(self) -> bool:
        result = super().check()
        if len(self._CompanyData.firma1 + self._CompanyData.firma2 + self._CompanyData.firma3) > 3*60:
            self._addmessage("Firma1-3 > 180 Zeichen")
            result = False
        if len(self._CompanyData.firma1 + self._CompanyData.firma2 + self._CompanyData.firma3) == 0:
            self._addmessage("Firma1-3 leer")
            result = False
        # no line breaks in company name
        if len(re.findall(r"\n", self._CompanyData.firma1 + self._CompanyData.firma2 + self._CompanyData.firma3)) > 0:
            self._addmessage("Firma1-3 enthält Umbrüche")
            result = False        
        # not more than 10 words per company
        if len(re.findall(r"\w+", self._CompanyData.firma1)) > 8:
            self._addmessage("Firma1 > 8 Worte:"+self._CompanyData.firma1)
            result = False        
        if len(re.findall(r"\w+", self._CompanyData.firma2)) > 8:
            self._addmessage("Firma2 > 8 Worte:"+self._CompanyData.firma2)
            result = False        
        if len(re.findall(r"\w+", self._CompanyData.firma3)) > 8:
            self._addmessage("Firma3 > 8 Worte:"+self._CompanyData.firma3)
            result = False        
        if not result:
            self.resetresultfield("firma1")
            self.resetresultfield("firma2")
            self.resetresultfield("firma3")
        return result

class CompanyDataCheckerManager(CompanyDataCheckerBase):
    '''
    Check the manager for plausibility
    '''

    def check(self) -> bool:
        result = super().check()
        self._addmessage("CompanyDataCheckerManager:"+self._CompanyData.manager)
        if len(self._CompanyData.manager) > 120:
            self._addmessage("Manager > 120 Zeichen")
            result = False
        # no line breaks
        if len(re.findall(r"\n", self._CompanyData.manager)) > 0:
            self._addmessage("Manager enthält Umbrüche")
            result = False        
        # no question marks
        if len(re.findall(r"\?|§|:", self._CompanyData.manager)) > 0:
            self._addmessage("Manager enthält Sonderzeicher")
            result = False        
        # at least 3 letters 
        if len(re.findall(r"[a-z]", self._CompanyData.manager)) < 4:
            self._addmessage("Manager < 4 Buchstaben:"+self._CompanyData.manager)
            result = False        
        # at least 2 words 
        if len(re.findall(r"\w+", self._CompanyData.manager)) < 2:
            self._addmessage("Manager < 2 Worte:"+self._CompanyData.manager)
            result = False        
        # not more than 10 words 
        if len(re.findall(r"\w+", self._CompanyData.manager)) > 10:
            self._addmessage("Manager > 10 Worte:"+self._CompanyData.manager)
            result = False        
        if len(re.findall(r"ist ausschließlich der$|des Unternehmens:?|^die Geschäftsführer$|^Der Vorstand$|^und Team$|^1. Vorsitzender:?$|^1. Vorstand$|^EU-Streitschlichtung$", self._CompanyData.manager)) > 0:
            self._addmessage("Manager enthält keinen Namen:"+self._CompanyData.manager)
            result = False                    
        if not result:
            self.resetresultfield("manager_position")
            self.resetresultfield("manager")
        return result

class CompanyDataCheckerTelefon(CompanyDataCheckerBase):
    '''
    Check the phone number for plausibility
    '''

    def check(self) -> bool:
        result = super().check()
        if len(re.findall(r"\n", self._CompanyData.telefon)) > 0:
            self._addmessage("Telefon enthält Umbrüche")
            result = False        
        if len(re.findall(r"[0-9]", self._CompanyData.telefon)) < 8:
            self._addmessage("Telefon enthält weiniger als 8 Ziffern")
            result = False        
        if len(re.findall(r"^(0|\(|\+)", self._CompanyData.telefon)) == 0:
            self._addmessage("Telefon beginnt nicht mit 0(+")
            result = False        
        if not result:
            self.resetresultfield("telefon")
        return result

class SearchElementBase():
    '''
    Base class for searching an element
    (was only created so that the childelement can be defined in the derived class)
    '''
    drop_element = True  # delete found string from HTML
    drop_html_before = False # delete HTML before the found one
    drop_html_after = False # delete HTML after the found one

    checker: CompanyDataCheckerBase = None # Checker object for checking the data
    _html = ""
    _result = ""  # result
    _starttime = 0 # timestamp to remember when starting the search method
    searchtime = 0 

    def __init__(self) -> None:
        self._searchtime = 0

    def endsearch(self) -> None:
        '''
        Must always be called at the end of search to stop the time
        '''
        self.searchtime += time.time_ns() - self._starttime 

    def search(self, ahtml: str) -> str:
        '''
        central method for determining hits
        '''
        self._starttime = time.time_ns()
        self._html = ahtml
        return self._html

    def resetresult(self) -> None:
        '''
        empty result
        '''
        self._result = ''

    def __call__(self) -> str:
        return self._result

class SearchElement(SearchElementBase):
    '''
    Class for searching an element
    Searches for a regular expression _re in _html
    Via drop_element, drop_html_before and drop_html_after, only the found element and/or everything before or after it can be deleted from the HTML
    Via childelement, dependent SearchElement objects can be linked.

    Via the property checker, a corresponding Checker object can be assigned, which checks several fields for plausibility.
    If a childelement is specified, it will be searched until all checker objects of the following childelements have found all fields in order
    found. This allows multiple fields to be checked in relation (e.g. PLZ, city and street)
    '''
    search_flags = re.IGNORECASE
    debugfile: str ="" # Filename in which the HTML is written for debugging purposes
    childelement: SearchElementBase = None # dependent SearchElement, for which the search() method is also called

    _re = ""  
    _resultelements = []  # if regexp.match should not return group() but group(2) or further
    _matches = [] # found matches
    _resultindex = -1 # hit index for result, is incremented for multiple searches
    foundresult = "" # preset result, if set, exactly this result is searched for and all subsequent childelements are processed accordingly
    
    def __init__(self, regex: str, resultelements=[]) -> None:
        self._re = regex
        self._resultelements = resultelements
        self._result = self._emptyresult()
     
    def check(self) -> bool:
        '''
        Call the check-method of the associated checker object and all childelements
        '''
        result = True
        if self.checker:
            result = self.checker.check()
            self.writedebug("\nself.checker.message: "+self.checker.message)
        if self.childelement:
            result = result and self.childelement.check()
        return result
    
    def writedebug(self, msg: str, mode: str='a') -> None:
        '''
        write to debugfile if defined
        '''
        if self.debugfile:
            with open(self.debugfile, mode, encoding="utf-8") as f:
                f.write(msg)        
    
    def _emptyresult(self):
        '''
        return empty result, depending on the expected result type (_resultelements)
        '''
        result = ""
        # build empty array, for multiple elements to search for
        if len(self._resultelements)>1:
            result = []
            for x in self._resultelements:
                result.append('')
        return result
    
    def resetresult(self) -> None:
        '''
        empty result
        '''
        self._result = self._emptyresult()
        if self.childelement:
            self.childelement.resetresult()
    
    def _rclean(self, ahtml: str) -> str:
        '''
        remove non-printable and empty characters at the end of ahtml
        '''
        #return re.sub(r"(\n| |\t|\r){1,2}$", r"", ahtml)
        ahtml = re.sub(r"(\n| |\t|\r)$", r"", ahtml)
        ahtml = re.sub(r"(\n| |\t|\r)$", r"", ahtml)
        return ahtml
    
    def _getresult(self, occurence: int=0):
        '''
        extract regexp-Match-result from self._matches and reduce self._html accordingly
        '''
        result = self._emptyresult()
        if len(self._matches) > 0 and occurence < len(self._matches):
            self.writedebug("\nself._matches["+str(occurence)+"]: "+str(self._matches[occurence]))
            #depending on the desired result, provide as string or array
            self.writedebug("\nlen(self._resultelements): "+str(len(self._resultelements)))
            self.writedebug("\nself._matches["+str(occurence)+"]: "+str(self._matches[occurence]))
            self.writedebug("\nself._matches["+str(occurence)+"].groups(): "+str(self._matches[occurence].groups()))
            self.writedebug("\nlen(self._matches["+str(occurence)+"].groups()): "+str(len(self._matches[occurence].groups())))
            if len(self._resultelements)==0:
                result = self._matches[occurence].group().strip()
            elif len(self._resultelements)==1:
                result = self._matches[occurence].group(self._resultelements[0]).strip()
            else:
                result = []
                for x in self._resultelements:
                    #if x < len(self._matches[occurence].groups()):
                    self.writedebug("\nself._matches["+str(occurence)+"].group("+str(x)+"): "+str(self._matches[occurence].group(x)))
                    self.writedebug("\nself._matches["+str(occurence)+"].group("+str(x)+").strip(): "+str(self._matches[occurence].group(x).strip()))
                    result.append(self._matches[occurence].group(x).strip())
            #remove found element and HTML before or after
            if self.drop_element:
                self._html = self._rclean(self._html[0:self._matches[occurence].span(0)[0]]) + "\n" + self._rclean(self._html[self._matches[occurence].span(0)[1]:])
            if self.drop_html_after:
                self._html = self._rclean(self._html[0:self._matches[occurence].span(0)[0]])
            if self.drop_html_before:
                self._html = self._rclean(self._html[self._matches[occurence].span(0)[1]:])
        return result

    def getFoundresultinMatch(self, match: re.match)-> bool:
        '''
        returns True if the passed Match-Object matches the preset self.foundresult
        '''
        if len(self._resultelements)==0:
            return self.foundresult == match.group().strip()
        elif len(self._resultelements)==1:
            return self.foundresult == match.group(self._resultelements[0]).strip()
        else:
            result = True
            i = 0
            for x in self._resultelements:
                print('Check "'+self.foundresult[i]+'" =? "'+match.group(x).strip()+'"')
                result = result and self.foundresult[i] == match.group(x).strip()
                i += 1
            return result

    def search(self, ahtml: str) -> str:
        '''
        central method for determining hits
        '''
        self._html = super().search(ahtml)
        # Abort if no regular expression is specified
        if not self._re:
            return self._html
        self.writedebug(self._html, 'w')
        self.writedebug("\nself._re: '"+self._re+"'")
        self._resultindex = -1
        #self._matches = re.search(self._re, self._html, self.search_flags)
        m = re.finditer(self._re, self._html, self.search_flags)
        self._matches = []
        for matchNum, match in enumerate(m, start=0):
            # if foundresult is set, only write matches to foundresult
            if not self.foundresult or self.getFoundresultinMatch(match):
                self._matches.append(match)
                self.writedebug("\nappended: "+str(match))
            else:
                self.writedebug("\nnot appended"+str(match))
        # Return the first hit by default, if a checker is passed, continue searching until satisfied, or no more hits are left
        while True:
            self._resultindex += 1
            # if self._resultindex == 2:
            #     break
            if self._resultindex >= len(self._matches):
                self.writedebug("\nself._resultindex >= len(self._matches)")
                break
            self.writedebug("\nself._resultindex: "+str(self._resultindex)+", len(self._matches): "+str(len(self._matches)))
            self._result = self._getresult(self._resultindex)
            self.writedebug("\nself._html[letzte 4 Zeichen]: '"+self._html[(len(self._html)-4):len(self._html)]+"'")
            # call search-method for dependent SearchElement
            if self.childelement:
                self.writedebug("\nself.childelement.search")
                self._html = self.childelement.search(self._html)
            if self.check():
                self.writedebug("\ncheck()==True")
                break
            else:
                self.writedebug("\nReset Results")
                # reset results and HTML
                self._html = ahtml
                self.resetresult()
        self.endsearch() # zum Timetracking
        return self._html

class SearchElementCSSSelector(SearchElement):
    '''
    Class for searching an element via CSS selector
    '''
    _htmlparser: HTMLParser = None
    _cssselektor: list = []
    _attributename: str = ''
    _jsonflag: bool = False

    def __init__(self, htmlparser: HTMLParser, cssselektor: list, attributename: str, jsonflag: bool=False) -> None:
        super().__init__('')
        self._htmlparser = htmlparser
        self._cssselektor = cssselektor
        self._attributename = attributename
        self._jsonflag = jsonflag

    def search(self, ahtml: str) -> str:
        self._html = super().search(ahtml)
        self._result = ''
        for s in self._cssselektor:
            for a in self._htmlparser.css(s):
                if self._jsonflag:
                    try:
                        nodeattributes = json.loads(a.text())
                    except (json.decoder.JSONDecodeError):
                        nodeattributes = {}
                else:
                    nodeattributes = a.attributes
                if a and nodeattributes and self._attributename in nodeattributes:
                    self._result=nodeattributes[self._attributename]
                    #if result found and checker satisfied, then done
                    if self._result and self.check():
                        self.endsearch() # zum Timetracking
                        return self._html
        self.endsearch() # zum Timetracking
        return self._html

class CompanyDataDE(CompanyDataBase):
    '''
    Class for extracting German addresses
    First searches for email and phone
    For the address, a PLZ-city combination is searched for, then address and company names are identified before it
    '''
    # regular expressions
    _re_email   = r"[A-Za-z\.\-\_\(\)]+([0-9]*)(@|&#064;|\(at\)|\[at\])[a-zA-Z0-9_\-]+(\.|\(dot\))(de|com|net|eu)"
    _re_telefon = r"(tel(e(f|ph)one?)?|((k|c)onta(k|c)t)|mobile?|t|(f|ph)one?)[.:]*\s?(\+?[\.\-\+\s\d\(\)\\u2014\/(\&\#8211;)]{8,50}\d)"
    _re_com_reg_entry = [r"((NIP)(.{0,20})((\d{3})[ -]?(\d{3})[ -]?(\d{2})[ -]?(\d{2})))",
                         r"((OIB)(.{0,20})(\d{11}))",
                         r"((HRA|HRB)(\xA0| )?((\d{4,6})(\xA0| )?([A-Z]{2,3})?))"#,
                     #    r"((DE) ?((\d{3}) ?(\d{3}) ?(\d{3})))",
                         ]
    _re_vat_id =   [r"\W((DE)\s?((\d{3})\s?(\d{3})\s?(\d{3})))\W",
                    r"\W((ATU)\s?((\d{4})\s?(\d{4})))\W",
                    r"\W((FR)\s?(([A-Z0-9]{2}\d)\s?(\d{3})\s?(\d{3})\s?(\d{2})))\W",
                    r"\W((BE)\s?((\d{3})\s?(\d{3})\s?(\d{4})))\W",
                    r"\W((CZ)\s?((\d{8,10})))\W",
                    r"\W((ES)\s?(([A-Z])(\d{7})([A-Z]|[0-9])))\W",
                    r"\W((HR)\s?((\d{3})\s?(\d{3})\s?(\d{3})\s?(\d{2})))\W",
                    r"\W((HU)\s?((\d{4})\s?(\d{4})))\W",
                    r"\W((IT)\s?((\d{3})\s?(\d{3})\s?(\d{3})\s?(\d{2})))\W",
                    r"\W((NL)\s?((\d{4})\s?(\d{4})\s?(\d{4})))\W",
                    r"\W((PL)\s?((\d{3})\s?(\d{3})\s?(\d{4})))\W",
                    r"\W((PT)\s?((\d{3})\s?(\d{3})\s?(\d{3})))\W",
                    r"\W((SK)\s?((\d{3})\s?(\d{3})\s?(\d{4})))\W",
                    r"\W((RO)\s?((\d{2,10})))\W"
                  ]
    _re_manager = [r"\n(Gesetzlicher Ver?treter):?\W*(.+)", 
                    r"\n(Vertret.+ Geschäftsführer[in]{0,2}):?\W*(.+)", 
                    r"\n(Vertret.+ Vorstand):?\W*(.+)", 
                    r"\n(Vertret.+ Gesellschafter[in]{0,2}):?\W*(.+)", 
                    r"\n(Vertreten durch):?\W*(.+)", 
                    r"\n(Geschäftsführer[in]{0,2}\s.*\sInhalt\s.+§ ?5 TMG):?\W*(.+)", 
                    r"\n(Geschäftsführer[in]{0,2} und verantwortlich für den Inhalt):?\W*(.+)", 
                    r"\n(Geschäftsführer[in]{0,2} und Inhaber[in]{0,2}):?\W*(.+)", 
                    r"\n(Geschäftsführer[in]{0,2}.+Gesellschaft):?\W*(.+)", 
                    r"\n(Geschäftsführer[in]{0,2}):?\W*(.+)", 
                    r"\n(GeschäftsführerIn/InhaberIn):?\W*(.+)", 
                    r"\n(Inhaber[in]{0,2} .+d.+ Inhalt):?\W*(.+)", 
                    r"\n(Inhaber[in]{0,2} d.+se?iten?):?\W*(.+)", 
                    r"\n(Inhaber[in]{0,2} und.+Verantwortlich.+\s.+§ ?5.+TMG.?):?\W*(.+)", 
                    r"\n(Inhaber[in]{0,2} und.+Verantwortlich.+\s.+§ ?5.+RStV.?):?\W*(.+)",
                    r"\n(Inhaber[in]{0,2} und.+Verantwort?lich[er]{0,2}):?\W*(.+)", 
                    r"\n(Inhaber[in]{0,2}):?\W*(.+)", 
                    r"\n(Vertretungsberechtigte\(r\)):?\W*(.+)", 
                    r"\n(Vertretungsberechtigter?):?\W*(.+)", 
                    r"\n\(?(Inh\.):?\W*(.+)\)?", 
                    r"\n(Geschäftsinhaber[in]{0,2}):?\W*(.+)", 
                    r"\n(Verantwortlich.+für Inhalte und Bilder) sind:?\W*(.+)", 
                    r"\n(Verantwortlich.+§ ?55 .+ RStV):?\W*(.+)", 
                    r"\n(Verantwortlich[er]{0,2} für.+Inhalt.+§ ?18.+MStV):?\W*(.+)", 
                    r"\n(Verantwortlich[er]{0,2} für.+Inhalt.+§ ?5 TMG):?\W*(.+)", 
                    r"\n(Verantwortlich[er]{0,2} für.+Inhalt d.+Se?iten?):?\W*(.+)", 
                    r"\n(Verantwortlich[er]{0,2} für.+Inhalt):?\W*(.+)", 
                    r"\n(Verantwortlich[er]{0,2} für das Shopsystem.+§ ?6 Abs\.1 Mediendienste-Staatsvertrages) ist:?\W*(.+)"
                  ]
    _re_plz_ort = r"(DE? ?\-|DE|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    _re_adresse = r"([\w &\.\+\-\(\),\/-]+.?)$"
    _re_firma2  = r"([\w &\.\*,:\-\(\)'-]+.?)$"
    _re_firma1  = r"([\w &\.\*,:\-\(\)'-]+.?)$"

    defaultlkz = 'DE'

    def __init__(self, ahtml: str) -> None:
        '''
        Initialize searchelements and link them to each other
        '''
        super().__init__(ahtml)
        # Initialize searchelements
        self._searchelements['geocode'] = SearchElementCSSSelector(self._htmlparser, [r"meta[name='geo.position']", r"meta[name='ICBM']", r"meta[name='icbm']"], 'content') 
        self._searchelements['latitude'] = SearchElementCSSSelector(self._htmlparser, [r"meta[itemprop='latitude']"], 'content') 
        self._searchelements['longitude'] = SearchElementCSSSelector(self._htmlparser, [r"meta[itemprop='longitude']"], 'content') 
        self._searchelements['plzortadresse'] = SearchElementCSSSelector(self._htmlparser, [r"script[type='application/ld+json']"], 'address', jsonflag=True) 

        self._searchelements['email']   = SearchElement(self._re_email) 
        self._searchelements['telefon'] = SearchElement(self._re_telefon, [8])  
        self._searchelements['telefon'].debugfile = 'telefon.txt'
        for x in range(0, len(self._re_com_reg_entry)):
            self._searchelements['com_reg_entry'+str(x)] = SearchElement(self._re_com_reg_entry[x], [2,4])  
            self._searchelements['com_reg_entry'+str(x)].debugfile = 'com_reg_entry'+str(x)+'.txt'
            self._searchelements['com_reg_entry'+str(x)].checker = None
        for x in range(0, len(self._re_vat_id)):
            self._searchelements['vat_id'+str(x)] = SearchElement(self._re_vat_id[x], [2,3])  
            self._searchelements['vat_id'+str(x)].debugfile = 'vat_id'+str(x)+'.txt'
            self._searchelements['vat_id'+str(x)].checker = None
        for x in range(0, len(self._re_manager)):
            self._searchelements['manager'+str(x)] = SearchElement(self._re_manager[x], [1,2])  
            self._searchelements['manager'+str(x)].debugfile = 'manager'+str(x)+'.txt'
            self._searchelements['manager'+str(x)].checker = CompanyDataCheckerManager(self)
        self._searchelements['plzort']  = SearchElement(self._re_plz_ort, [2,3])
        self._searchelements['adresse'] = SearchElement(self._re_adresse)
        self._searchelements['firma2']  = SearchElement(self._re_firma2)
        self._searchelements['firma1']  = SearchElement(self._re_firma1)

        self._searchelements['plzort'].drop_html_after = True
        self._searchelements['adresse'].drop_html_after = True
        self._searchelements['firma2'].drop_html_after = True

        # Define dependencies of elements:
        # Search Firma1 before Firma2
        self._searchelements['firma2'].childelement = self._searchelements['firma1']
        # Search Firma2 before Adresse
        self._searchelements['adresse'].childelement = self._searchelements['firma2']
        # Adresse vor PLZ-Ort suchen
        self._searchelements['plzort'].childelement = self._searchelements['adresse']

        # Define debugfiles
        self._searchelements['plzort'].debugfile = 'plzort.txt'
        self._searchelements['adresse'].debugfile = 'adresse.txt'
        self._searchelements['firma1'].debugfile = 'firma1.txt'

        # Create checker objects and pass them to the searchelements
        self._searchelements['plzortadresse'].checker = CompanyDataCheckerAdresse(self)
        self._searchelements['geocode'].checker = None
        self._searchelements['email'].checker = None
        self._searchelements['telefon'].checker = CompanyDataCheckerTelefon(self)
        self._searchelements['adresse'].checker = CompanyDataCheckerAdresse(self)
        self._searchelements['firma1'].checker =  CompanyDataCheckerFirma(self)

    def search(self) -> None:
        # since the html is changed/shortened by the search, put it in an extra variable
        html_cleaned2 = self._html_cleaned

        # Search geocodes
        html_cleaned2=self._searchelements['geocode'].search(html_cleaned2)
        geocode_result = self._searchelements['geocode']()
        
        # Handle None geocode result
        if geocode_result and geocode_result.strip():
            geocodes = geocode_result.split(',')
            #print(len(geocodes))
            #print(geocodes)
            if len(geocodes)==2:
                self.location['lat'] = geocodes[0]
                self.location['lon'] = geocodes[1]
            elif len(geocodes)==1:
                geocodes2=geocodes[0].split(';')
                #Special case: coordinates separated by semicolon
                if len(geocodes2)==2:
                    self.location['lat'] = geocodes2[0]
                    self.location['lon'] = geocodes2[1]
                else:
                    self.location['lat'] = ''
                    self.location['lon'] = ''
                    #search for lat/lon separated
                    html_cleaned2=self._searchelements['latitude'].search(html_cleaned2)
                    lat_result = self._searchelements['latitude']()
                    self.location['lat'] = lat_result if lat_result else ''
                    html_cleaned2=self._searchelements['longitude'].search(html_cleaned2)
                    lon_result = self._searchelements['longitude']()
                    self.location['lon'] = lon_result if lon_result else ''
        else:
            # No geocode found, try lat/lon separately
            self.location['lat'] = ''
            self.location['lon'] = ''
            html_cleaned2=self._searchelements['latitude'].search(html_cleaned2)
            lat_result = self._searchelements['latitude']()
            self.location['lat'] = lat_result if lat_result else ''
            html_cleaned2=self._searchelements['longitude'].search(html_cleaned2)
            lon_result = self._searchelements['longitude']()
            self.location['lon'] = lon_result if lon_result else ''

        # search for email
        html_cleaned2 = self._searchelements['email'].search(html_cleaned2)
        self.email = self._searchelements['email']()

        # search for phone
        html_cleaned2 = self._searchelements['telefon'].search(html_cleaned2)
        self.telefon = self._searchelements['telefon']()

        # search for com_reg_entry
        self.com_reg_entry = ''
        x=0
        while self.com_reg_entry == '' and x<len(self._re_com_reg_entry):
            html_cleaned2 = self._searchelements['com_reg_entry'+str(x)].search(html_cleaned2)
            self.com_reg_entry = (self._searchelements['com_reg_entry'+str(x)]()[0]+ ' ' +self._searchelements['com_reg_entry'+str(x)]()[1]).strip()
            x=x+1

        # search for vat_id
        self.vat_id = ''
        x=0
        while self.vat_id == '' and x<len(self._re_vat_id):
            html_cleaned2 = self._searchelements['vat_id'+str(x)].search(html_cleaned2)
            self.vat_id = (self._searchelements['vat_id'+str(x)]()[0]+ ' ' +self._searchelements['vat_id'+str(x)]()[1]).strip()
            x=x+1

        # search for manager
        self.manager_position = ''
        self.manager = ''
        x=0
        while self.manager_position == '' and self.manager == '' and x<len(self._re_manager):
            html_cleaned2 = self._searchelements['manager'+str(x)].search(html_cleaned2)
            self.manager_position = (self._searchelements['manager'+str(x)]()[0]).strip()
            self.manager          = (self._searchelements['manager'+str(x)]()[1]).strip()
            x=x+1

        # search for JSON address
        html_cleaned2 = self._searchelements['plzortadresse'].search(html_cleaned2)
        address = self._searchelements['plzortadresse']()
        if "streetAddress" in address:
            self.adresse = address["streetAddress"]
        if "addressCountry" in address:
            self.lkz = address["addressCountry"]
        if "postalCode" in address:
            self.plz = address["postalCode"]
        if "addressLocality" in address:
            self.ort = address["addressLocality"]
        
        #PLZOrt not preset, since it is sometimes written differently
        #if "postalCode" in address and "addressLocality" in address:
        #    self._searchelements['plzort'].foundresult = [self.plz, self.ort]
        # here it is defined that he searches for the company name before the address found in the JSON
        if "streetAddress" in address:
            self._searchelements['adresse'].foundresult = self.adresse

        # search for PLZ-city combination
        html_cleaned2  = self._searchelements['plzort'].search(html_cleaned2)
        self.plz = self._searchelements['plzort']()[0]
        self.ort = self._searchelements['plzort']()[1]

        # extract results of dependent fields
        self.adresse = self._searchelements['adresse']()
        
        self.firma2 = self._clean_firma(self._searchelements['firma2']())
        self.firma1 = self._clean_firma(self._searchelements['firma1']())

        # sort company fields if firma1 is empty
        if not self.firma1:
            self.firma1 = self.firma2
            self.firma2 = self.firma3
            self.firma3 = ''

        if not self.firma2:
            self.firma2 = self.firma3
            self.firma3 = ''
        # Default LKZ only set if address is found
        if (not self.lkz or not isinstance(self.lkz, str)) and self.adresse and self.plz:
            self.lkz = self.defaultlkz
    
    def _clean_firma(self, afirma: str) -> str:
        '''
        remove company name components from company name
        '''
        afirma = afirma.strip()
        afirma = re.sub(r"5 TMG:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Angaben gemäß §", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"IMPRESSUM:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"(Haus)?Anschrift:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"(Betreiber und )?Kontakt:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Name:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Pflichtangaben:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Betreibergesellschaft:?", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Vertretungsberechtigte Gesellschafter:?", r"", afirma, flags=re.IGNORECASE)

        #diese dann ganz rauswerfen
        afirma = re.sub(r"Sitz der Gesellschaft:?.*", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Firmensitz:?.*", r"", afirma, flags=re.IGNORECASE)
        afirma = re.sub(r"Adresse:?.*", r"", afirma, flags=re.IGNORECASE)
        return afirma.strip()

class CompanyDataAT(CompanyDataDE):
    '''
    Class for extracting Austrian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(AT? ?\-|AT|[^0-9]) ?([0-9]{4}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'AT'

    # def __init__(self, ahtml: str) -> None:
    #     '''
    #     Initialize searchelements and link them to each other
    #     '''
    #     super().__init__(ahtml)

class CompanyDataBE(CompanyDataDE):
    '''
    Class for extracting Belgian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(BE? ?\-|BE|[^0-9]) ?([0-9]{4}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'BE'

class CompanyDataIT(CompanyDataDE):
    '''
    Class for extracting Italian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(IT? ?\-|IT|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'IT'

class CompanyDataPL(CompanyDataDE):
    '''
    Class for extracting Italian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(PL? ?\-|PL|[^0-9]) ?([0-9]{2} ?\-?–? ?[0-9]{3}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'PL'

class CompanyDataPT(CompanyDataDE):
    '''
    Class for extracting Portuguese addresses
    '''
    # regular expressions
    _re_plz_ort = r"(PT? ?\-|PT|[^0-9]) ?([0-9]{4} ?\-?–? ?[0-9]{3}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'PT'

class CompanyDataES(CompanyDataDE):
    '''
    Class for extracting Spanish addresses
    '''
    # regular expressions
    _re_plz_ort = r"(ES? ?\-|ES|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'ES'

class CompanyDataFR(CompanyDataDE):
    '''
    Class for extracting French addresses
    '''
    # regular expressions
    _re_plz_ort = r"(FR? ?\-|FR|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'FR'

class CompanyDataSK(CompanyDataDE):
    '''
    Class for extracting Slovenian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(SK? ?\-|SK|[^0-9]) ?([0-9]{3}) ?\-?–? ?([0-9]{2}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'SK'

class CompanyDataHU(CompanyDataDE):
    '''
    Class for extracting Hungarian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(HU? ?\-|HU|[^0-9]) ?([0-9]{4}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'HU'

class CompanyDataHR(CompanyDataDE):
    '''
    Class for extracting Croatian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(HR? ?\-|HR|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'HR'

class CompanyDataRO(CompanyDataDE):
    '''
    Class for extracting Romanian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(RO? ?\-|RO|[^0-9]) ?([0-9]{6}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'RO'

class CompanyDataCZ(CompanyDataDE):
    '''
    Class for extracting Czech addresses
    '''
    # regular expressions
    _re_plz_ort = r"(CZ? ?\-|CZ|[^0-9]) ?([0-9]{3}) ?\-?–? ?([0-9]{2}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'CZ'

class CompanyDataNL(CompanyDataDE):
    '''
    Class for extracting Dutch addresses
    '''
    # regular expressions
    _re_plz_ort = r"(NL? ?\-|NL|[^0-9]) ?([0-9]{4}) ?([A-Z]{2}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'NL'

class CompanyDataTR(CompanyDataDE):
    '''
    Class for extracting Turkish addresses
    '''
    # regular expressions
    _re_plz_ort = r"(TR? ?\-|TR|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'TR'

class CompanyDataCH(CompanyDataDE):
    '''
    Class for extracting Swiss addresses
    '''
    # regular expressions
    _re_plz_ort = r"(CH? ?\-|CH|[^0-9]) ?([0-9]{4}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'CH'

class CompanyDataDK(CompanyDataDE):
    '''
    Class for extracting Danish addresses
    '''
    # regular expressions
    _re_plz_ort = r"(DK? ?\-|DK|[^0-9]) ?([0-9]{4}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'DK'

class CompanyDataUA(CompanyDataDE):
    '''
    Class for extracting Ukrainian addresses
    '''
    # regular expressions
    _re_plz_ort = r"(UA? ?\-|UA|[^0-9]) ?([0-9]{5}) ([a-zäöüß \-]{3,60})"
    defaultlkz = 'UA'

#class AddressExtractor(Console):
class AddressExtractor():
    '''
    '''
    db = None
    c = None
    argmap = {
        "shell": "Shell",
		"anzahl": "Anzahl",
        "idx": "IDX",
        "parent_idx": "PARENT_IDX",
        "country": "COUNTRY",
        #"test": "Test",   #to enable or disable test mode. If test mode is enabled, no data is written to the database.
        "verbose": "Verbose",
        "feedback": "Feedback",
        "user": "User",
        "pw": "PW",
        "host": "Host",
        "port":"Port",
        "db":"DB"

    }
    quellenid = 5  # Source ID for new objects
    extidtypid = 5 # ExtID type ID for new objects
   
    def __init__(self, conn_id: str = "google_alloydb_dev", limit: int = 1000):
        '''
        Constructor
        '''
        super().__init__()
        self.conn_id = conn_id
        self.limit = limit
        try:
            self.conn = initialize_db_connection()
            self.db = self.conn
        except Exception as e:
            logging.error(f'No database Connection established: {str(e)}')
            

    # This function is designed to find and return all subclasses of a given class (classType) that are defined in the same module as the caller.
    # return list of tuples (objclass, name) containing all subclasses in callers' module
    # Calling FindAllSubclasses(CompanyDataBase) will return [CompanyDataDE, CompanyDataFR,etc].
    def FindAllSubclasses(self, classType):
        import sys, inspect
        subclasses = []
        callers_module = sys._getframe(1).f_globals['__name__']
        classes = inspect.getmembers(sys.modules[callers_module], inspect.isclass)
        for name, obj in classes:
            if (obj is not classType) and (classType in inspect.getmro(obj)):
                subclasses.append(obj)
        return subclasses

    def get_address(self, aLKZ, ahtml):
        '''
        Filter addresses from ahtml
        '''
        self.c = None #reset for safety
        #call all derived classes from CompanyDataBase and depending on the LKZ/DefaultLKZ in the class
        for subsclass in self.FindAllSubclasses(CompanyDataBase):
            #print(subsclass.defaultlkz)
            if aLKZ == subsclass.defaultlkz:
                self.c = subsclass(ahtml)
        if not self.c:
            available_countries = [cls.defaultlkz for cls in self.FindAllSubclasses(CompanyDataBase)]
            raise ValueError(f'no class for country code "{aLKZ}" configured. Available countries: {available_countries}')
        self.c.search()
        return self.c()
    

    # Purpose of write_objekt:
    # The write_objekt method is responsible for inserting or updating all relevant company and address information into the database for a given object (such as a business or company) extracted from web data. 
    # It ensures that all related tables (object, address, communication, features, etc.) are updated in a consistent and transactional way.
    # For each row returned by the main SQL query, after address extraction and validation, if not in "Test" mode, write_objekt is called to write the extracted data to the database.
    # The first four parameters come from the SQL query result row. (masterobjektid, objektid, quellenid, idx)
    # The rest come from the extracted data in the self.c object, which is populated by parsing the HTML content for each row.

    def write_objekt(self, masterobjektid, objektid, quellenid, idx, firma1, firma2, firma3, adresse, lkz, plz, ort, email, telefon, lat, lon, com_reg_entry, vat_id, manager_position, manager):
        # at least 1 field must be filled
        if firma1 or firma2 or firma3 or adresse or plz or ort or email or telefon or lat or lon or com_reg_entry or vat_id or manager_position or manager:
            try:
                if not objektid:
                    query = ''' SELECT nextval('smartdatadb.objekt_objektid_seq') '''
                    with self.conn.cursor() as curs:
                        curs.execute(query)
                        newobjektid = [k[0] for k in curs][0]
                else:
                    newobjektid = objektid

                query = ''' INSERT INTO smartdatadb.objekt (MASTEROBJEKTID, OBJEKTID, FIRMA1, FIRMA2, FIRMA3, QUELLENID) 
                            VALUES (%s, %s, %s, %s, %s, %s) '''
                # Update only for objects created by this process
                if quellenid==self.quellenid:
                    query = query + '''
                                    ON CONFLICT (objektid) DO 
                                        UPDATE 
                                        SET  masterobjektid = EXCLUDED.masterobjektid, FIRMA1=EXCLUDED.firma1, FIRMA2=EXCLUDED.firma2, FIRMA3=EXCLUDED.firma3 '''
                else:
                    query = query + ''' ON CONFLICT (objektid) DO NOTHING'''

                with self.conn.cursor() as curs:
                    curs.execute(query, (masterobjektid, newobjektid, firma1[0:120], firma2[0:120], firma3[0:120], self.quellenid))

                if not objektid:
                    query = ''' UPDATE smartdatadb.b2b_urls set OBJEKTID=%s WHERE idx=%s '''
                    with self.conn.cursor() as curs:
                        curs.execute(query, (newobjektid, idx))

                    query = ''' INSERT INTO smartdatadb.externid (extid, extidtypid, objektid)  VALUES ('%s', %s, %s) '''
                    with self.conn.cursor() as curs:
                        curs.execute(query, (idx, self.extidtypid, newobjektid))

                    query = '''INSERT INTO smartdatadb.importstatushist (gueltigab, objektid, importstatusid) 
                            VALUES(now() + interval '%s second', %s, %s)'''
                    with self.conn.cursor() as curs:
                        curs.execute(query, (0, newobjektid, 0))

                #do not create empty addresses
                if ort and plz:
                    query = ''' INSERT INTO smartdatadb.adresse (objektid, adresstypid, strasse, lkz, plz, ort, geo_lat, geo_long, eingefuegtvon) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'extract_addresses.py') '''
                    # Update only for objects created by this process and if the address to be updated has not already been checked
                    if quellenid==self.quellenid:
                        query = query + '''
                                        ON CONFLICT (objektid, adresstypid) DO
                                        UPDATE 
                                        SET strasse=EXCLUDED.strasse, hausnr='', hausnrergaenzung='', lkz=EXCLUDED.lkz, plz=EXCLUDED.plz, ort=EXCLUDED.ort, geo_lat=EXCLUDED.geo_lat, geo_long=EXCLUDED.geo_long,
                                            geaendertvon='extract_addresses.py' 
                                        where not exists(select 1 from smartdatadb.adressstatushist ah where ah.adressid =adresse.adressid 
                                                and ah.adressehash=md5(coalesce(EXCLUDED.lkz, '')||coalesce(EXCLUDED.plz, '')||coalesce(EXCLUDED.ort, '')||coalesce(EXCLUDED.strasse, '')||coalesce(EXCLUDED.hausnr, '')||coalesce(EXCLUDED.hausnrergaenzung,''))::uuid)                                
                                        '''
                    else:
                        query = query + ''' ON CONFLICT (objektid, adresstypid) DO NOTHING'''
                    
                    with self.conn.cursor() as curs:
                        curs.execute(query, (newobjektid, 1, adresse, lkz, plz, ort, lat, lon))

                query = ''' INSERT INTO smartdatadb.kommunikation (objektid, kommtypid, text1) 
                            VALUES (%s, %s, %s)  '''
                # Update only for objects created by this process
                if quellenid==self.quellenid:
                    query = query + '''
                                    ON CONFLICT (objektid, kommtypid) DO
                                    UPDATE 
                                    SET text1=EXCLUDED.text1 '''
                else:
                    query = query + ''' ON CONFLICT (objektid, kommtypid) DO NOTHING'''
                
                with self.conn.cursor() as curs:
                    if telefon!='':
                        curs.execute(query, (newobjektid, 1, telefon))

                
                with self.conn.cursor() as curs:
                    if email!='':
                        curs.execute(query, (newobjektid, 3, email))

                query = ''' INSERT INTO smartdatadb.objektmerkmal (objektid, merkmalid, wert) 
                            VALUES (%s, %s, %s)  '''
                # Update only for objects created by this process
                #if quellenid==self.quellenid:
                query = query + '''
                                ON CONFLICT (objektid, merkmalid) DO
                                UPDATE 
                                SET wert=EXCLUDED.wert '''
                #else:
                #    query = query + ''' ON CONFLICT (objektid, merkmalid) DO NOTHING'''
                
                with self.conn.cursor() as curs:
                    if com_reg_entry!='':
                        curs.execute(query, (newobjektid, 386, com_reg_entry))
                
                with self.conn.cursor() as curs:
                    if vat_id!='':
                        curs.execute(query, (newobjektid, 388, vat_id))
                
                with self.conn.cursor() as curs:
                    if manager!='':
                        curs.execute(query, (newobjektid, 392, manager))
                
                with self.conn.cursor() as curs:
                    if manager_position!='':
                        curs.execute(query, (newobjektid, 393, manager_position))

                query = '''INSERT INTO smartdatadb.importstatushist (gueltigab, objektid, importstatusid) 
                        VALUES(now() + interval '%s second', %s, %s)'''
                
                with self.conn.cursor() as curs:
                    curs.execute(query, (1, newobjektid, 12))

                
                self.conn.commit()
                #if 'Feedback' in self.args:
                if 'Feedback' in self.argmap.values():
                    logging.info('Firma: '+str(firma1)+str(firma2)+str(firma3)+', Adresse: '+str(adresse)+', LKZ: '+str(lkz)+', PLZ: '+str(plz)+', Ort: '+str(ort)+', Email: '+str(email)+', Telefon: '+str(telefon)+', Geocodes: {lat: '+str(lat)+', lon: '+str(lon)+'}, com_reg_entry: '+str(com_reg_entry)+', vat_id: '+str(vat_id)+', manager_position: '+str(manager_position)+', manager: '+str(manager))
                if 'Verbose' in self.argmap.values() or 'Feedback' in self.argmap.values():
                    if not objektid:
                        logging.info('Object with ID '+str(newobjektid)+' from IDX "'+str(idx)+'" created.')
                    else:
                        logging.info('Object with ID '+str(newobjektid)+' from IDX "'+str(idx)+'" updated.')

            except (Exception, TypeError) as error:
                logging.error('Error creating object with ID '+str(newobjektid)+' from IDX "'+str(idx)+'" created: '+str(error))
                self.conn.rollback()
                raise error

    #This query is designed to resolve a country code (LKZ) from various possible country identifiers.
    #It tries to match an input value (%s, which is a parameter) against several fields in the lkz table.
    def get_LKZ(self, aLKZ, aCountry, aTLD) -> str:
        ''' Determine country code from master table, if aLKZ cannot be found, search for TLD and finally aCountry'''
        result = ""
        # Handle None values
        aLKZ = aLKZ if aLKZ is not None else ""
        aCountry = aCountry if aCountry is not None else ""
        aTLD = aTLD if aTLD is not None else ""
        print(aLKZ+' :: '+aCountry+' :: '+aTLD)
        query = '''
                with lkzdata as(select l.* from smartdatadb.lkz l
				                join smartdatastagdb.config_country_export c on c.countrycode=l.lkz)						
                select min(coalesce(l.lkz, l2.lkz, l3.lkz,l4.lkz,l5.lkz,l6.lkz)) 
                from (select %s as lkz) a 
                    left join lkzdata l on l.lkz =upper(a.lkz)
                    left join lkzdata l2 on upper(l2.bezeichnung) =upper(a.lkz)
                    left join lkzdata l3 on upper(l3.bezeichnung2) =upper(a.lkz)
                    left join lkzdata l4 on upper(l4.tld) =upper(a.lkz)
                    left join lkzdata l5 on upper(l5.kfz) =upper(a.lkz)
                    left join lkzdata l6 on upper(l6.lkz3) =upper(a.lkz)
                '''
        
        with self.conn.cursor() as curs:
            curs.execute(query, [aLKZ]) 
            result = [k[0] for k in curs][0]

        if not result:
            
            with self.conn.cursor() as curs:
                curs.execute(query, [aTLD])
                result = [k[0] for k in curs][0]
        if not result:
            
            with self.conn.cursor() as curs:
                curs.execute(query, [aCountry])
                result = [k[0] for k in curs][0]
        return result

    # Purpose of the Query
    # This query is designed to retrieve and combine information about two types of URLs from your database:
    # Child URLs that are specifically marked as "contact" or "imprint" links (using keywords 'link_kontakt' or 'link_impressum'), along with their parent page and associated metadata.
    # Parent URLs (main pages with no parent themselves) that do not yet have a "contact" or "imprint" child link.
    # It ensures that only the latest HTML version for each URL is included and gathers relevant metadata such as object IDs, country, dates, and the top-level domain (TLD) extracted from the URL.

    def fetch_documents(self):
        #if 'IDX' in self.args:
        if 'IDX' in self.argmap.values(): 
            #addquery = 'and idx in ('+self.args['IDX']+')'
            addquery = 'and idx in ('+self.argmap['idx']+')'
        else:
            addquery = ' '
        #if 'PARENT_IDX' in self.args:
        if 'PARENT_IDX' in self.argmap.values():
            addquery2 = 'and parent_idx in ('+self.argmap['parent_idx']+')'
        else:
            addquery2 = ' '
        #if 'COUNTRY' in self.args:
        if 'COUNTRY' in self.argmap.values():
            #addquery3 = 'and country in ('+self.args['COUNTRY']+')'
            addquery3 = 'and country in ('+self.argmap['country']+')'
        else:
            addquery3 = ' '

        query = '''
        SELECT* FROM (
        SELECT u.idx,
            case when u.url_absolute is null then u.url else u.url_absolute end as url_absolute,
            h.html,
            h.version,
            u.url, o.masterobjektid, u.objektid, p.country, uo.quellenid, uo.geaendertam, uo.eingefuegtam, 
            u.parent_idx, ih.gueltigab,
            (regexp_matches(u.url, '(?:\.)([^\/\.\:]+)(?:\/|$)'))[1] as tld
        FROM smartdatadb.b2b_html h
         JOIN smartdatadb.b2b_urls u ON h.idx = u.idx 
         JOIN smartdatadb.b2b_keywords k ON k.keyword=u.from_keyword and k.name in ('link_kontakt', 'link_impressum')
         JOIN  smartdatadb.b2b_urls p ON p.idx = u.parent_idx
         left join smartdatadb.objekt o on p.objektid=o.objektid
         left join smartdatadb.objekt uo on uo.objektid=u.objektid
         left join (select objektid, max(gueltigab) as gueltigab
         			from smartdatadb.importstatushist
         			where importstatusid=12
         			group by objektid) ih on ih.objektid=u.objektid 
        WHERE o.masterobjektid is not null        
        and not exists(select 1 from smartdatadb.b2b_html h2
                        where h2.idx=h.idx
                        and h2.version>h.version)
		union all 
		 SELECT p.idx,
            case when p.url_absolute is null then p.url else p.url_absolute end as url_absolute,
            h.html,
            h.version,
            p.url, o.masterobjektid, p.objektid, p.country, o.quellenid, o.geaendertam, o.eingefuegtam, 
            p.idx as parent_idx, ih.gueltigab,
            (regexp_matches(p.url, '(?:\.)([^\/\.\:]+)(?:\/|$)'))[1] as tld
        FROM smartdatadb.b2b_html h
         JOIN smartdatadb.b2b_urls p ON p.idx = h.idx and p.parent_idx is null
         join smartdatadb.objekt o on p.objektid=o.objektid and o.masterobjektid is not null
         left join smartdatadb.objekt uo on uo.objektid=p.objektid
         left join (select objektid, max(gueltigab) as gueltigab
         			from smartdatadb.importstatushist
         			where importstatusid=12
         			group by objektid) ih on ih.objektid=p.objektid 
        WHERE not exists(select 1 from smartdatadb.b2b_html h2
                        where h2.idx=h.idx
                        and h2.version>h.version)
        and not exists(select 1 from smartdatadb.b2b_urls u JOIN smartdatadb.b2b_keywords k ON k.keyword=u.from_keyword and k.name in ('link_kontakt', 'link_impressum')
                        where u.parent_idx = h.idx)
        ) t
        where 1=1
        '''+ addquery + '''
        '''+ addquery2 + '''
        '''+ addquery3 + '''
        /* Sortierung */
        order by gueltigab
        nulls first                        
        LIMIT %s ;
        '''
        #LIMIT '''+ self.args['Anzahl'] +''';
        #print(query)
        
        processed_cnt=0
        with self.conn.cursor() as curs:
            curs.execute(query, (self.limit,))
            for row in curs:
                logging.info(f"Starting URLs process with address extractor for IDX: {row[0]}")
                aLKZ = ''
                #determine LKZ
                aLKZ = self.get_LKZ(aLKZ, str(row[7]), str(row[13]))
                
                # Fallback to default country if LKZ is None or empty
                if not aLKZ or aLKZ == 'None':
                    aLKZ = 'DE'  # Default to Germany
                    logging.warning(f'No valid country code found for IDX {row[0]}, using default: DE')
                
                try:
                    daten = self.get_address(aLKZ,row[2])
                    aLKZ = str(self.c.lkz)

                except (Exception, TypeError, ValueError, AttributeError) as error:
                    logging.error('Error parsing from IDX "'+str(row[0])+'": '+str(error))
                    self.conn.rollback()
                    raise error

                except (KeyboardInterrupt):
                    logging.error('User aborted during IDX "'+str(row[0])+'": '+str(row[1]))
                    self.conn.rollback()
                    raise

                if self.c:
                    #determine LKZ
                    self.c.lkz = self.get_LKZ(aLKZ, str(row[7]), str(row[13]))
                    daten = self.c()
                    #if 'Test' in self.args:
                    if 'Test' in self.argmap.values():
                        print('masterobjektid: '+str(row[5])+', objektid: '+str(row[6])+', quellenid: '+str(row[8])+', idx: '+str(row[0])+' version: '+str(row[3])+
                                ' '+str(row[1])+' '+str(daten)+' '+self.c._getCheckerResult())
                    else:
                        #if 'Feedback' in self.args:
                        if 'Feedback' in self.argmap.values():
                            logging.info('URL: '+str(row[1]))
                        self.write_objekt(masterobjektid=row[5], objektid=row[6], quellenid=row[8], idx=row[0], firma1=self.c.firma1, firma2=self.c.firma2, firma3=self.c.firma3, 
                                            adresse=self.c.adresse, lkz=self.c.lkz, plz=self.c.plz, ort=self.c.ort, email=self.c.email, telefon=self.c.telefon, 
                                            lat=self.c.location['lat'], lon=self.c.location['lon'], com_reg_entry=self.c.com_reg_entry, vat_id=self.c.vat_id, 
                                            manager_position=self.c.manager_position, manager=self.c.manager)
                processed_cnt+=1
                        
            logging.info(f"Total Number of URLs processed with address extractor are: {processed_cnt}")       
                      
                                 
    def run(self):
        '''
        Run method
        '''
        self.fetch_documents()
        #if 'Verbose' in self.args:
        if 'Verbose' in self.argmap.values():
            if self.c:
                logging.info(self.c.getSearchtime())
        logging.info('Address extractor finished')

#Call the class
#if __name__ == '__main__':
def main_function():
    try:
        logging.info("Address Extraction started")
        ae = AddressExtractor()
        ae.run()  
        success_message = 'Address Extraction completed Successfully'
        logging.info(success_message)
    except Exception as e:
        error_message = f'Address Extraction Failed: {str(e)}'
        logging.error(error_message)
        raise str(e)


    
