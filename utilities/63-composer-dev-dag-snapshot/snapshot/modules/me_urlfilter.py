#!/usr/bin/python
import sys
import os
# import countries here (create a file for each country):

from .me_urlfilter_nl import re_exclude_patterns as regexpat_nl

class MeUrlFilter():
    urlfilter = {}
    # add countries here:
    urlfilter['nl'] = regexpat_nl

    def filter_urls(self, urls:list = None, country="nl"):
        """
        Takes a list of URLs and a country. Returns ONE bool per list. 
        If there is no filter list defined for the given country
        the URLs are assumed to be okay.
        Returns True for "good" URLs and False for "bad" URLs
        """
        if country not in self.urlfilter.keys(): 
            return True

        for url in urls:
            if not url:
                continue
            
            if self.urlfilter[country].findall(url.lower()): 
                return False
                
        return True

# if __name__ == "__main__":
#     args = sys.argv[1:]
#     if args:
#         uf = MeUrlFilter()
#         inputData = [arg for arg in args]
#         print(inputData)
#         print(uf.filter_urls(inputData))
#     else:
#         print("Please provide one or multiple strings (URLs) for the url filter to check.")
