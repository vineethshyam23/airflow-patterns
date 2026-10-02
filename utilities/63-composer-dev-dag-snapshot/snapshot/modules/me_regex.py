#!/usr/bin/python3

import re

class MeFormatting: 
    """Class to define any commonly used regex patterns for formatting"""
    merge_whitespace: re.Pattern = re.compile("\s*\n\s*")
    merge_newlines: re.Pattern = re.compile("\n{2,}")
    merge_spaces: re.Pattern = re.compile("\s{2,}")
    tab_space_eol: re.Pattern = re.compile("[ \t]+\n[ \t\n]+\n[ \t]+") #formerly formatier_dich
    pricepart: re.Pattern = re.compile("^[0-9/,. €zł]+$")

class MeSubstr:
    """Class to define commonly used regex patterns for substrings"""
    two_digits: re.Pattern = re.compile("[0-9][0-9]") #formerly dd
    has_char: re.Pattern = re.compile("[ :/()+-]")
    two_slashes: re.Pattern = re.compile("/.*/")

class MeClassifiers:
    """Class to define commonly used regex patterns for classification of the string. 
    These patterns are used to determine the type of the string, e.g. if it is a price, a date, etc.
    for extraction."""
    price: re.Pattern = re.compile(r'(((€|\b([€Ee]uro?)|\b([Zz][lł]))[ \u00A0]{0,2}[0-9]{1,}([,.]([0-9o]{2}|[\-\–]))?)|(\b[0-9]{1,}([,.]([0-9]{2}|[\-\–]))?)[ \u00A0]{0,2}(€|([€Ee]uro?\b)|([Zz][lł]\b)))', re.M|re.S)
    price_numberpart: re.Pattern = re.compile(r"([0-9]{1,}([,.]([0-9o]{2}|[\-\–]))?)", re.M|re.S)
    price_number_chars: re.Pattern = re.compile(r"[o]{2}|[\-\–]", re.M|re.S)
    volpercent: re.Pattern = re.compile("[0-9.,]+[%]")
    date: re.Pattern = re.compile("[0-3]?[0-9][./-][0-1]?[0-9][./-]((19)|(20))?[0-9]{2}") #Updated to european dates; formerly "datum"
    # E-Mail
    email: re.Pattern = re.compile("[a-zA-Z0-9._-]+@[a-zA-Z0-9]+([.][a-zA-Z]{2,3})?[.][a-zA-Z]{2,8}", re.M|re.S)
    # Phone
    phone: re.Pattern = re.compile("(\(|\+)?[0-9]+[0-9\/ ()-\.]{6,}[0-9]") #re.compile("\+?[0-9]+[0-9\/ ()]{9,}[0-9]")

class MePatterns: 
    """Class to define commonly used regex patterns that identify patterns that are used to clean around the string."""
    ordernumber: re.Pattern = re.compile('^M?[0-9]+')
    liter: re.Pattern = re.compile(r"1?[0-9][.,]?[0-9]?[0-9]? ?[cdm]?[lL]\b|1?[0-9][.,]?[0-9]?[0-9]? ?cc\b") #[0-9][.,][0-9][0-9]? ?[cm]?[lL]\b
    flasche: re.Pattern = re.compile(r"[0-9]{1,2} ?[Ff]lessen\b|1 [Ff]les\b|[0-9]{1,2} ?[Ff]laschen?\b|[0-9]{1,2} ?fl\b|[0-9]{1,2} ?[Bb]outeilles?")
    can: re.Pattern = re.compile(r"\d{1,2} ?[Bb]lik[je]{2}s?[ken]{3}?|\d{1,2} ?[Dd]osen?|\d{1,2} ?[Cc]anettes?")
    weight: re.Pattern = re.compile(r"([0-9]?[,.]?[0-9]+ ?k?[Gg]r?(?:am)?m?s?\b)")
    diameter: re.Pattern = re.compile(r"i?n? ?Ø? ?([0-9]+ ?cm)")
    itemcount: re.Pattern = re.compile(r"\d{1,2} ?[Ss]t[uü]?c?k?s?|\d{1,2} ?[Pp]i[èe]ces?|\b[Xx] ?\d?\d\b|\b\d?\d ?[Xx]\b")
    sizewords: re.Pattern = re.compile(r'[Gg]ross|[Gg]roß|[Gg]root|([Gg]rand)|[Ll]arge|[Kk]lein|[Ss]mall|[Pp]etit|[Mm]edium|[Mm]ittel?|[Mm]iddel|[Mm]oyen')
    itemsizes = [
        liter, flasche, can, weight, diameter, itemcount, sizewords
    ]
    # price: re.Pattern = re.compile("[0-9]+[,.][0-9][0-9]")
    # price: re.Pattern = re.compile(r"(((€|\b([€Ee]uro?)|\b([Zz][lł]))[ \u00A0]{0,2}[0-9]{1,}([,.][0-9]{2})?)|(\b[0-9]{1,}([,.][0-9]{2})?)[ \u00A0]{0,2}(€|([€Ee]uro?\b)|([Zz][lł]\b)))") #new
    # full_price: re.Pattern = re.compile(r'((€|([Ee]uro?\b)|([Zz][lł]))[ \u00A0]?[0-9]{1,}([,.][0-9]{2})?)|([0-9]{1,}([,.][0-9]{2})?[ \u00A0]?(€|([Ee]uro?\b)|([Zz][lł])))', re.M|re.S)
    full_price: re.Pattern = re.compile(r'(((€|\b([€Ee]uro?)|\b([Zz][lł]))[ \u00A0]{0,2}[0-9]{1,}([,.]([0-9o]{2}|[\-\–]))?)|(\b[0-9]{1,}([,.]([0-9]{2}|[\-\–]))?)[ \u00A0]{0,2}(€|([€Ee]uro?\b)|([Zz][lł]\b)))', re.M|re.S)
    procent: re.Pattern = re.compile("[0-9]*[,.][0-9]{1,2}[\s]?[%]|[1-9][0-9o]*[,.][o]{2}[\s]?[%]") #new
    no_price: re.Pattern = re.compile("[0-9]*[,.][0-9]{1,2}[\s][msMHhS][\s]|[0-9]*[,.][0-9]{1,2}[\s][msMHhS]$|[1-9][0-9o]*[,.][o]{2}[\s]?[msMHhS][\s]|[1-9][0-9o]*[,.][o]{2}[\s]?[msSMHh]$") #new
    currency: re.Pattern = re.compile(r'€|[zZ][łl]|PLN|[Ee][Uu][Rr]o?\b') #new
    words: re.Pattern = re.compile(r"\b[^\d\W]{2,}\b")#("[A-ZÄÖÜäöüßa-zézãehsçgydtkaiáqóâWcupônlmorfbvjxwêÉZÃEHSÇGYDTKAIÁQÓÂwCUPÔNLMORFBVJXWÊ'][A-ZÄÖÜäöüßa-zézãehsçgydtkaiáqóâWcupônlmorfbvjxwêÉZÃEHSÇGYDTKAIÁQÓÂwCUPÔNLMORFBVJXWÊ']+")
    replace_no_price: re.Pattern = re.compile(r"\+\s?[0-9][.,][0-9]{1,2}\s?€?")
    numbers: re.Pattern = re.compile(r"[\d,\.=]+", re.M|re.S)
    br_pattern: re.Pattern = re.compile(r'<br\s*/?>((?:(?!<br\s*/?>|<[^>]+>).)*)', re.I|re.DOTALL)
    invalid_html: re.Pattern = re.compile(r'<[^>]*<|<[^>]*<', re.M|re.S)
    binary_image: re.Pattern = re.compile(r'(<body>)?ÿ?Øÿ(à|á|î|\¸)(([\u0010-\u0018\u0095\u001C]{0,4}[\w\*]?(Exif|JFIF))|(!Adobed))', re.M|re.S)


# TODO: see if MeClassifiers and MePatterns can be merged into one class. 
