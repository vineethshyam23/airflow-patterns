# me_extractor v2
import sys
import re
import pandas as pd
import traceback
import logging
from typing import List, Tuple
from pathlib import Path
from datetime import datetime
from selectolax.parser import HTMLParser
from timeout_decorator import timeout, TimeoutError as TD_timeoutError
from hashlib import md5
from .me_config import MeExtractorConfig
from .me_regex import MeFormatting, MeClassifiers, MePatterns
from .me_db_queries import DbCountingQueries, DbFetchQueries, DbInsertQueries, DbDeleteQueries, DbReturnfunct
from .me_urlfilter import MeUrlFilter
from .db_postgres import PostgresDB, check_execution_result


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('extractor.log')
    ]
)
logger = logging.getLogger(__name__)

db_delete_queries = DbDeleteQueries()

class Extractor():
    """
    Extractor v2 - improved strategies
    Class to extract information from HTML documents. 
    Uses the standard db class from me_db.py. 
    """
    config = MeExtractorConfig()
    db = None
    docs_to_process = None
    doc_count = 0
    discard_count = 0

    def __init__(self, console=None, init_with_db=True, provided_db_connection=None, source_doctype='html'):
        """
        Initializes the Extractor class. If init_with_db is set to False, the extractor can be used without
        a database connection. If init_with_db is set to True, the extractor uses the PostgresDB class.
        If a database connection is provided, the extractor uses this connection instead of establishing
        a new one.
        If the console is provided, the extractor uses this console instead of the MeConsole class.
        """
        unique_id = md5(str(datetime.now()).encode()).hexdigest()
        self.unique_extractor_id = f"{self.config.HTML_EXTRACTOR_STRING}-{unique_id}"
        self.words = MePatterns.words
        self.merge_spaces = MeFormatting.merge_spaces
        self.merge_whitespace = MeFormatting.merge_whitespace
        self.pricepart = MeFormatting.pricepart
        self.price = MeClassifiers.price
        self.volpercent = MeClassifiers.volpercent
        self.datum = MeClassifiers.date
        self.itemsizes = MePatterns.itemsizes
        self.numbers = MePatterns.numbers
        self.invalid_html = MePatterns.invalid_html
        self.binary_image = MePatterns.binary_image
        self.item_classes = ['.item', '.tablepress']


        # if console:
        #     self.console = console
        # else:
        #     from me_console import MeConsole
        #     self.console = MeConsole

        log_message = f"Extractor initialized with unique id: {self.unique_extractor_id}"
        print(log_message)
        logger.info(log_message)

        if init_with_db:
            try:
                if provided_db_connection:
                    self.db = PostgresDB(provided_connection=provided_db_connection)
                else:
                    self.db = PostgresDB()

            except:
                log_message = "No database Connection established"
                print(log_message)
                logger.error(log_message + " in the extractor")

        if source_doctype == 'html': 
            self.count_query = DbCountingQueries.count_html_documents
            self.count_query_returnfunct = DbReturnfunct.count_documents


        elif source_doctype == 'pdf':
            self.dish = False
            self.count_query = DbCountingQueries.count_pdf_urls
            self.quellenid = 9
        elif source_doctype == 'dishpdf':
            self.dish = True
            self.count_query = DbCountingQueries.count_pdf_urls_dish
            self.quellenid = 45

        self.uf = MeUrlFilter()

        log_message = f'''Extractor initialized{" with database" if init_with_db 
                              else " without database. Remember to submit a filepath to store the results"}'''
        print(log_message)
        logger.info(log_message)


    def load_html_documents(self, path="htmls"):
        """
        Recursively loads HTML Documents from
        below path and enqueues them to be
        processed; uses the 
        standard db class from me_db.py
        """

        def readfile(file):
            with open(file, "rb") as f:
                return f.read()

        if Path(path).exists():
            self.docs_to_process = (
                (
                    0,
                    0,
                    i.as_posix().lstrip(path + "/"),
                    i.as_posix().lstrip(path + "/"),
                    readfile(i),
                )
                for i in Path(path).glob("**/*")
                if i.is_file()
            )
        else:
            # TODO: Refactor to use logging
            print("Could not load documents from " + path)


    def count_html_documents(self, query=None):
        """Counts the documents to fetch from the table defined in 
        DbCountingQueries.count_html_documents; uses the 
        standard db class from me_db.py"""
        if not query:
            query = DbCountingQueries.count_html_documents

        return self.db.fetch(query, returnfunction=DbReturnfunct.fetch_all_sub)


    @check_execution_result
    def _reserve_html_documents(self, limit=0):
        """Reserves the documents to fetch from the table defined in 
        DbFetchQueries.reserve_and_fetch_html_documents; uses the 
        standard db class from me_db.py"""

        query = DbInsertQueries.reserve_html_documents
        query = query.format(self.unique_extractor_id, limit)
        return self.db.execute(query)

    
    def reserve_and_fetch_html_documents(self, limit=0):
        """Reserves the documents to fetch from the table defined in 
        DbFetchQueries.reserve_and_fetch_html_documents; uses the 
        standard db class from me_db.py"""

        self._reserve_html_documents(limit=limit)

        query = DbFetchQueries.fetch_html_documents.format(self.unique_extractor_id, self.unique_extractor_id)

        yield from self.db.fetch(query, returnfunction=DbReturnfunct.fetch_columns)


    @check_execution_result
    def remove_html_doc_reservations(self):
        """Cleans up the reservation of documents in the table defined in 
        DbInsertQueries.reserve_html_documents; uses the 
        standard db class from me_db.py"""
        query = db_delete_queries.remove_html_doc_reservations
        query = query.format(self.unique_extractor_id)
        return self.db.execute(query)


    def fetch_html_documents(self, query=None, limit=0, offset=0):
        """Fetches the html documents from the table defined in
        DbFetchQueries.fetch_html_documents; uses the 
        standard db class from me_db.py"""
        if not query: 
            query = DbFetchQueries.fetch_html_documents
        if offset or limit: 
            query = query.format(limit, offset)

        yield from self.db.fetch(query, returnfunction=DbReturnfunct.fetch_columns)


    def fetch_html_documents_from_list(self, idxlist, query=None):
        """Fetches the html documents from the table defined in
        DbFetchQueries.fetch_html_documents_from_list;
        uses the standard db class from me_db.py"""
        if not query: 
            query = DbFetchQueries.fetch_html_documents_from_list

        yield from self.db.fetch(query, returnfunction=DbReturnfunct.fetch_columns, itemtuple=idxlist)


    @check_execution_result
    def insert_item_and_menu_multi(self, menutuplelist, itemtuplelist):
        """inserts the menu records and the menuitems for them in one 
        transaction, preventing RACE conditions.""" 
        menuquery = DbInsertQueries.insert_menu_multi["query"]
        itemquery = DbInsertQueries.insert_item_multi["query"]
        menu_value_template = DbInsertQueries.insert_menu_multi["value_template"]
        item_value_template = DbInsertQueries.insert_item_multi["value_template"]
        return self.db.multi_insert([menuquery, itemquery], [menu_value_template, item_value_template], itemtuplelist=[menutuplelist, itemtuplelist])


    @check_execution_result
    def insert_processed_multi(self, itemtuplelist):
        """Inserts a single item with the query defined in 
        DbInsertQueries.insert_processed_multi; takes a list of tuples and
        uses the standard db class from me_db.py""" 
        query = DbInsertQueries.insert_processed_multi["query"]
        value_template = DbInsertQueries.insert_processed_multi["value_template"]
        return self.db.multi_insert([query], [value_template], itemtuplelist=[itemtuplelist])


    def clean_doc(self):
        """
        Shortcut to strip <style >and <script> tags from the
        current document since they could disrupt various
        other process steps
        """
        if self.doc:
            self.doc["doc"].strip_tags(["script", "style","sup"]) #new


    def extract_links(self):
        """
        Extracts all links found in the document
        """
        links = self.doc["doc"].tags("a")
        self.doc["links"] = []
        for link in links:
            try:
                self.doc["links"].append(
                    (link.text(), link.attributes["href"], link.html)
                )
            except:
                continue


    def remove_script_and_style(self, html: HTMLParser) -> HTMLParser:
        for tag in html.css('script, style'):
            tag.decompose()

        with open("no_script.html", "w") as file:
            file.write(html.html)
        return html


    def replace_br_with_tag(self, html_content: str, tag: str = 'div') -> str:
        # Step 1: Temporarily replace <p> blocks with a placeholder
        p_blocks = []

        def replace_p_blocks(match):
            p_blocks.append(match.group(0))
            return f"<p>{' ' * (len(match.group(0)) - 7)}</p>"

        no_p_tags = re.sub(r'<p[^>]*>.*?</p>', replace_p_blocks, html_content, flags=re.DOTALL)

        # Step 2: Function to wrap matched text in the specified tag
        def wrap_in_tag(match):
            text_after_br = match.group(1).strip()
            if text_after_br:  # Only wrap non-empty strings
                return f"<{tag}>{text_after_br}</{tag}>"
            return ""

        # Step 3: Define the pattern to match <br> tags and content between them
        br_pattern = re.compile(r'<br\s*/?>(.*?)<br\s*/?>', re.DOTALL)

        # Step 4: Replace occurrences in the modified content
        new_html = re.sub(br_pattern, wrap_in_tag, no_p_tags)

        # Step 5: Restore the original <p> blocks
        def restore_p_blocks(p_blocks):
            return p_blocks.pop(0)

        new_html = re.sub(r'<p>\s*</p>', restore_p_blocks, new_html)

        return new_html


    def identify_card_type(self, html: HTMLParser) -> Tuple[str,str]:
        for node in html.css('table'):
            if len(self.price.findall(node.text(deep=True))) > len(node.css('tr')) and len(node.css('li'))<2: 
                return "multi-price-table", ""
            elif (
                len(self.price.findall(node.text(deep=True))) < len(node.css('tr')) + (len(node.css('tr'))//5) and 
                len(self.price.findall(node.text(deep=True))) > len(node.css('tr')) - (len(node.css('tr'))//5) and 
                len(node.css('li')) < 2
                ):
                return "single-price-table", ""
        for it_class in self.item_classes:
            if html.css(it_class):
                return "item-class-card", it_class
        return "flat-menu", "*"


    def get_inner_nodes(self, html: HTMLParser, css_tag:str='*') -> List[HTMLParser]:
        node_list = []
            
        # Helper function to determine if the node contains the search text within its subtree
        def contains_search_text(node):
            if self.price.search(node.text(deep=True)):
                return True
            return False

        # Attempt to extract from menu-item first. If not found, search all nodes. 
        if css_tag != '*':
            node_list = [node for node in html.css(css_tag) if contains_search_text(node)]

        # If the flat css tag is used, search all nodes
        if not node_list:
            for node in html.css('*'):
                if self.invalid_html.search(node.html):
                    continue
                # Check if node itself contains the search text anywhere in its subtree
                if contains_search_text(node):
                    # Check all children to ensure this is the innermost node containing the text
                    is_innermost = True
                    for child in node.iter():
                        if child != node and contains_search_text(child):
                            is_innermost = False
                            break

                    if is_innermost:
                        node_list.append(node)

        return node_list


    def extend_to_highest_parents(self, node: HTMLParser) -> HTMLParser:
        match_num = len([1 for i in self.price.finditer(node.text())])
        while node.parent and len([1 for i in self.price.finditer(node.parent.text())]) <= match_num:
            node = node.parent

        return node


    def extend_to_descriptive_parent(self, node: HTMLParser) -> HTMLParser:
        reduced_text = ""
        patterns = self.itemsizes + [self.price] + [self.merge_spaces] + [self.numbers]
        while len(reduced_text.strip()) < 3:
            node = node.parent
            reduced_text = node.text(separator=" ").strip()
            if reduced_text.startswith("mit"): 
                continue
            for regx in patterns:
                reduced_text = regx.sub("", reduced_text)

        return node


    def is_size_header(self, col: str) -> bool:
        for regx in self.itemsizes: 
            if regx.search(col):
                return True


    def extract_rows_from_table(self, table: HTMLParser) -> list:
        clean_table = HTMLParser(self.merge_spaces.sub(" ", table.html))
        rows:list[list] = [[],[]]
        for node in clean_table.css('*'):
            for tr in node.css('tr'):
                row = []
                for th_td in tr.css('td,th'):
                    row.append(th_td.text().strip())
                rows.append(row)
        return rows


    def extend_incomplete_rows(self, data: list) -> list:
        # Keep track of the last complete row
        last_complete_row = None
        num_columns = 2
        data_output = []
        for row in data:

            # Determine empty columns
            num_full = len(row)
            for num_empty, col in enumerate(row):
                if col.strip() != '':
                    break
            num_full = len(row) - num_empty

            if num_empty >= num_columns and last_complete_row:
                # Combine the incomplete row with the last complete row
                combined_row = last_complete_row[:-num_full] + row[num_empty:]
                data_output.append(combined_row)
            else:
                # Add the complete row as is
                data_output.append(row)
                # Update the last complete row
                last_complete_row = row

        return data_output


    def extract_from_table_with_headers(self, rows: list) -> list:
        header = rows.pop(0)
        table_body = []
        tentative_table_width = len(header)

        for row in rows:
            if len(row) == tentative_table_width or len(table_body) == 0:
                table_body.append(row)
            else:
                table_body[-1] += row # merge off-width rows with the previous row, as they are likely to be part of the same entry

        table_width = max([len(row) for row in table_body]) # get the adjusted table width

        # Adjust the length of all rows
        for row in table_body:
            if len(row) < table_width:
                row += [''] * (table_width - len(row)) 

        # Adjust the header length
        if len(header) < table_width:
            header += [f'description {i+1}' for i in range(table_width - len(header))]

        df = pd.DataFrame(table_body, columns=header)
        df.drop_duplicates(inplace=True) # some structures may duplicate rows inadvertedly

        size_headers = [col for col in df.columns if self.is_size_header(col)]
        non_size_headers = [col for col in df.columns if col not in size_headers]
        # make melt conditional based on the presence of size headers
        df = df.melt(id_vars=non_size_headers, value_vars=size_headers, var_name='size', value_name='price')
        result = []
        for _, row in df.iterrows():
            if row['price']:
                result.append([str(value) for value in row])

        return result


    def extract_from_table(self, table: HTMLParser, table_type:str) -> list:
        result = []
        for node in table.css('table'):
            rows = self.extract_rows_from_table(node)
            if not rows:
                continue
            # Build the table width based on the top row == header if it is a multi-price-table only.
            if any([self.is_size_header(col) for col in rows[0]]) and table_type == "multi-price-table":
                result += self.extract_from_table_with_headers(rows)

            # Extract results from tables without sizes in the header - no need to adjust
            for row in rows:
                if any([self.price.search(col) for col in row]):
                    result.append([str(value) + ' ' for value in row])
                elif result:
                    result[-1] += [str(value) + ' ' for value in row]

        return [', '.join(row) for row in result]


    def extract_from_nodes(self, html: HTMLParser, item_class: str) -> list:
        nodes = self.get_inner_nodes(html)
        nodes = [self.extend_to_highest_parents(item) for item in nodes]

        output = []
        # used_parent = False
        for node in nodes:
            inner_node = self.merge_spaces.sub(" ", node.text(separator=" "))
            reduced_text = node.text(separator=" ")
            for regx in (self.itemsizes+[self.price]+[self.merge_spaces]+[self.numbers]):
                reduced_text = regx.sub("", reduced_text)
            if len(reduced_text.strip()) < 3: 
                descriptor = self.extend_to_descriptive_parent(node)
                # Make the tree structure into a list of strings, so the function can deal with them with more flexibility
                descriptor_list = [partial.strip() for partial in descriptor.text(separator="$$#@$#%!@#$$").split("$$#@$#%!@#$$") if len(self.merge_spaces.sub("", partial)) > 3]
                # Check if the higher level contains too much data (prices)
                if len(descriptor_list) == 0: 
                    output.append(self.merge_spaces.sub(" ", descriptor.text(separator=" ")))
                elif (len(self.price.findall(descriptor.text(separator=""))) > len(self.price.findall(inner_node)) and
                    len(self.price.sub("", descriptor.text(separator=""))) > 3 and
                    len(self.numbers.sub("", self.price.sub("", descriptor_list[0]))) > 3):
                    output.append(
                        self.merge_spaces.sub(" ", descriptor_list[0] + " " + inner_node)
                                )
                else: 
                    output.append(self.merge_spaces.sub(" ", descriptor.text(separator=" ")))
                # used_parent = True
            else: 
                output.append(inner_node)

        return output


    def strip_texts_and_duplicates(self, texts: List[str]) -> List[str]:
        texts = [self.merge_whitespace.sub(" ", text.strip()) for text in texts]
        return list(sorted(set(texts)))


    def extract_menu_items(self, html: HTMLParser) -> list:
        if self.binary_image.search(html.html):
            print("\033[31m" + "JPEG in HTML" + "\033[0m" )
        html = self.remove_script_and_style(html)
        card_type, item_class = self.identify_card_type(html)
        if card_type == "multi-price-table":
            return self.strip_texts_and_duplicates(self.extract_from_table(html, card_type))
        elif card_type == "single-price-table":
            return self.strip_texts_and_duplicates(self.extract_from_table(html, card_type))

        return self.strip_texts_and_duplicates(self.extract_from_nodes(html, item_class))


    @timeout(5)
    def extract_menu(self):
        """
        Recursively search for Menu Items below the given html
        Element (by default in the current document)
        """
        if self.binary_image.search(self.doc["doc"].html):
            self.doc["menu"] = []
        else:
            self.doc["menu"] = self.extract_menu_items(self.doc["doc"])


    def _store_items(self, menus_to_insert, items_to_insert, processed_to_insert, output_file=None):
        if output_file:
            with open(output_file, "a") as f:
                for item in items_to_insert:
                    f.write(item + "\n")
            with open(output_file.stem + "_processed.txt", "a") as f:
                for item in processed_to_insert:
                    f.write(item + "\n")

        elif self.db:
            if items_to_insert:
                self.insert_item_and_menu_multi(menus_to_insert,items_to_insert)
            if processed_to_insert:
                self.insert_processed_multi(processed_to_insert)

        else:
            error_msg = "No output file or database was specified for the Extractor!"
            logger.error(error_msg)
            raise Exception(error_msg)


    def process_chunk(self, output_file: Path=None, source_doctype='html', html_size_limit=3500000):
        """Processes a chunk of documents and inserts them into the database
        Use this function to process the documents loaded in 
        self.docs_to_process.
        use process_docs() to load all documents from the database
        and process them.
        The original while loop is replaced with a 
        pythonic for loop. For ... in already iterates
        over all elements in an iterable, ending 
        when the generator is exhausted. (Calling the 
        next() method on the generator under the hood).
        """
        # this is defined as a list of lists, where the first list contains the idx and the second list contains the tuples for the query
        # this allows for faster checks if the idx is already in the list (2x faster than comparing the tuples in the list)
        menus_to_insert = [[],[]] 
        items_to_insert = []
        processed_to_insert = []

        if source_doctype == 'html':
            source_docid = 5

        for doc in self.docs_to_process:
            try:
                self.doc = {
                    "idx": doc[0],
                    "version": doc[1],
                    "prev_menuid":doc[2], #added prev-menuid 
                    "url": doc[3],
                    "url_absolute": doc[4],
                    "html": doc[5],
                    "objektid": doc[6]
                }
                if not self.uf.filter_urls([self.doc['url'],self.doc['url_absolute']]):
                    self.discard_count += 1
                    processed_to_insert.append(tuple([self.doc['idx'], self.doc['version'], 0]))
                    continue

                doclstripped = self.doc['html'].lstrip()

                if doclstripped:
                    if doclstripped[0] != '<':
                            processed_to_insert.append(tuple([self.doc['idx'], self.doc['version'], 0]))
                            self.discard_count += 1
                            continue
                    
                if len(self.doc['html']) > html_size_limit:
                    self.doc["menu"] = []
                    self.discard_count += 1
                    processed_to_insert.append(tuple([self.doc['idx'], self.doc['version'], 0]))
                    continue

                self.doc["doc"] = HTMLParser(
                    self.merge_whitespace.sub(" ", self.doc["html"])
                    ) if len(self.doc["html"]) < html_size_limit else None

            except ValueError:
                processed_to_insert.append(tuple([self.doc['idx'], self.doc['version'], 0])) # make sure to insert the doc into the processed table even if it is discarded
                # print('value error')
                continue
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.error(f'EXCEPTION: {exc_type}; {exc_value}; {exc_traceback}')
                print("*** print_exception:")# exc_type below is ignored on 3.5 and later
                traceback.print_exception(exc_type, exc_value, exc_traceback,file=sys.stderr)   
                raise Exception    

            self.clean_doc()
            try: 
                self.extract_menu()
            except TD_timeoutError:
                print(self.doc['idx'], len(self.doc['html']), "\n\033[31mSKIPPED due to timeout\033[0m")
                self.doc["menu"] = []
                

            if self.doc["menu"]:

                items_found = len(self.doc["menu"])

                if self.doc['idx'] not in menus_to_insert[0]: 
                    menus_to_insert[0].append(self.doc['idx'])
                    menus_to_insert[1].append(tuple([self.doc['idx'], int(self.doc['prev_menuid'])+1, self.config.HTML_EXTRACTOR_ID, 
                                                     self.doc["version"], source_docid, self.config.HTML_EXTRACTOR_STRING]))
                for n in range(items_found):
                    # menuitem = self.doc['menu'][n] # TODO: figure out why this is needed - unused.
                    items_to_insert.append(tuple([self.doc['idx'], int(self.doc['prev_menuid'])+1, (n+1)*100, self.doc['menu'][n], self.config.HTML_EXTRACTOR_STRING, n])) #multiplied the extract_reihenfolge by 100 and added 1 to make space for more items (spliting during cleaning)
            else:
                items_found = 0
            self.doc_count += 1

            processed_to_insert.append(tuple([self.doc['idx'], self.doc['version'], items_found]))
        self._store_items(menus_to_insert[1], items_to_insert, processed_to_insert, output_file=output_file)


    def process_docs(self, chunksize:int=10000, chunklimit=None, chunkposition=None, output_file: Path=None, source_doctype='html'):
        """
        Process all docs

        Specify the chunksize to process the docs in chunks.
        Specify the chunklimit to limit the number of chunks that the extractor will process.
        If an output_file is specified, the processed docs
        will be written to this file. No output will be submitted
        to the database in this case.

        """
        # This should return fine. If a query is used that creates a longer generator, unexpected results/fails may occur.
        # count the documents in the database
        count_gen = self.db.fetch(self.count_query, returnfunction=self.count_query_returnfunct)
        ndocs = int(next(count_gen)[0]) # cast to int to fail if unexpected return value.

        if not chunkposition:
            pos = 0
        else: 
            pos = chunkposition
        pos=0
        # TODO: ? refactor the chunksize into the db class. Supported by psycopg2.
        log_message = f'total docs: {ndocs}'
        print(log_message)
        logger.info(log_message)
        iterations = 0
        while pos < ndocs:
            log_message = f'Fetching chunk from {pos} to {pos+chunksize}. Processed: {self.doc_count}, Discarded: {self.discard_count}'
            print(log_message)
            logger.info(log_message)
            self.docs_to_process = self.reserve_and_fetch_html_documents(limit=chunksize)
            self.process_chunk(output_file=output_file, source_doctype=source_doctype)
            self.remove_html_doc_reservations()
            pos += chunksize
            if chunklimit:
                iterations += 1
                if iterations >= chunklimit:
                    break
        log_message = f'Fetched chunk from {pos} to {pos+chunksize}. Processed: {self.doc_count}, Discarded: {self.discard_count}'
        print(log_message)
        logger.info(log_message)
        log_message = f'Extracted all documents. Processed: {self.doc_count}, Discarded: {self.discard_count}'
        print(log_message)
        logger.info(log_message)
        # Cleaning up after processing


# if __name__ == "__main__":
#     # Runs the Extractor.process_docs function
#     # when executed as a script but is not exe-
#     # cuted when imported.
#     e = Extractor()

#     if len(sys.argv) > 1:
#         idxlist = [int(v) for v in sys.argv[1:]]        
#         e.docs_to_process = e.fetch_html_documents_from_list(idxlist)

#         try:
#             e.process_docs()

#         except StopIteration:
#             pass
#         except:
#             exc_type, exc_value, exc_traceback = sys.exc_info()
#             print("*** print_tb:")
#             traceback.print_tb(exc_traceback, file=sys.stderr)
#             print("*** print_exception:")# exc_type below is ignored on 3.5 and later
#             traceback.print_exception(exc_type, exc_value, exc_traceback,file=sys.stderr)   
#             exit(5)
#     else:
#         exit(0)