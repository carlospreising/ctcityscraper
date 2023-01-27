from bs4 import BeautifulSoup
from os import path
import requests
import sys
import getopt
import warnings
import urllib3
import json
import re
import sys

#ssl cert bad for hamden site, will throw annoying errors.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

_VGSIURL_ = "https://www.vgsi.com/connecticut-online-database/"
_VSGIPATH_ = "vgsi_cities.json"

def open_vgsi_cities(path = _VSGIPATH_):
    with open (path) as city_json:
        return json.load(city_json)

def get_vgsi_cities(url=_VGSIURL_):

    #checks if the json exists and loads it to update it.
    if path.exists(_VSGIPATH_):
        city_dict = open_vgsi_cities()
        json_exists = True
        sys.stdout.write(f"loading {_VSGIPATH_}, will overwrite if there's changes.\n")
    else:
        city_dict = {}
        json_exists = False
        sys.stdout.write(f"creating new {_VSGIPATH_} from {_VGSIURL_}")

    page = requests.get(url, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")

    #checks for all links with the source as https://gis.vgsi.com/, 
    #also grabs the city names corresponding to the links.
    #writing to dictionary. example below
    #{newhaven, ct: {url: gis.com, type: vgsi}}

    #east lyme 8625
    for city_row in soup.find_all(href = re.compile("https://gis.vgsi.com/")):
        if not json_exists:
            city_dict[city_row.get_text()] = {"url":None, "type":None, "upper_limit":-1} #adding empty key to update after.
        city_dict[city_row.get_text()]['url'] = city_row.get_attribute_list("href")[0] + "Parcel.aspx?pid="
        city_dict[city_row.get_text()]['type'] = "vgsi"

    with open(_VSGIPATH_, "w") as outfile:
        json.dump(city_dict, outfile, indent=4)
    sys.stdout.write(f"updated {_VSGIPATH_}")
    

#extracts one item. compartementalizing for now
def get_item(span_id, soup):
    item = soup.find("""span""", id=span_id).get_text()
    return item

#extracts the landlord value from the Hamden vgsi.
#could map these to different methods depending on the site,
#as they would all be different.
def extract_landlord(url):
	page = requests.get(url, verify=False) #ssl errors lol
	landlord_span_id = "MainContent_lblGenOwner" #id for "owner" in vgsi
	soup = BeautifulSoup(page.content, "html.parser")
	return get_item(landlord_span_id, soup)

def arguments(argv):
    arg_url = ""
    arg_pid = ""
    arg_help = "{0} -u <url>".format(argv[0])
    
    try:
        opts, args = getopt.getopt(argv[1:], "u:h:j", ["url=", "help", "json"])
    except:
        print(arg_help)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-u", "--url"):
            arg_url = arg
        elif opt in ("-j", "--json"):
            get_vgsi_cities()

    return arg_url


if __name__ == "__main__":
	url = arguments(sys.argv)