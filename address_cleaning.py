from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def prefix_replacemnt(address):
    if address is not None:
        address = address[0]
        address = address.replace("east", "E")
        address = address.replace("west", "W")
        address = address.replace("north", "N")
        address = address.replace("south", "S")
        address = address.replace("street", "st")
        address = address.replace("road", "rd")
        address = address.replace("avenue", "ave")
        address = address.replace("close", "cl")
        address = address.replace("court", "ct")
        address = address.replace("boulevarde", "blvd")
        address = address.replace("drive", "dr")
        address = address.replace("place", "pl")
        address = address.replace("square", "sq")
        address = address.replace("parade", "pde")
        address = address.replace("circuit", "cct")
        return address

def fuzzy_matching(address, min_score=70, num_of_words=2):
    list_of_prefixes = ["east", "west", "north", "south", "street",
                        "road", "avenue", "close", "court", "boulevarde",
                        "drive", "place", "square", "parade", "circuit"]
    address = address
    highest = process.extract(address, list_of_prefixes, limit=num_of_words)
    for k in highest:
        address_componeents = address.split()
        for i in address_componeents:
            score = fuzz.ratio(k[0], i)
            if score > min_score:
                word = i
                matched_word = k[0]
                address = address.replace(word, matched_word)
    return address

