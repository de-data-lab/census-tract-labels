from fuzzywuzzy import fuzz
from fuzzywuzzy import process


class censusTractLabel:

    def __init__(self, addresses, method):
        self.data = addresses
        self.method = method

    def fuzzy_match(self, min_score=70, num_of_words=2):
        list_of_prefixes = ["east", "west", "north", "south", "street",
                            "road", "avenue", "close", "court", "boulevarde",
                            "drive", "place", "square", "parade", "circuit"]
        address = self.data
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


adress = censusTractLabel('93 main st newark de 19713', 'single')
print(adress.fuzzy_match())
