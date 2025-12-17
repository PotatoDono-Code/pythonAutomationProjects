from .base_extractor import BaseExtractor

def safe_int(val):
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
        return None

class AncestryExtractor(BaseExtractor):
    
    def extract_main(self):

        return {
            "id" : self.id,
            "name" : self.retrieve("name"),
            "type" : "ancestry",
            "rarity" : self.retrieve("system", "traits", "rarity"),
            "description" : self.retrieve("system", "description", "value"),
            "hp" : safe_int(self.retrieve("system", "hp")),
            "reach" : safe_int(self.retrieve("system", "reach")),
            "size" : self.retrieve("system", "size"),
            "speed" : safe_int(self.retrieve("system", "speed")),
            "vision" : self.retrieve("system", "vision"),
            "bonus_language_count" : self.retrieve("system", "additionalLanguages", "count")
        }
    
    def extract_meta(self):

        return {
            "id" : self.id,
            "source" : self.retrieve("system", "publication", "title"),
            "remaster" : self.retrieve("system", "publication", "remaster"),
            "license" : self.retrieve("system", "publication", "license")
        }
    
    def extract_flaw(self):
        
        flaw = self.retrieve("system", "flaws", "0", "value")
        if not flaw:
            return None
        
        return {
            "id" : self.id,
            "flaw" : flaw[0]
        }

    def extract_boosts(self):
        results = []

        boost_block = self.sys.get("boosts", {}) or {}
        if not isinstance(boost_block, dict):
            return results

        for i, (_, boost) in enumerate(boost_block.items()):
            boost_stat = boost.get("value")
            if not boost_stat:
                i -= 1
                continue
            
            if len(boost_stat) > 1:
                boost_value = "free"
            else:
                boost_value = boost_stat[0]

            results.append({
                "id" : self.id,
                "boost_index" : i,
                "stat" : boost_value
            })
        
        return results
    
    def extract_languages(self):
        results = []

        language_list = self.retrieve("system", "languages", "value") or []
        
        for each in language_list:
            results.append({
                "id" : self.id,
                "language" : each
            })

        return results

    def extract_additional_languages(self):
        results = []

        additional_language_list = self.retrieve("system", "additionalLanguages", "value") or []
        
        for each in additional_language_list:
            results.append({
                "id" : self.id,
                "language" : each
            })

        return results
    
    def extract_race_features(self):
        results = []

        feature_block = self.sys.get("items", {}) or {}
        if not isinstance(feature_block, dict):
            return results
        
        for i, (_, feature) in enumerate(feature_block.items()):
            results.append({
                "id" : self.id,
                "feature_index" : i,
                "feature" : feature.get("name")
            })
        
        return results
        
    def extract_traits(self):
        results = []

        traits_list = self.retrieve("system", "traits", "value") or []

        for each in traits_list:
            results.append({
                "id" : self.id,
                "trait" : each
            })

        return results
    
    def extract_all(self):
        return {
        "main": self.extract_main(),
        "meta": self.extract_meta(),
        "boosts": self.extract_boosts(),
        "flaw": self.extract_flaw(),
        "traits": self.extract_traits(),
        "languages": self.extract_languages(),
        "additional_languages": self.extract_additional_languages(),
        "race_features": self.extract_race_features()
    }