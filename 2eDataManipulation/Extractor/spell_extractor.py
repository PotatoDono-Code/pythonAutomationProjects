from .base_extractor import BaseExtractor

def safe_int(val):
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
        return None

class SpellExtractor(BaseExtractor):
    
    def extract_main(self):
        spell_type = self.retrieve("type")
        traits = self.retrieve("system", "traits", "value") or []


        if self.retrieve("system", "ritual"):
            spell_type = "ritual"
        elif "focus" in traits:
            spell_type = "focus"


        return {
            "id" : self.id,
            "name" : self.retrieve("name"),
            "level" : safe_int(self.retrieve("system", "level", "value")),
            "type" : spell_type,
            "rarity" : self.retrieve("system", "traits", "rarity")
        }
    
    def extract_meta(self):

        return {
            "id" : self.id,
            "source" : self.retrieve("system", "publication", "title"),
            "remaster" : self.retrieve("system", "publication", "remaster"),
            "license" : self.retrieve("system", "publication", "license")
        }
    
    def extract_details(self):

        return {
            "id" : self.id,
            "cost" : self.retrieve("system", "cost", "value"),
            "sustained" : self.retrieve("system", "duration", "sustained"),
            "duration" : self.retrieve("system", "duration", "value"),
            "range" : self.retrieve("system", "range", "value"),
            "targets" : self.retrieve("system", "target", "value"),
            "cast_time" : self.retrieve("system", "time", "value"),
            "area_type" : self.retrieve("system", "area", "type"),
            "area_range" : safe_int(self.retrieve("system", "area", "value")),
            "save" : self.retrieve("system", "defense", "save", "statistic"),
            "basic" : self.retrieve("system", "defense", "save", "basic"),
            "description" : self.retrieve("system", "description", "value")
        }
    
    def extract_damage(self):
        results = []

        damage_block = self.sys.get("damage", {}) or {}
        if not isinstance(damage_block, dict):
            return results

        for i, (_, dmg) in enumerate(damage_block.items()):
            results.append({
                "id" : self.id,
                "damage_index" : i,
                "damage" : dmg.get("formula"),
                "damage_type" : dmg.get("type"),
                "persistent" : dmg.get("category"),
                "mod" : dmg.get("applyMod"),
                "kind" : dmg.get("kinds"),
                "materials" : dmg.get("materials")    
            })

        return results
    
    def extract_heighten(self):
        h = self.sys.get("heightening", {}) or {}
        if not h:
            return None
        
        return {
            "id" : self.id,
            "type" : self.retrieve("system", "heightening", "type"),
            "interval" : safe_int(self.retrieve("system", "heightening", "interval"))
        }
    
    def extract_heighten_interval(self):
        results = []

        interval_block = self.sys.get("heightening", {}) or {}

        if interval_block.get("type") != "interval":
            return results
        
        int_dmg_block = interval_block.get("damage", {}) or {}
        if not isinstance(int_dmg_block, dict):
            return results
        
        for i, (_, dmg) in enumerate(int_dmg_block.items()):
             results.append({
                "id" : self.id,
                "damage_index" : i,
                "damage" : dmg
            })
        
        return results
    
    def extract_heighten_level(self):
        results = []
        damage_results = []

        heighten = self.sys.get("heightening", {}) or {}

        if heighten.get("type") == "interval":
            return results, damage_results

        level_block = heighten.get("levels", {}) or {}
        
        if not isinstance(level_block, dict):
            return results, damage_results
        
        for level_block_index, entry in level_block.items():
            area = entry.get("area", {}) or {}

            results.append({
                "id" : self.id,
                "heighten_level" : level_block_index,
                "area" : area.get("type"),
                "area_type" : area.get("area_type"),
                "area_value" : safe_int(area.get("value")),
                "range" : (entry.get("range") or {}).get("value"),
                "target" : (entry.get("target") or {}).get("value")
            })

            level_dmg_block = entry.get("damage", {}) or {}

            if not isinstance(level_dmg_block, dict):
                continue

            for i, (_, dmg) in enumerate(level_dmg_block.items()):
                damage_results.append({
                    "id" : self.id,
                    "heighten_level" : level_block_index,
                    "damage_index" : i,
                    "damage" : dmg.get("formula"),
                    "damage_type" : dmg.get("type"),
                    "persistent" : dmg.get("category"),
                    "mod" : dmg.get("applyMod"),
                    "kind" : dmg.get("kinds"),
                    "materials" : dmg.get("materials")
                })

        return results, damage_results

    def extract_traits(self):
        results = []

        traits_list = self.sys.get("traits").get("value") or []

        for each in traits_list:
            results.append({
                "id" : self.id,
                "trait" : each
            })

        return results
    
    def extract_traditions(self):
        results = []

        trad = self.sys.get("traits").get("traditions") or []

        for each in trad:
            results.append({
                "id" : self.id,
                "tradition" : each
            })

        return results
    
    def extract_ritual(self):
        
        primary_check = self.retrieve("system", "ritual", "primary", "check")
        if primary_check is None:
            return None

        return {
            "id" : self.id,
            "primary_check" : primary_check,
            "secondary_check" : self.retrieve("system", "ritual", "secondary", "checks"),
            "ritual_description" : self.retrieve("system", "description", "gm"),
            "secondary_casters" : safe_int(self.retrieve("system", "ritual", "secondary", "casters"))
        }
    
    def extract_all(self):
        level, level_damage = self.extract_heighten_level()
        return {
        "main": self.extract_main(),
        "meta": self.extract_meta(),
        "details": self.extract_details(),
        "damage": self.extract_damage(),
        "traits": self.extract_traits(),
        "traditions": self.extract_traditions(),
        "heightening": self.extract_heighten(),
        "heighten_interval": self.extract_heighten_interval(),
        "heighten_level": level,
        "heighten_level_damage": level_damage,
        "ritual": self.extract_ritual(),
    }