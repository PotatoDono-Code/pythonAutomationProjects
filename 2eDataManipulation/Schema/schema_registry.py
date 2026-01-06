TYPE_REGISTRY = {
        "ancestry" : {
            "main": {"kind" : "one"},
            "meta": {"kind" : "one"},
            "boosts": {"kind" : "many"},
            "flaw": {"kind" : "one"},
            "traits": {"kind" : "many"},
            "languages":{"kind" : "many"},
            "additional_languages": {"kind" : "many"},
            "race_features": {"kind" : "many"}
        },
        "spell" : {
            "main" : {"kind" : "one"},
            "meta" : {"kind" : "one"},
            "details" : {"kind" : "one"},
            "damage" : {"kind" : "many"},
            "heightening": {"kind" : "one"},
            "heighten_interval" : {"kind" : "many"},
            "heighten_level" : {"kind" : "many"},
            "heighten_level_damage" : {"kind" : "many"},
            "traits" : {"kind" : "many"},
            "traditions" : {"kind" : "many"},
            "ritual" : {"kind" : "one"}
        }
    }