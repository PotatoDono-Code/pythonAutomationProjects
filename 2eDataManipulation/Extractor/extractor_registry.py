from Extractor.spell_extractor import SpellExtractor
from Extractor.ancestry_extractor import AncestryExtractor

EXTRACTOR_REGISTRY = {
    "spell" : SpellExtractor,
    "ancestry" : AncestryExtractor
}