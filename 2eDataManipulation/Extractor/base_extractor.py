
## -  Universal extractor method for use in subsequent extractors

class BaseExtractor:
    
    ## - Initialize with full json file, system nested data, and unique _id field
    def __init__ (self, obj):
        self.obj = obj
        self.sys = obj.get("system", {})
        self.id = obj.get("_id")

    ## - Getter method to retrieve data in nested json cleanly
    def retrieve(self, *path):

        step = self.obj

        for each in path:
            if not isinstance(step, dict):
                return None
            step = step.get(each)
        
        return step
    
