import importlib.util

class Feature:
    def __init__(self, name: str, transform_path: str):
        self.name = name        # Self explanatory
        # self.dtype = dtype      # Type of the feature
        self.transform_path = transform_path    # SQL query / Spark job to compute the feature

    # def validate(self, value):
    #     """Ensures incoming data matches feature definition"""
    #     if not isinstance(value, self.dtype):
    #         raise ValueError(f"Feature {self.name} expects type {self.dtype}, got {type(value)}")

    def compute(self, df):
        spec = importlib.util.spec_from_file_location("transform_module", self.transform_path)
        
        transform_module = importlib.util.module_from_spec(spec)
        
        spec.loader.exec_module(transform_module)
        
        if hasattr(transform_module, 'compute_driver_stats'):
            return transform_module.compute_driver_stats(df)
        else:
            raise AttributeError(f"Module at {self.transform_path} has no 'compute_driver_stats' function.")