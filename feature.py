class Feature:
    def __init__(self, name: str, dtype: type, source: str):
        self.name = name        # Self explanatory
        self.dtype = dtype      # Type of the feature
        self.source = source    # SQL query / Spark job to compute the feature

    def validate(self, value):
        """Ensures incoming data matches feature definition"""
        if not isinstance(value, self.dtype):
            raise ValueError(f"Feature {self.name} expects type {self.dtype}, got {type(value)}")
    