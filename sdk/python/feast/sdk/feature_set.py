

class FeatureSet:
    def __init__(self):
        pass 
    
    def create_training_dataset(self, start_date, end_date):
        # need access to bigquery and the feature's table
        # and then create another table based on start_date and end_date
        pass
    
    def get_serving_data(self, entity_keys, type=None):
        pass