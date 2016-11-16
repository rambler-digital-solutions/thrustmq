class Message:

    def __init__(self, bucket_id, data):
        self.bucket_id = bucket_id
        self.data = data
        self.length = len(data)

    def __str__(self):
        return "{} {}".format(bucket_id, data)
