from faust.serializers import codecs
from faust_s3_backed_serializer import S3BackedSerializer

import faust

app = faust.App("faust-s3-backed-demo", broker="kafka://localhost:9094")

value_serializer = "s3_backed_str_serializer"
str_serializer = codecs.get_codec("raw")
s3_serializer = S3BackedSerializer(output_topic=None,
                                   base_path="s3://s3-backed-659420695732/",
                                   region_name=None,
                                   is_key=False)
codecs.register(value_serializer, str_serializer | s3_serializer)
schema = faust.Schema(
    key_type=str,
    value_type=str,
    key_serializer="raw",
    value_serializer=value_serializer
)
texts_topic = app.topic("texts", schema=schema)


@app.agent(texts_topic)
async def print_length(texts):
    async for key, text in texts.items():
        print("{} has {} characters".format(key, len(text)))
