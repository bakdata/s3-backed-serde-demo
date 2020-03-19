from faust.serializers import codecs
from faust_s3_backed_serializer import S3BackedSerializer

import faust

broker = "kafka://localhost:9094"
input_topic = "texts"

app = faust.App("faust-s3-backed-demo", broker=broker)

value_serializer = "s3_backed_str_serializer"
str_serializer = codecs.get_codec("raw")
s3_serializer = S3BackedSerializer()
codecs.register(value_serializer, str_serializer | s3_serializer)

schema = faust.Schema(
    key_type=str,
    key_serializer="raw",
    value_type=str,
    value_serializer=value_serializer
)
texts_topic = app.topic(input_topic, schema=schema)


@app.agent(texts_topic)
async def print_length(texts):
    async for key, text in texts.items():
        print("{} has {} characters".format(key, len(text)))


if __name__ == '__main__':
    app.main()
