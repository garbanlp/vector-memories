import uuid
from time import monotonic

import weaviate
from weaviate_sample.const import PATH_DATA, SAMPLE_FILE

_URL_WEA = 'http://localhost:8080'
_WEAVIATE_CLASS_NAME = "TextExample"

# texts = ["Este es el primer texto de prueba.",
# 'me gustan las manzanas', 'no me gustan las peras']
# id_texts = ['ded068b2-6aab-4882-b68f-b1f81be04c4e',
# '68ba8add-6499-4467-8e14-1f45f24bb49b',
# '5bd43c57-530b-4dc3-932d-2f78681fbabc']
texts = list({row.split('\t')[-1] for row in (PATH_DATA / SAMPLE_FILE).read_text().split('\n') if row})
id_texts = [str(uuid.uuid4()) for _ in texts]


def _create_weaviate_schema():
    client = weaviate.Client(_URL_WEA)
    client.schema.delete_all()

    weaviate_schema = {
        "class": _WEAVIATE_CLASS_NAME,
        "description": "A text example",
        "properties": [
            {
                "name": "text",
                "dataType": ["string"],
                "description": "Just plane text",
            },
        ]
    }
    client.schema.create_class(weaviate_schema)


def _add_data():
    client = weaviate.Client(_URL_WEA)

    for id_t, text in zip(id_texts, texts):
        text_sample = {
            'text': text
        }
        result = client.data_object.validate(
            data_object=text_sample,
            class_name=_WEAVIATE_CLASS_NAME,
            uuid=id_t
        )
        assert result['valid']  # so sad do this manually :(
        tic = monotonic()
        client.data_object.create(
            data_object=text_sample,
            class_name=_WEAVIATE_CLASS_NAME,
            uuid=id_t
        )
        print(f"create text sample took {monotonic()- tic:.2f}")


def _add_batch_sample():
    _create_weaviate_schema()
    tic = monotonic()
    _add_data()
    print(f"ALL DATA FEED IN WEAVIATE TOOK {monotonic() - tic:.2f}")


if __name__ == '__main__':
    _add_batch_sample()




