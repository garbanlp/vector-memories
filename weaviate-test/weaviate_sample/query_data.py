import argparse
from time import monotonic

import weaviate
_URL_WEA = 'http://localhost:8080'
_WEAVIATE_CLASS_NAME = "TextExample"


def _query_data(query: str, client: weaviate.Client ) -> list[str]:
    result =  client.query.get(class_name= _WEAVIATE_CLASS_NAME, properties='text').with_limit(5).with_near_text({'concepts': [query]}).do()
    return [row['text'] for row in result['data']['Get']['TextExample']]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Weaviate Query Script')
    parser.add_argument('query', type=str, help='Query string')
    args = parser.parse_args()

    client = weaviate.Client(_URL_WEA)
    tic = monotonic()
    similar_texts = _query_data(args.query, client)
    print(f'query took {monotonic()-tic:.2f}')
    print('-----')
    print('\n'.join(similar_texts))