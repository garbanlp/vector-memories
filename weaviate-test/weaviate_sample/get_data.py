import httpx
import gzip

from weaviate.const import PATH_DATA, SAMPLE_FILE

url_data = "https://sbert.net/datasets/stsbenchmark.tsv.gz"
file_name_gz = "stsbenchmark.tsv.gz"


def _download_data_locally():
    PATH_DATA.mkdir(parents=True, exist_ok=True)
    response = httpx.get(url_data, follow_redirects=True)
    path_local = (PATH_DATA / file_name_gz)
    if response.status_code == 200:
        path_local.write_bytes(response.content)
    else:
        print(f"Request failed with status code: {response.status_code}")
        return

    with gzip.open(path_local, 'rb') as gz_file:
        (PATH_DATA / SAMPLE_FILE).write_bytes(gz_file.read())
    path_local.unlink()


if __name__ == '__main__':
    _download_data_locally()
