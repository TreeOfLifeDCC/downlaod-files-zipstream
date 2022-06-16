import io
import json

import httpx
from fastapi import FastAPI, Form
from fastapi.responses import StreamingResponse

from zipstream import ZipStream
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import RequestsHttpConnection
from elasticsearch import Elasticsearch

app = FastAPI()
es = Elasticsearch("ttp://elasticsearch.default:9200",
                   connection_class=RequestsHttpConnection,
                   use_ssl=False, verify_certs=False)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/download")
async def root():
    zip_buffer = io.BytesIO()
    files = []
    urls = ['https://www.ebi.ac.uk/ena/browser/api/fasta/GCA_939531405?download=true&gzip=true',
            'https://www.ebi.ac.uk/ena/browser/api/fasta/GCA_927399515?download=true&gzip=true']
    # with zipstream.ZipFile(zip_buffer,"w", zipfile.ZIP_DEFLATED, False) as zf:
    file_name = 'GCA_939531405'
    # url = "https://www.ebi.ac.uk/ena/browser/api/fasta/GCA_939531405?download=true&gzip=true"
    # print(url)

    # print("Downloading file:%s" % file_name)
    # r = requests.get(url, stream=True)
    # data = b''
    # # for chunk in r.iter_content(chunk_size=1024):
    # if r.status_code == '<Response [200]>':
    # for chunk in r.content(1024):
    # z.write(g)
    # zf.write_iter(get_source_bytes_iter(url))
    # urls.append(url)
    # create response object
    for url in urls:
        files.append({'stream': s3_content_generator(url), 'name': url.split('/')[-1]})
    # download started
    # zip_inf = zipstream.ZipInfo(file_name)
    # z.write(data)
    # def iterfile():  #
    #     with open(some_file_path, mode="rb") as file_like:  #
    #         yield from file_like  #

    zip_buffer.seek(0)
    # z.seek(0)
    zf = ZipStream(files, chunksize=32768)

    response = StreamingResponse(zf.stream(), media_type='application/zip')
    # response['Content-Disposition'] = 'attachment; filename={}'.format('files.zip')
    return response


def s3_content_generator(url):
    # s3_bucket - your s3 bucket name
    with httpx.stream('GET', url) as r:
        yield from r.iter_bytes()


@app.post("/files/assemblies")
def download(taxonomyFilter: str = Form()):
    taxonomyFilter1 = json.loads(taxonomyFilter)
    print(taxonomyFilter)
    print(len(taxonomyFilter1))
    print(taxonomyFilter1[0])
    files = []
    query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
    if taxonomyFilter:
        for index, taxonomy in enumerate(taxonomyFilter1):
            print(taxonomy.get('rank'))
            if len(taxonomyFilter1) == 1:
                query_param = query_param + '"nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}} '
            elif (len(taxonomyFilter1) - 1) == index:
                query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}}} '
            else:
                query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName" :''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}}}, '

        query_param = query_param + '] }}}'
        print(query_param)
        data_portal = es.search(index="data_portal", size=10000, body=query_param)
        for organism in data_portal['hits']['hits']:
            print(organism['_id'])
            # print(organism.get('_source').get("assemblies"))
            if organism.get('_source').get("assemblies"):
                print(len(organism.get('_source').get("assemblies")))
                for assemblies in organism.get('_source').get("assemblies"):
                    print(assemblies.get("accession"))
                    url = "https://www.ebi.ac.uk/ena/browser/api/fasta/" + assemblies.get("accession") + "?download" \
                                                                                                         "=true&gzip" \
                                                                                                         "=true "
                    print(url)
                    files.append(
                        {'stream': s3_content_generator(url), 'name': assemblies.get("accession") + '.fasta.gz'})

        zf = ZipStream(files, chunksize=32768)

        response = StreamingResponse(zf.stream(), media_type='application/zip')
        return response
    else:
        return {"Please Provide a Valid input fields"}


@app.post("/files/annotations")
def download(taxonomyFilter: str = Form()):
    taxonomyFilter1 = json.loads(taxonomyFilter)
    print(taxonomyFilter)
    print(len(taxonomyFilter1))
    print(taxonomyFilter1[0])
    files = []
    query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
    if taxonomyFilter:
        for index, taxonomy in enumerate(taxonomyFilter1):
            print(taxonomy.get('rank'))
            if len(taxonomyFilter1) == 1:
                query_param = query_param + '"nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}} '
            elif (len(taxonomyFilter1) - 1) == index:
                query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}}} '
            else:
                query_param = query_param + '{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName" :''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}}}, '

        query_param = query_param + '] }}}'
        x = 0
        print(query_param)
        data_portal = es.search(index="data_portal", size=10000, body=query_param)
        names = list()
        for organism in data_portal['hits']['hits']:
            print(organism['_id'])
            # print(organism.get('_source').get("assemblies"))
            if organism.get('_source').get("annotation"):
                print(len(organism.get('_source').get("annotation")))
                print(organism.get('_source').get("annotation")[0].get("annotation"))
                for annotationObj in organism.get('_source').get("annotation"):
                    print(annotationObj.get("accession"))
                    url = "https://www.ebi.ac.uk/ena/browser/api/fasta/" + annotationObj.get("accession") + "?download" \
                                                                                                            "=true&gzip" \
                                                                                                            "=true "
                    print(url)
                    files.append(
                        {'stream': s3_content_generator(url), 'name': annotationObj.get("accession") + '.fasta.gz'})

            print('---------------------------------------------')
        zf = ZipStream(files, chunksize=32768)

        response = StreamingResponse(zf.stream(), media_type='application/zip')
        return response
    else:
        return {"Please Provide a Valid input fields"}
