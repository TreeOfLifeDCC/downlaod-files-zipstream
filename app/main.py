import json
from typing import Optional
import zipstream
import httpx
from fastapi import FastAPI, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import RequestsHttpConnection
from elasticsearch import Elasticsearch

app = FastAPI()
es = Elasticsearch("http://elasticsearch.default:9200",
                   connection_class=RequestsHttpConnection,
                   use_ssl=False, verify_certs=False)
# es = Elasticsearch('http://45.88.81.118:80/elasticsearch', connection_class=RequestsHttpConnection,
#                    use_ssl=False, verify_certs=False, timeout=10000)
origins = ["*"]
taxaRankArray = ['superkingdom', 'kingdom', 'subkingdom', 'superphylum', 'phylum', 'subphylum', 'superclass', 'class',
                 'subclass', 'infraclass', 'cohort', 'subcohort', 'superorder', 'order', 'suborder', 'infraorder',
                 'parvorder', 'section', 'subsection', 'superfamily', 'family', 'subfamily', 'tribe', 'subtribe',
                 'genus', 'series', 'subgenus', 'species_group', 'species_subgroup', 'species', 'subspecies',
                 'varietas',
                 'forma']

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def ena_content_generator(url):
    print(url)
    with httpx.stream('GET', url, timeout=None) as r:
        yield from r.iter_bytes()


@app.post("/files/assemblies")
def download(taxonomyFilter: str = Form(), filter: Optional[str] = Form()):
    taxonomyFilter1 = json.loads(taxonomyFilter)
    if filter:
        filterArray = filter.split(",")

    query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
    if taxonomyFilter:
        for index, taxonomy in enumerate(taxonomyFilter1):
            if len(taxonomyFilter1) == 1:
                query_param = query_param + ' { "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}} }'
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
        if len(filterArray) > 0:
            for filterVal in filterArray:
                print(filterVal)
                split_array = filterVal.split("-")
                print(split_array)
                if split_array and split_array[0].strip() == 'Biosamples':
                    query_param = query_param + ',{ "terms" : { "biosamples" : [''"' + split_array[1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Raw data':
                    query_param = query_param + ',{ "terms" : { "raw_data" : [''"' + split_array[1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Mapped reads':
                    query_param = query_param + ',{ "terms" : { "mapped_reads" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Assemblies':
                    query_param = query_param + ',{ "terms" : { "assemblies_status" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Annotation complete':
                    query_param = query_param + ',{ "terms" : { "annotation_complete" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Annotation':
                    query_param = query_param + ',{ "terms" : { "annotation_status" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Genome Notes':
                    query_param = query_param + ',{ "nested": {"path": "genome_notes","query": {"bool": {"must": [{"exists": {"field": "genome_notes.url"}}]}}}}'
                elif split_array and split_array[0].strip() in taxaRankArray:
                    query_param = query_param + ',{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                                '"path" : ' \
                                                '"taxonomies.' + split_array[
                                      0].strip() + '"' ', "query" : { "bool" : { ' \
                                                   '"must" : [{ ' \
                                                   '"term" : { ' \
                                                   '"taxonomies.' + split_array[
                                      0].strip() + '.tax_id" :''"' + split_array[1].strip() + '"' '}}]}}}}}} '
                else:
                    query_param = query_param + ',{ "nested" : { "path": "experiment", "query" : { "bool" : { "must" : [' \
                                                '{ "term" : { "experiment.library_construction_protocol.keyword" : ' + \
                                  '"' + filterVal + '"' '}}]}}}}'

        query_param = query_param + '] }}}'
        print(query_param)
        data_portal = es.search(index="data_portal", size=10000, body=query_param)
        z = zipstream.ZipFile(allowZip64=True)
        for organism in data_portal['hits']['hits']:
            print(organism['_id'])
            # print(organism.get('_source').get("assemblies"))
            if organism.get('_source').get("assemblies"):
                print(len(organism.get('_source').get("assemblies")))
                for assemblies in organism.get('_source').get("assemblies"):
                    print(assemblies)
                    url = "https://www.ebi.ac.uk/ena/browser/api/fasta/" + assemblies.get("accession") + "?download" \
                                                                                                         "=true&gzip" \
                                                                                                         "=true "
                    print(url)
                    if assemblies.get("version"):
                        z.write_iter(assemblies.get("accession") + '.' + assemblies.get("version") + '.fasta.gz',
                                     ena_content_generator(url))
                    else:
                        z.write_iter(assemblies.get("accession") + '.fasta.gz', ena_content_generator(url))
                # z.write_iter('GCA_905147045.1.fasta.gz',
                #                  ena_content_generator("https://www.ebi.ac.uk/ena/browser/api/embl/GCA_905147045.1?download=true&gzip=true"))

        def generator():
            for chunk in z:
                yield chunk

        response = StreamingResponse(generator(), media_type='application/zip')
        return response
    else:
        return {"Please Provide a Valid input fields"}


@app.post("/files/annotations")
def download(taxonomyFilter: str = Form(), filter: Optional[str] = Form()):
    taxonomyFilter1 = json.loads(taxonomyFilter)
    if filter:
        filterArray = filter.split(",")

    query_param = ' { "'"from"'" : 0, "'"size"'" : 5000, "'"query"'" : { "'"bool"'" : { "'"must"'" : [ '
    if taxonomyFilter:
        for index, taxonomy in enumerate(taxonomyFilter1):
            if len(taxonomyFilter1) == 1:
                query_param = query_param + ' { "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                            '"path" : ' \
                                            '"taxonomies.' + taxonomy.get(
                    'rank') + '"' ', "query" : { "bool" : { "must" : [{ ' \
                              '"term" : { ' \
                              '"taxonomies.' + taxonomy.get('rank') + '.scientificName":''"' + taxonomy.get(
                    'taxonomy') + '"' '}}]}}}}} }'
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
        if len(filterArray) > 0:
            for filterVal in filterArray:
                print(filterVal)
                split_array = filterVal.split("-")
                if split_array and split_array[0].strip() == 'Biosamples':
                    query_param = query_param + ',{ "terms" : { "biosamples" : [''"' + split_array[1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Raw data':
                    query_param = query_param + ',{ "terms" : { "raw_data" : [''"' + split_array[1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Mapped reads':
                    query_param = query_param + ',{ "terms" : { "mapped_reads" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Assemblies':
                    query_param = query_param + ',{ "terms" : { "assemblies_status" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Annotation complete':
                    query_param = query_param + ',{ "terms" : { "annotation_complete" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Annotation':
                    query_param = query_param + ',{ "terms" : { "annotation_status" : [''"' + split_array[
                        1].strip() + '"'']}}'
                elif split_array and split_array[0].strip() == 'Genome Notes':
                    query_param = query_param + ',{ "nested": {"path": "genome_notes","query": {"bool": {"must": [{"exists": {"field": "genome_notes.url"}}]}}}}'
                elif split_array and split_array[0].strip() in taxaRankArray:
                    query_param = query_param + ',{ "nested" : { "path" : "taxonomies", "query" : { "nested" : { ' \
                                                '"path" : ' \
                                                '"taxonomies.' + split_array[
                                      0].strip() + '"' ', "query" : { "bool" : { ' \
                                                   '"must" : [{ ' \
                                                   '"term" : { ' \
                                                   '"taxonomies.' + split_array[
                                      0].strip() + '.tax_id" :''"' + split_array[1].strip() + '"' '}}]}}}}}} '
                else:
                    query_param = query_param + ',{ "nested" : { "path": "experiment", "query" : { "bool" : { "must" : [' \
                                                '{ "term" : { "experiment.library_construction_protocol.keyword" : ' + \
                                  '"' + filterVal + '"' '}}]}}}}'

        query_param = query_param + '] }}}'
        print(query_param)
        data_portal = es.search(index="data_portal", size=10000, body=query_param)
        z = zipstream.ZipFile(allowZip64=True)
        for organism in data_portal['hits']['hits']:
            if organism.get('_source').get("annotation"):
                print(len(organism.get('_source').get("annotation")))
                print(organism.get('_source').get("annotation")[0].get("annotation"))
                for annotationObj in organism.get('_source').get("annotation"):
                    print(annotationObj.get("accession"))
                    if annotationObj.get('annotation'):
                        urlGFT = annotationObj.get('annotation').get('GTF')
                        z.write_iter('GFT/' + urlGFT.split('/')[-1], ena_content_generator(urlGFT))
                        urlGFF3 = annotationObj.get('annotation').get('GFF3')
                        z.write_iter('GFF3/' + urlGFF3.split('/')[-1], ena_content_generator(urlGFF3))
                    if annotationObj.get('proteins'):
                        url_proteins = annotationObj.get('proteins').get('FASTA')
                        z.write_iter('proteins/' + url_proteins.split('/')[-1], ena_content_generator(url_proteins))
                    if annotationObj.get('softmasked_genome'):
                        url_softmasked_genome = annotationObj.get('softmasked_genome').get('FASTA')
                        z.write_iter('softmaskedGenome/' + url_softmasked_genome.split('/')[-1],
                                     ena_content_generator(url_softmasked_genome))
                    if annotationObj.get('transcripts'):
                        url_transcripts = annotationObj.get('transcripts').get('FASTA')
                        z.write_iter('transcripts/' + url_transcripts.split('/')[-1],
                                     ena_content_generator(url_transcripts))
            # z.write_iter('GCA_905147045.1.fa.gz',
            #              ena_content_generator(
            #                  "http://ftp.ensembl.org/pub/rapid-release/species/Inachis_io/GCA_905147045.1/geneset/2021_05/Inachis_io-GCA_905147045.1-2021_05-pep.fa.gz"))

        def generator():
            for chunk in z:
                yield chunk

        response = StreamingResponse(generator(), media_type='application/zip')
        return response
    else:
        return {"Please Provide a Valid input fields"}
