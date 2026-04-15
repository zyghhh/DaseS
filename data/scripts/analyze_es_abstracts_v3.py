import asyncio
from elasticsearch import AsyncElasticsearch
import json

async def run_analysis():
    es = AsyncElasticsearch(
        hosts=["http://49.52.27.139:9200"],
        request_timeout=60,
    )
    alias = "dblp_search"

    try:
        # 1. Total count
        total_resp = await es.count(index=alias)
        total_count = total_resp['count']
        
        # 2. Count with abstract_source (which we assume means they have abstracts)
        with_abstract_resp = await es.count(index=alias, query={
            "exists": {"field": "abstract_source"}
        })
        with_abstract_count = with_abstract_resp['count']
        
        print(f"Total documents: {total_count}")
        print(f"Documents with abstract: {with_abstract_count} ({with_abstract_count/total_count*100:.2f}%)")
        print(f"Documents without abstract: {total_count - with_abstract_count} ({(total_count - with_abstract_count)/total_count*100:.2f}%)")

        # 3. Analyze coverage by pub_type
        print("\n--- Coverage by Publication Type ---")
        pub_types_resp = await es.search(
            index=alias,
            aggs={
                "all_types": {
                    "terms": {"field": "pub_type", "size": 10},
                    "aggs": {
                        "has_abstract": {
                            "filter": {"exists": {"field": "abstract_source"}}
                        }
                    }
                }
            },
            size=0
        )
        
        for bucket in pub_types_resp['aggregations']['all_types']['buckets']:
            type_name = bucket['key']
            type_total = bucket['doc_count']
            type_with_abstract = bucket['has_abstract']['doc_count']
            percentage = (type_with_abstract / type_total * 100) if type_total > 0 else 0
            print(f"  {type_name:15}: {type_with_abstract:8} / {type_total:8} ({percentage:6.2f}%)")

        # 4. Analyze coverage by CCF Rating
        print("\n--- Coverage by CCF Rating ---")
        ccf_resp = await es.search(
            index=alias,
            aggs={
                "all_ratings": {
                    "terms": {"field": "ccf_rating", "size": 10},
                    "aggs": {
                        "has_abstract": {
                            "filter": {"exists": {"field": "abstract_source"}}
                        }
                    }
                }
            },
            size=0
        )
        
        for bucket in ccf_resp['aggregations']['all_ratings']['buckets']:
            rating = bucket['key']
            rating_total = bucket['doc_count']
            rating_with_abstract = bucket['has_abstract']['doc_count']
            percentage = (rating_with_abstract / rating_total * 100) if rating_total > 0 else 0
            print(f"  CCF-{rating:3}: {rating_with_abstract:8} / {rating_total:8} ({percentage:6.2f}%)")

        # 5. Verify a few documents with abstracts
        print("\n--- Verification Sample (with abstract_source) ---")
        sample_with_abstract = await es.search(
            index=alias, 
            query={"exists": {"field": "abstract_source"}},
            size=3
        )
        for hit in sample_with_abstract['hits']['hits']:
            src = hit['_source']
            abstract_len = len(str(src.get('abstract', '')))
            print(f"ID: {hit['_id']}, Source: {src.get('abstract_source')}, Abstract Length: {abstract_len} chars")
            if abstract_len > 100:
                print(f"  Snippet: {str(src.get('abstract'))[:100]}...")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await es.close()

if __name__ == "__main__":
    asyncio.run(run_analysis())
