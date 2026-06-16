---
name: dbclient-engine-search
description: >
  Search / log-retrieval engines in DBClient: Elasticsearch 8.x and OpenSearch.
  Use this skill when adding, debugging, or modifying such a Tester.
---

# DBClient Engine: Search

## Covered Testers and Aliases

| Engine | Tester | Aliases |
|---|---|---|
| Elasticsearch 8.x | `ElasticSearchTester.java` | `elasticsearch`, `elasticsearch8`, `elastic`, `es` |
| Elasticsearch 7.x / OpenSearch | `OpenSearchTester.java` | `opensearch`, `elasticsearch7` |

## Key Dependencies (`build.gradle`)

```groovy
// Elasticsearch 8.x
implementation 'co.elastic.clients:elasticsearch-java:8.8.2'

// OpenSearch / Elasticsearch 7.x
implementation 'org.opensearch.client:opensearch-rest-high-level-client:1.2.4'
implementation 'org.apache.httpcomponents.client5:httpclient5:5.2.1'
```

## Key Concerns

- Elasticsearch 8 and OpenSearch client APIs have diverged, hence two separate Testers.
- Both usually speak REST/HTTP; the query parameter can be DSL JSON or a simplified query string, depending on the Tester.
- Authentication, TLS, and index name handling differ; check the specific Tester implementation.

## Minimal Verification Command (Elasticsearch 8 Example)

```bash
java -jar build/libs/oneclient-1.0-all.jar \
  -h 127.0.0.1 -P 9200 -e elasticsearch -t query \
  -q '{ "query": { "match_all": {} } }'
```

## Maintenance Notes
- Do not register `es` / `elasticsearch` to `OpenSearchTester`.
- When upgrading the client SDK, watch for transport protocol version and Java API breaking changes.
