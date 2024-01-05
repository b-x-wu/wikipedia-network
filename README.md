# Wikipedia Network

Scripts to generate a graph modeling Wikipedia articles and their relationships in a local Neo4j database.

## Runbook

1. Download a dump of Wikipedia to the package root with the instructions [here](https://en.wikipedia.org/wiki/Wikipedia:Database_download).
    1. Be sure to download the multistream BZ2 dump.
2. Start a local instance of a Neo4j database.
3. In the database configuration, change the following settings and restart if needed.
    1. Remove the `server.directories.import` setting.
    2. If you experience connection issues, set `server.default_listen_address=0.0.0.0`.
4. Create a file in the project root directory with the name `.env` and set the following variables. Note that these are for the default Neo4j settings.
    1. `NEO4J_URI="bolt://localhost:7687"`
    2. `NEO4J_USERNAME="neo4j"`
    3. `NEO4J_PASSWORD="<password>"`
    4. `WIKIPEDIA_ZIP_FILE_NAME="<wikipedia_dump_file_name>"`
5. In the package root, run `npm install && npm run start:write_wikipedia_network`
