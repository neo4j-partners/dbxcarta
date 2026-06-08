# Questions for Customer

We wanted to understand how you plan to use this project. A few quick questions:

- **What is your primary use case?** What problem are you trying to solve with it?

- **Does this align with how it was designed?** It was primarily built to ingest your Unity Catalog schema into Neo4j and make it searchable for agents via GraphRAG. As part of that, it creates vector embeddings of the metadata itself: catalogs, schemas, tables, columns, and sampled column values. Each one is embedded with its full name path plus any comments, and they are stored as Neo4j vector indexes so agents can search the schema semantically. Is that the direction you're heading?

- **What's the scale?** Roughly how many catalogs, schemas, and tables are you working with?

- **How rich is your schema metadata?** Do your tables have foreign keys and constraints actually defined in Unity Catalog, and do tables/columns carry comments or descriptions? The quality of the graph depends a lot on this.

- **Can we sample your data?** Is it okay to read sample column values and build embeddings from them, or is there PII / sensitive data that limits sampling?

- **Do you already run Neo4j?** If so, is it Aura or self-hosted, and what version? Otherwise we'd help you stand one up.

- **What are you using today?** Genie, hand-written prompts, something else, or nothing yet? Where does it fall short for you?

- **Who or what will be querying it?** Are agents the main consumer, or do you also have people exploring the graph directly?

- **What does success look like for you?** What would make this project a win in your environment?
