USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:////home/fran/Proyectos/PracticaNeo4j/conferences.csv"
AS conferences
WITH conferences, toInt(conferences.Year) as Year, [w in split(conferences.Authors,";")] AS auths
MERGE (p:Paper {title:conferences.Title, year:coalesce(Year,"none")})
FOREACH (auth IN auths |
	MERGE (a:Author {surname:trim(auth)})
	MERGE (a)-[:WROTE]->(p)
)
MERGE (c:Conference {name:conferences.ConferenceName,city:conferences.City,year:Year})
MERGE (c)-[r:HAS]->(p)
MERGE (rev:Reviewer {surname:conferences.Reviewer})
MERGE (rev)-[re:REVIEWED]->(p)


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:////home/fran/Proyectos/PracticaNeo4j/journals.csv"
AS journals
WITH journals, [w in split(journals.Authors,";")] AS auths
MERGE(p:Paper {title:journals.Title})
MERGE (j:Journal {name:journals.JournalName,volume:journals.Volume})
MERGE (j)-[r:HAS]->(p)
FOREACH (auth IN auths |
	MERGE (a:Author {surname:auth})
	MERGE (a)-[w:WROTE]->(p)
)
MERGE (rev:Reviewer {surname:journals.Reviewer})
MERGE (rev)-[re:REVIEWED]->(p)


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:////home/fran/Proyectos/PracticaNeo4j/friendships.csv"
AS f
WITH f
MERGE (rev:Reviewer {surname:f.Reviewer})
MERGE (a:Author {surname:f.Author})
MERGE (a)-[i:IS_FRIEND]->(rev)
