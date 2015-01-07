USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:////home/fran/Proyectos/PracticaNeo4j/conferences.csv"
AS conferences
WITH conferences, toInt(conferences.Year) as Year, split(conferences.Authors,";") AS auths
MERGE (p:Paper {title:conferences.Title, year:coalesce(Year,"none")})
MERGE (c:Conference {name:conferences.ConferenceName,city:conferences.City,year:Year})-[r:HAS]->(p)
FOREACH (auth IN auths |
	MERGE (a:Author {surname:auth})-[w:WROTE]->(p)
)
MERGE (rev:Reviewer {surname:conferences.Reviewer})-[re:REVIEWED]->(p)


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:////home/fran/Proyectos/PracticaNeo4j/journals.csv"
AS journals
WITH journals, split(journals.Authors,";") AS auths
MERGE(p:Paper {title:journals.Title})
MERGE (j:Journal {name:journals.JournalName,volume:journals.Volume})-[r:HAS]->(p)
FOREACH (auth IN auths |
	MERGE (a:Author {surname:auth})-[w:WROTE]->(p)
)
MERGE (rev:Reviewer {surname:journals.Reviewer})-[re:REVIEWED]->(p)


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:////home/fran/Proyectos/PracticaNeo4j/friendships.csv"
AS f
WITH f
MERGE (rev:Reviewer {surname:f.Reviewer})
MERGE (a:Author {surname:f.Author})-[i:IS_FRIEND]->(rev)