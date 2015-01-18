MATCH (j:Journal {volume:'27'})-[:HAS]->(p:Paper)<-[:WROTE]-(a:Author)-[:IS_FRIEND]->(r:Reviewer)
RETURN j,p,a,r;