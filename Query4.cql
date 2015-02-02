MATCH (j:Journal {volume:'27', name:'ieee explore'})-[:HAS]->(p:Paper)<-[:WROTE]-(a:Author)-[:IS_FRIEND]->(r:Reviewer)-[:REVIEWED]->(p)
RETURN j,p,a,r;
