MATCH (a:Author)-[WROTE]->(p:Paper)<-[REVIEWED]-(r:Reviewer)
WHERE p.title = "Transactional failure recovery for a distributed key-value store"
RETURN p,a,r;
