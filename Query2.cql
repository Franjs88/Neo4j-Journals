MATCH (c:Conference)-[HAS]->(p:Paper)
WHERE c.name = "SAC" 
RETURN c,p;
