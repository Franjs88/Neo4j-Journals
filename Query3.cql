MATCH (a:Author {surname:'Patiño Martínez'})-[:WROTE]->(p:Paper)
RETURN a,p;