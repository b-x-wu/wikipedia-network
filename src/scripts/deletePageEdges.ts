import Neo4j, { Integer } from 'neo4j-driver'
import dotenv from 'dotenv'
dotenv.config()

const main = async () => {
    const driver = Neo4j.driver(
        process.env.NEO4J_URI ?? '',
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? ''),
        {
            maxConnectionLifetime: 1000 * 60 * 5, // five minutes
        }
    )
    
    while (((await driver.executeQuery('match ()-[r:LINKS_TO]->() return count(r) as count')).records.at(0)?.get('count') as Integer).toInt() > 0) {
        await driver.executeQuery('match (:Page)-[r:LINKS_TO]->(:Page) with r limit 100000 delete r')
    }
    await driver.close()
}

main().catch(console.error)