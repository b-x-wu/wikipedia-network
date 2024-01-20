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
    
    while (((await driver.executeQuery('match (p:Page) return count(p) as count')).records.at(0)?.get('count') as Integer).toInt() > 0) {
        await driver.executeQuery('match (p:Page) with p limit 50000 delete p')
    }

    await driver.close()
}

main().catch(console.error)