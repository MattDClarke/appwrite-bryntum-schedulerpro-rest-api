import { Client, TablesDB, Query, ID } from 'node-appwrite';

const PROJECT_ID = process.env.PROJECT_ID;
const DATABASE_ID = process.env.DATABASE_ID;
const RESOURCES_TABLE_ID = process.env.RESOURCES_TABLE_ID;
const EVENTS_TABLE_ID = process.env.EVENTS_TABLE_ID;
const ASSIGNMENTS_TABLE_ID = process.env.ASSIGNMENTS_TABLE_ID;
const DEPENDENCIES_TABLE_ID = process.env.DEPENDENCIES_TABLE_ID;
const CALENDARS_TABLE_ID = process.env.CALENDARS_TABLE_ID;

export default async ({ req, res }) => {
    const client = new Client()
        .setEndpoint('https://cloud.appwrite.io/v1')
        .setProject(PROJECT_ID)
        .setJWT(req.headers['authorization']);

    const tablesDB = new TablesDB(client);

    if (req.method === 'OPTIONS') {
          return res.send('', 200, {
              'Access-Control-Allow-Origin'  : 'http://localhost:3000',
              'Access-Control-Allow-Methods' : 'POST, GET, OPTIONS',
              'Access-Control-Allow-Headers' : 'Content-Type, Authorization',
          })
    }
    if (req.method === 'GET') {
        try {
            const [
                resourcesRes,
                eventsRes,
                assignmentsRes,
                dependenciesRes,
                calendarsRes
            ] = await Promise.all([
                tablesDB.listRows(DATABASE_ID, RESOURCES_TABLE_ID),
                tablesDB.listRows(DATABASE_ID, EVENTS_TABLE_ID),
                tablesDB.listRows(DATABASE_ID, ASSIGNMENTS_TABLE_ID),
                tablesDB.listRows(DATABASE_ID, DEPENDENCIES_TABLE_ID),
                tablesDB.listRows(DATABASE_ID, CALENDARS_TABLE_ID)
            ]);

            function cleanRow(row) {
                row.id = row.$id;
                const obj = Object.fromEntries(
                    Object.entries(row)
                        .filter(([_, v]) => v != null)
                        .filter(([k]) => k[0] !== '$')
                );
                // Parse JSON string fields back to objects
                ['intervals', 'exceptionDates', 'segments'].forEach((field) => {
                    if (typeof obj[field] === 'string') {
                        try { obj[field] = JSON.parse(obj[field]); } catch (e) { /* keep as string */ }
                    }
                });
                return obj;
            }

            return res.json({
                success      : true,
                resources    : { rows : resourcesRes.documents.map(cleanRow) },
                events       : { rows : eventsRes.documents.map(cleanRow) },
                assignments  : { rows : assignmentsRes.documents.map(cleanRow) },
                dependencies : { rows : dependenciesRes.documents.map(cleanRow) },
                calendars    : { rows : calendarsRes.documents.map(cleanRow) }
            }, 200, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
        catch(err) {
            return res.json({
                success : false,
                message : 'Scheduler Pro data could not be loaded'
            }, 500, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
    }

};