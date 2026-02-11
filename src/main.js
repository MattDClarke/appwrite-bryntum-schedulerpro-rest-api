import { Client, TablesDB, Query, ID } from 'node-appwrite';

const PROJECT_ID = process.env.PROJECT_ID;
const DATABASE_ID = process.env.DATABASE_ID;
const RESOURCES_TABLE_ID = process.env.RESOURCES_TABLE_ID;
const EVENTS_TABLE_ID = process.env.EVENTS_TABLE_ID;
const ASSIGNMENTS_TABLE_ID = process.env.ASSIGNMENTS_TABLE_ID;
const DEPENDENCIES_TABLE_ID = process.env.DEPENDENCIES_TABLE_ID;
const CALENDARS_TABLE_ID = process.env.CALENDARS_TABLE_ID;

export default async ({ req, res }) => {
    if (req.method === 'OPTIONS') {
        return res.send('', 200, {
            'Access-Control-Allow-Origin'  : 'http://localhost:3000',
            'Access-Control-Allow-Methods' : 'POST, GET, OPTIONS',
            'Access-Control-Allow-Headers' : 'Content-Type, Authorization',
        });
    }

    const jwt = req.headers['authorization'];
    if (!jwt) {
        return res.json({ success: false, message: 'Unauthorized' }, 401, {
            'Access-Control-Allow-Origin': 'http://localhost:3000',
        });
    }

    const client = new Client()
        .setEndpoint('https://fra.cloud.appwrite.io/v1')
        .setProject(PROJECT_ID)
        .setJWT(jwt);

    const tablesDB = new TablesDB(client);

    function createOperation(added, tableId) {
        return Promise.all(
            added.map(async(record) => {
                const { $PhantomId, ...data } = record;
                const prepared = prepareRowData(tableId, data);
                const { $id } = await tablesDB.createRow({
                    databaseId : DATABASE_ID,
                    tableId,
                    rowId      : ID.unique(),
                    data       : prepared
                });
                return { $PhantomId, id : $id };
            })
        );
    }

    function deleteOperation(removed, tableId) {
        return Promise.all(
            removed.map(({ id }) => tablesDB.deleteRow({
                databaseId : DATABASE_ID,
                tableId,
                rowId      : id
            }))
        );
    }

    function updateOperation(updated, tableId) {
        return Promise.all(
            updated.map(({ $PhantomId, id, ...data }) => {
                const prepared = prepareRowData(tableId, data);
                return tablesDB.updateRow({
                    databaseId : DATABASE_ID,
                    tableId,
                    rowId      : id,
                    data       : prepared
                });
            })
        );
    }

    function prepareRowData(tableId, data) {
        const prepared = { ...data };
        // Remove dependency alias fields that aren't table columns.
        if (tableId === DEPENDENCIES_TABLE_ID) {
            delete prepared.from;
            delete prepared.to;
        }
        // Stringify JSON fields for storage.
        ['exceptionDates', 'segments', 'intervals'].forEach((field) => {
            if (prepared[field] && typeof prepared[field] === 'object') {
                prepared[field] = JSON.stringify(prepared[field]);
            }
        });
        return prepared;
    }

    async function applyTableChanges(tableId, changes) {
        let rows;
        if (changes.added) {
            rows = await createOperation(changes.added, tableId);
        }
        if (changes.removed) {
            await deleteOperation(changes.removed, tableId);
        }
        if (changes.updated) {
            await updateOperation(changes.updated, tableId);
        }
        // New row IDs to send to the client.
        return rows;
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
                tablesDB.listRows({ databaseId : DATABASE_ID, tableId : RESOURCES_TABLE_ID }),
                tablesDB.listRows({ databaseId : DATABASE_ID, tableId : EVENTS_TABLE_ID }),
                tablesDB.listRows({ databaseId : DATABASE_ID, tableId : ASSIGNMENTS_TABLE_ID }),
                tablesDB.listRows({ databaseId : DATABASE_ID, tableId : DEPENDENCIES_TABLE_ID }),
                tablesDB.listRows({ databaseId : DATABASE_ID, tableId : CALENDARS_TABLE_ID })
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
            console.log(err)
            return res.json({
                success : false,
                error: err,
                message : 'Scheduler Pro data could not be loaded'
            }, 500, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
    }
    if (req.method === 'POST') {
        const { requestId, resources, events, assignments, dependencies, calendars } = req.body;
        try {
            const response = { requestId, success : true };
            let eventMapping = {};

            if (resources) {
                const rows = await applyTableChanges(RESOURCES_TABLE_ID, resources);
                if (rows) response.resources = { rows };
            }
            if (events) {
                const rows = await applyTableChanges(EVENTS_TABLE_ID, events);
                if (rows) {
                    // Map phantom event IDs to real IDs for assignment references.
                    rows.forEach((row) => {
                        eventMapping[row.$PhantomId] = row.id;
                    });
                    response.events = { rows };
                }
            }
            if (assignments) {
                // Replace phantom event IDs with real IDs.
                if (events?.added) {
                    assignments.added?.forEach((assignment) => {
                        if (eventMapping[assignment.eventId]) {
                            assignment.eventId = eventMapping[assignment.eventId];
                        }
                    });
                }
                const rows = await applyTableChanges(ASSIGNMENTS_TABLE_ID, assignments);
                if (rows) response.assignments = { rows };
            }
            if (dependencies) {
                const rows = await applyTableChanges(DEPENDENCIES_TABLE_ID, dependencies);
                if (rows) response.dependencies = { rows };
            }
            if (calendars) {
                const rows = await applyTableChanges(CALENDARS_TABLE_ID, calendars);
                if (rows) response.calendars = { rows };
            }

            return res.json(response, 200, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
        catch(err) {
            console.log(err)
            return res.json({
                requestId,
                success : false,
                message : 'There was an error syncing the data changes'
            }, 500, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
    }
};