import { Client, TablesDB, Query, ID } from 'node-appwrite';

const PROJECT_ID = process.env.PROJECT_ID;
const DATABASE_ID = process.env.DATABASE_ID;
const RESOURCES_TABLE_ID = process.env.RESOURCES_TABLE_ID;
const EVENTS_TABLE_ID = process.env.EVENTS_TABLE_ID;
const ASSIGNMENTS_TABLE_ID = process.env.ASSIGNMENTS_TABLE_ID;
const DEPENDENCIES_TABLE_ID = process.env.DEPENDENCIES_TABLE_ID;
const CALENDARS_TABLE_ID = process.env.CALENDARS_TABLE_ID;

export default async ({ req, res, log, error }) => {
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

    // Fetch valid column names for each table so we only send known fields.
    const allTableIds = [
        RESOURCES_TABLE_ID, EVENTS_TABLE_ID, ASSIGNMENTS_TABLE_ID,
        DEPENDENCIES_TABLE_ID, CALENDARS_TABLE_ID
    ];
    const columnResults = await Promise.all(
        allTableIds.map(tableId =>
            tablesDB.listColumns({ databaseId : DATABASE_ID, tableId })
        )
    );
    const TABLE_COLUMNS = {};
    allTableIds.forEach((tableId, i) => {
        TABLE_COLUMNS[tableId] = columnResults[i].columns.map(col => col.key);
    });

    function createOperation(added, tableId) {
        return Promise.all(
            added.map(async(record) => {
                const { $PhantomId } = record;
                const prepared = prepareRowData(tableId, record);
                const row = await tablesDB.createRow({
                    databaseId : DATABASE_ID,
                    tableId,
                    rowId      : ID.unique(),
                    data       : prepared
                });
                return { $PhantomId, id : row.$id };
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
            updated.map((record) => {
                const { id } = record;
                const prepared = prepareRowData(tableId, record);
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
        // Filter to only valid columns for this table.
        const columns = TABLE_COLUMNS[tableId] || [];
        const prepared = Object.fromEntries(
            Object.entries(data).filter(([k]) => columns.includes(k))
        );
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
                resources    : { rows : resourcesRes.rows.map(cleanRow) },
                events       : { rows : eventsRes.rows.map(cleanRow) },
                assignments  : { rows : assignmentsRes.rows.map(cleanRow) },
                dependencies : { rows : dependenciesRes.rows.map(cleanRow) },
                calendars    : { rows : calendarsRes.rows.map(cleanRow) }
            }, 200, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
        catch(err) {
            error(JSON.stringify(err, null, 2));
            return res.json({
                success : false,
                message : err.message || 'Scheduler Pro data could not be loaded',
                type    : err.type || null
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
            error(JSON.stringify(err, null, 2));
            return res.json({
                requestId,
                success : false,
                message : err.message || 'There was an error syncing the data changes',
                type    : err.type || null
            }, 500, {
                'Access-Control-Allow-Origin': 'http://localhost:3000',
            });
        }
    }
};