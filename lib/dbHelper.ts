import mysql from 'mysql2/promise';

export async function initializeIndexingDB(db: mysql.Connection): Promise<void> {
    await db.execute(`
            CREATE TABLE IF NOT EXISTS kv_int (
                \`key\`   VARCHAR(255) PRIMARY KEY,
                \`value\` BIGINT NOT NULL
            )
        `);
}

export async function getIntValue(db: mysql.Connection, key: string, defaultValue: number): Promise<number> {
    const [rows] = await db.execute<mysql.RowDataPacket[]>(
        'SELECT `value` FROM kv_int WHERE `key` = ?',
        [key]
    );
    return rows[0]?.['value'] ?? defaultValue;
}

export async function setIntValue(db: mysql.Connection, key: string, value: number): Promise<void> {
    await db.execute(
        'INSERT INTO kv_int (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)',
        [key, value]
    );
}
