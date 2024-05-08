
def insert_to_upsert(query: str, source_columns: list) -> str:
    query = query.replace("INSERT INTO", "INSERT IGNORE INTO")
    update_clause = ",".join([f'{column} = VALUES({column})' for column in source_columns])
    query += f" ON DUPLICATE KEY UPDATE {update_clause}"
    return query


def apply_limit(query: str, limit: int) -> str:
    return query + f" ORDER BY id LIMIT {limit}"
