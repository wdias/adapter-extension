from sqlalchemy import text as sql


def extension_trigger_create(conn, extension_id, triggers):
    for t in triggers:
        assert 'trigger_type' in t, f'trigger_type should be provided in each triggers'
        assert t.get('trigger_type') in ['OnChange', 'OnTime'], \
            f'trigger trigger_type should have one of "OnChange", "OnTime"'
        assert 'trigger_on' in t and isinstance(t.get('trigger_on'), list), \
            f'trigger_on as a list should be provided in each triggers'
        for trigger_on in t.get('trigger_on', []):
            conn.execute(sql('''
                INSERT IGNORE INTO triggers (extensionId, `trigger_type`, `trigger_on`)
                VALUES (:extension_id, :trigger_type, :trigger_on)
            '''), extension_id=extension_id, trigger_type=t.get('trigger_type'), trigger_on=trigger_on)


def extension_trigger_get(conn, extension_id):
    rows = conn.execute(sql('''
        SELECT trigger_type, GROUP_CONCAT(trigger_on) as trigger_on
        FROM triggers WHERE extensionId=:extension_id GROUP BY trigger_type
    '''), extension_id=extension_id).fetchall()
    triggers = []
    for r in rows:
        triggers.append({
            'trigger_type': r['trigger_type'],
            'trigger_on': r['trigger_on'],
        })
    return triggers


def extension_get_trigger_type(conn, trigger_type, timeseries_id=None):
    assert trigger_type in ['OnChange', 'OnTime'], \
        f'trigger trigger_type should have one of "OnChange", "OnTime"'
    if timeseries_id is None:
        rows = conn.execute(sql('''
            SELECT extensionId
            FROM triggers WHERE trigger_type=:trigger_type
        '''), trigger_type=trigger_type).fetchall()
    else:
        rows = conn.execute(sql('''
            SELECT extensionId
            FROM triggers WHERE trigger_type=:trigger_type AND trigger_on=:timeseries_id
        '''), trigger_type=trigger_type, timeseries_id=timeseries_id).fetchall()
    return [r['extensionId'] for r in rows]


def extension_trigger_delete(conn, extension_id):
    rows = conn.execute(sql('''
        DELETE FROM triggers
        WHERE extensionId=:extension_id
    '''), extension_id=extension_id)
    return rows
