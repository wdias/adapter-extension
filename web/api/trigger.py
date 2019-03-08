from sqlalchemy import text as sql


def extension_trigger_create(conn, extension_id, triggers, check_trigger_on):
    for t in triggers:
        assert 'trigger_type' in t, f'trigger_type should be provided in each triggers'
        assert t.get('trigger_type') in ['OnChange', 'OnTime'], \
            f'trigger trigger_type should have one of "OnChange", "OnTime"'
        assert 'trigger_on' in t and isinstance(t.get('trigger_on'), list), \
            f'trigger_on as a list should be provided in each triggers'
        for trigger_on in t.get('trigger_on', []):
            trigger_on = check_trigger_on(trigger_on)
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
            'trigger_on': r['trigger_on'].split(','),
        })
    return triggers


def extension_get_trigger_on_change(conn, timeseries_id):
    rows = conn.execute(sql('''
        SELECT extensionId, trigger_on
        FROM triggers WHERE trigger_type=:trigger_type AND trigger_on=:timeseries_id
    '''), trigger_type='OnChange', timeseries_id=timeseries_id).fetchall()
    return [r['extensionId'] for r in rows]


def extension_get_trigger_on_time(conn):
    rows = conn.execute(sql('''
        SELECT trigger_on, GROUP_CONCAT(extensionId) as extensionIds
        FROM triggers WHERE trigger_type=:trigger_type GROUP BY trigger_on
    '''), trigger_type='OnTime').fetchall()
    triggers = []
    extension_ids = []
    for r in rows:
        triggers.append({
            'trigger_on': r['trigger_on'],
            'extensions': r['extensionIds'].split(','),
        })
        extension_ids += r['extensionIds'].split(',')
    return triggers, list(set(extension_ids))


def extension_trigger_delete(conn, extension_id):
    rows = conn.execute(sql('''
        DELETE FROM triggers
        WHERE extensionId=:extension_id
    '''), extension_id=extension_id)
    return rows
