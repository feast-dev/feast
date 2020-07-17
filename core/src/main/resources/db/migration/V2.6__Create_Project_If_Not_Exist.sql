-- SQL migration to migrate create 'default' project if it does not already exist.
INSERT INTO projects 
    (name, archived)
    SELECT 'default', false
    WHERE NOT EXISTS (
        SELECT name FROM projects WHERE name='default'
    );
