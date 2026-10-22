-- Initialize pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Verify the extension is installed
SELECT * FROM pg_extension WHERE extname = 'vector';

-- Create a test table with vector column to verify functionality
CREATE TABLE IF NOT EXISTS vector_test (
    id SERIAL PRIMARY KEY,
    embedding vector(3)
);

-- Insert a test vector
INSERT INTO vector_test (embedding) VALUES ('[1,2,3]');

-- Test a simple vector query
SELECT * FROM vector_test ORDER BY embedding <-> '[3,2,1]' LIMIT 1;

-- Clean up test table
DROP TABLE vector_test;

-- Output success message
DO $$
BEGIN
    RAISE NOTICE 'pgvector extension successfully installed and tested!';
END
$$;